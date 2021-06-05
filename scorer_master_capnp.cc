#include "math.h"

#include <stdio.h>
#include <unistd.h>

#include <cstdlib>
#include <cstring>

#include <vector>
#include <list>
#include <set>
#include <map>
#include <string>
#include <algorithm>
#include <thread>
#include <future>
#include <optional>
#include <iostream>
#include <fstream>
#include <sstream>
#include <cstdint>

#include <nlohmann/json.hpp>
#include "spdlog/spdlog.h"

#include "util.h"
#include "config.h"
#include "site.h"
#include "hash.h"
#include "capnp_server.h"

#include "indexer.capnp.h"

#include <kj/debug.h>
#include <kj/string.h>
#include <kj/async-io.h>
#include <kj/async-unix.h>

#include <capnp/rpc.h>
#include <capnp/rpc-twoparty.h>
#include <capnp/ez-rpc.h>
#include <capnp/message.h>
#include <iostream>

using nlohmann::json;

class ScorerImpl final: public Scorer::Server,
                        public kj::TaskSet::ErrorHandler {

/*
  struct worker {
    ScorerWorker::Client client;

    std::string output;
  };
*/

public:
  ScorerImpl(kj::AsyncIoContext &io_context, const config &s)
    : settings(s), tasks(*this), timer(io_context.provider->getTimer()),
      siteWorkers(site_hash_cap, nullptr),
      output_path(fmt::format("{}/main.json", settings.scores_path))
  {
  }

  float param_B = 5;
  float param_e = 0.001;
  float val_c = 0.0;
  float val_bias = 0.0;

  void startScoring() {
    if (pageCount == 0) {
      spdlog::warn("score 0 pages?");
      return;
    }

    // optimize over t.
    // W = param_e * Y. Y is random with geometric distribution
    // over param_e.
    // s = arbitrary constant
    // https://www.sciencedirect.com/science/article/pii/S0304397514002709
    // I gave up on math for a reason.

    // float sp = 1 + t * (1 + s) - E(pow(e, t * w));
    float sp = 100;

    val_c = 2 / (sp * param_e);
    uint32_t K = val_c * log(pageCount);
    iterations = param_B * log(pageCount / param_e);

    val_bias = K;//1 + 5 * K / iterations;

    spdlog::info("start scoring {} pages, c = {}, K = {}, seed bias = {}, iterations = {}",
        pageCount, val_c, K, val_bias, iterations);

    if (iterations == 0) {
      iterations = 1;
    }

    workerOps = workers.size();
    iterateRunning = true;

    output_paths.clear();

    size_t i = 0;
    for (auto &worker: workers) {
      output_paths.emplace_back(
            fmt::format("{}/block{}.capnp",
              settings.scores_path, i++));

      auto request = worker.setupRequest();
      request.setK(K);
      request.setE(param_e);
      request.setBias(val_bias);
      request.setPath(output_paths.back());

      tasks.add(request.send().then(
            [this] (auto result) {
              spdlog::debug("worker setup done");
              if (--workerOps == 0) {
                spdlog::debug("start iterating");
                iterate();
              }
            }));
    }
  }

  void saveWorkers() {
    workerOps = workers.size();

    for (auto &worker: workers) {
      auto request = worker.saveRequest();
      tasks.add(request.send().then(
        [this] (auto result) {
          spdlog::debug("worker iterate done");
          if (--workerOps == 0) {
            save();
            spdlog::info("new scores saved");
          }
        }));
    }
  }

  void iterate() {
    if (iterations == 0 || !iterateRunning) {
      spdlog::info("finished scoring");

      if (!iterateRunning) {
        spdlog::info("converged early, still have {} iterations",
          iterations);
      }

      saveWorkers();

      return;

    } else {
      iterations--;
    }

    spdlog::info("iterating {}", iterations);

    workerOps = workers.size();
    iterateRunning = false;

    for (auto &worker: workers) {
      auto request = worker.iterateRequest();
      tasks.add(request.send().then(
            [this] (auto result) {
              spdlog::debug("worker iterate done");
              if (--workerOps == 0) {

                spdlog::info("iterator workers all done. wait then iterate again");

                tasks.add(timer.afterDelay(5 * kj::SECONDS).then(
                      [this] () {
                        spdlog::info("iterator again");
                        iterateFinish();
                      }));
              }
            },
            [this] (auto exception) {
              spdlog::warn("exception iterating worker: {}", std::string(exception.getDescription()));
            }));
    }
  }

  void iterateFinish() {
    spdlog::info("finish iterating {}", iterations);

    workerOps = workers.size();

    for (auto &worker: workers) {
      auto request = worker.iterateFinishRequest();
      tasks.add(request.send().then(
            [this] (auto result) {
              spdlog::debug("worker iterate finish done");

              bool running = result.getRunning();
              iterateRunning |= running;

              if (--workerOps == 0) {

                spdlog::info("iterator workers all done");
      
                iterate();
              }
            },
            [this] (auto exception) {
              spdlog::warn("exception iterating worker: {}", std::string(exception.getDescription()));
            }));
    }
  }

  kj::Promise<void> registerScorerWorker(RegisterScorerWorkerContext context) override {
    spdlog::info("got register worker request");

    workers.push_back(context.getParams().getWorker());

    return kj::READY_NOW;
  }

  kj::Promise<void> getScore(GetScoreContext context) override {
    spdlog::info("got get score request");

    auto params = context.getParams();
    auto url = params.getUrl();

    auto site = util::get_host(url);

    uint32_t site_hash = hash(site, site_hash_cap);
    auto worker = siteWorkers[site_hash];
    if (worker == nullptr) {
      return kj::READY_NOW;
    }

    auto request = worker->getCounterRequest();
    request.setUrl(url);

    return request.send().then(
          [this, url, KJ_CPCAP(context)] (auto result) mutable {
            uint32_t counter = result.getCounter();
            spdlog::info("got counter response: {} : {}", counter, std::string(url));
            float score =  counter * param_e / (val_c * pageCount * log(pageCount));
            context.getResults().setScore(score);
          },
          [] (auto exception) {
            spdlog::warn("add walk failed: {}", std::string(exception.getDescription()));
          });
  }

  void save() {
    std::ofstream file;

    spdlog::debug("save {}", output_path);

    json j = {{ "blocks", output_paths }};

    file.open(output_path, std::ios::out | std::ios::trunc);
    if (!file.is_open()) {
      spdlog::warn("error opening file{}", output_path);
      return;
     }

     file << j;
     file.close();
  }

  void load() {
    std::ifstream file;

    spdlog::debug("load {}", output_path);

    file.open(output_path, std::ios::in);
    if (!file.is_open()) {
      spdlog::warn("error opening file{}", output_path);
      return;
    }

    try {
      json j = json::parse(file);

      j.at("blocks").get_to(output_paths);

    } catch (const std::exception &e) {
      spdlog::warn("failed to load {}", output_path);
    }

    file.close();

    if (workers.size() != output_paths.size()) {
      spdlog::warn("workers != output paths sizes. we fucked");
      return;
    }
      
    workerOps = workers.size();

    size_t i = 0;
    for (auto &worker: workers) {
      auto request = worker.loadRequest();
      request.setPath(output_paths[i++]);

      tasks.add(request.send().then(
        [this] (auto result) {
          spdlog::debug("worker load done");
          if (--workerOps == 0) {
            spdlog::info("workers all loaded");
          }
        }));
    }
  }

  void addSites() {
    if (workers.empty()) {
      spdlog::info("putting off add sites");

      tasks.add(timer.afterDelay(5 * kj::SECONDS).then(
            [this] () mutable {
              addSites();
            }));
      return;
    }

    spdlog::info("adding {} sites to {} workers", scoring_sites.size(), workers.size());

    size_t next_worker = 0;
    for (auto s: scoring_sites) {
      auto site_host = util::get_host_from_meta_path(s);

      uint32_t site_hash = hash(site_host, site_hash_cap);
      auto worker = siteWorkers[site_hash];
      if (worker == nullptr) {
        // hopefully even enough.
        siteWorkers[site_hash] = &workers[next_worker++ % workers.size()];
        worker = siteWorkers[site_hash];
      }

      auto request = worker->addSiteRequest();

      request.setSitePath(s);

      addingSites++;
      tasks.add(request.send().then(
          [this] (auto result) {
            size_t n = result.getPageCount();

            spdlog::debug("added site to worker, got {} pages", n);

            pageCount += n;

            if (--addingSites == 0) {
              addSeed();
            }
          }));
    }
  }

  void addSeed() {
    for (auto &s: scoring_seed) {
      auto site_host = util::get_host(s);

      uint32_t site_hash = hash(site_host, site_hash_cap);
      auto worker = siteWorkers[site_hash];
      if (worker == nullptr) {
        continue;
      }

      auto request = worker->setSeedRequest();

      request.setUrl(s);

      addingSites++;
      tasks.add(request.send().then(
          [this] (auto result) {
            if (--addingSites == 0) {
              startScoring();
            }
          }));
    }
  }

  kj::Promise<void> score(ScoreContext context) override {
    spdlog::info("got score request");

    pageCount = 0;
    addingSites = 0;

    auto params = context.getParams();
    
    scoring_sites.clear();
    scoring_seed.clear();

    for (auto s: params.getSitePaths()) {
      scoring_sites.emplace_back(s);
    }

    for (auto s: params.getSeed()) {
      scoring_seed.emplace_back(s);
    }

    addSites();

    return kj::READY_NOW;
  }

  kj::Promise<void> addWalks(AddWalksContext context) override {
    spdlog::info("walks request");

    auto params = context.getParams();

    std::map<ScorerWorker::Client *,
        std::vector<std::pair<std::string, uint32_t>>
      > requestWalks;

    for (auto walk: params.getWalks()) {
      std::string url = walk.getUrl();
      auto hits = walk.getHits();

      auto site = util::get_host(url);

      uint32_t site_hash = hash(site, site_hash_cap);
      auto worker = siteWorkers[site_hash];
      if (worker == nullptr) {
        continue;
      }

      auto it = requestWalks.try_emplace(worker);
      it.first->second.emplace_back(url, hits);
    }

    for (auto &r: requestWalks) {
      auto request = r.first->addWalksRequest();

      auto w = request.initWalks(r.second.size());

      spdlog::info("sending {} walks", r.second.size());

      for (size_t i = 0; i < r.second.size(); i++) {
        auto ww = w[i];
        ww.setUrl(r.second[i].first);
        ww.setHits(r.second[i].second);
      }

      tasks.add(request.send().then(
            [] (auto result) {},
            [] (auto exception) {
              spdlog::warn("add walk failed: {}", std::string(exception.getDescription()));
            }));
    }

    return kj::READY_NOW;
  }

  void taskFailed(kj::Exception&& exception) override {
    spdlog::warn("task failed: {}", std::string(exception.getDescription()));
    kj::throwFatalException(kj::mv(exception));
  }

  const config &settings;

  std::vector<ScorerWorker::Client> workers;

  kj::TaskSet tasks;
  kj::Timer &timer;

  size_t addingSites;

  size_t workerOps;

  uint64_t pageCount;

  size_t iterations;
  bool iterateRunning;

  std::vector<std::string> scoring_sites;
  std::vector<std::string> scoring_seed;

  std::string output_path;
  std::vector<std::string> output_paths;

  size_t site_hash_cap{1<<12};
  std::vector<ScorerWorker::Client *> siteWorkers;
};

int main(int argc, char *argv[]) {
  spdlog::set_level(spdlog::level::debug);

  std::string masterAddress = "localhost:1234";
  std::string bindAddress = "localhost:1235";

  spdlog::info("read config");
  config settings = read_config();

  // two way vat

  kj::UnixEventPort::captureSignal(SIGINT);
  auto ioContext = kj::setupAsyncIo();

  auto addrPromise = ioContext.provider->getNetwork().parseAddress(masterAddress)
  .then([](kj::Own<kj::NetworkAddress> addr) {
      spdlog::info("using addr {}", std::string(addr->toString().cStr()));
      return addr->connect().attach(kj::mv(addr));
  });

  auto stream = addrPromise.wait(ioContext.waitScope);

  capnp::TwoPartyVatNetwork network(*stream, capnp::rpc::twoparty::Side::CLIENT);

  auto rpcSystem = capnp::makeRpcClient(network);

  {
    capnp::MallocMessageBuilder message;
    auto hostId = message.getRoot<capnp::rpc::twoparty::VatId>();
    hostId.setSide(capnp::rpc::twoparty::Side::SERVER);

    Master::Client master = rpcSystem.bootstrap(hostId).castAs<Master>();

    spdlog::info("creating client");

    Scorer::Client scorer = kj::heap<ScorerImpl>(ioContext, settings);

    Server server(ioContext, bindAddress, scorer);

    spdlog::info("create request");
    auto request = master.registerScorerRequest();
    request.setScorer(scorer);

    spdlog::info("send scorer register");

    auto r = request.send().then(
        [] (auto result) {
          spdlog::info("scorer registered");
        },
        [] (auto exception) {
          spdlog::warn("exception registering scorer : {}", std::string(exception.getDescription()));
        });

    spdlog::info("waiting for register");
    r.wait(ioContext.waitScope);

    spdlog::info("waiting for sigint");
    ioContext.unixEventPort.onSignal(SIGINT).wait(ioContext.waitScope);
  }

  return 0;
}

