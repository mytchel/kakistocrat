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

public:
  ScorerImpl(kj::AsyncIoContext &io_context, const config &s)
    : settings(s), tasks(*this), timer(io_context.provider->getTimer())
  {
  }

  float param_B = 1000;
  float param_e = 0.1;
  float val_c = 0.0;

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
    //float sp = 1 + t * (1 + s) - E(pow(e, t * w));
    float sp = 0.1;

    val_c = 2 / (sp * param_e);
    uint32_t K = val_c * log(pageCount);
    iterations = param_B * log(pageCount / param_e);
    spdlog::info("start scoring {} pages, c = {}, K = {}, iterations = {}",
        pageCount, val_c, K, iterations);

    if (iterations == 0) {
      iterations = 1;
    }

    workerOps = workers.size();

    for (auto &worker: workers) {
      auto request = worker.setupRequest();
      request.setK(K);
      request.setE(param_e);

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

  void iterate() {
    if (iterations == 0) {
      spdlog::info("finished scoring");
      return;

    } else {
      iterations--;
    }

    spdlog::info("iterating {}", iterations);

    workerOps = workers.size();

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

    auto builder = kj::heapArrayBuilder<kj::Promise<double>>(workers.size());

    for (auto &worker: workers) {
      auto request = worker.getCounterRequest();
      request.setUrl(url);

      builder.add(request.send().then(
            [this, url] (auto result) {
              uint32_t s = result.getCounter();
              spdlog::info("got counter response: {} : {}", s, std::string(url));
              return s * param_e / (val_c * pageCount * log(pageCount));
            },
            [] (auto exception) {
              spdlog::warn("add walk failed: {}", std::string(exception.getDescription()));
              return 0;
            }));
    }

    return kj::joinPromises(builder.finish()).then(
        [url, KJ_CPCAP(context)] (auto results) mutable {
          for (auto s: results) {
            if (s > 0) {
              spdlog::info("got score response: {} : {}", s, std::string(url));
              context.getResults().setScore(s);
              return;
            }
          }
        });
  }

  void addSites(std::vector<std::string> sites) {
    if (workers.empty()) {
      spdlog::info("putting off add sites");

      tasks.add(timer.afterDelay(5 * kj::SECONDS).then(
            [this, sites] () mutable {
              addSites(sites);
            }));
      return;
    }

    spdlog::info("adding {} sites to {} workers", sites.size(), workers.size());

    size_t i = 0;
    for (auto s: sites) {
      addingSites++;

      auto request = workers[i % workers.size()].addSiteRequest();

      request.setSitePath(s);

      tasks.add(request.send().then(
            [this, i] (auto result) {
              size_t n = result.getPageCount();

              spdlog::debug("added site to worker {} got {} pages", i, n);

              pageCount += n;

              if (--addingSites == 0) {
                startScoring();
              }
            }));

      i++;
    }
  }

  kj::Promise<void> score(ScoreContext context) override {
    spdlog::info("got score request");

    pageCount = 0;
    addingSites = 0;

    auto params = context.getParams();

    std::vector<std::string> sites;

    for (auto s: params.getSitePaths()) {
      sites.emplace_back(s);
    }

    addSites(sites);

    return kj::READY_NOW;
  }

  kj::Promise<void> addWalk(AddWalkContext context) override {
    auto params = context.getParams();
    auto site = params.getSite();
    std::string url = params.getUrl();
    auto hits = params.getHits();

    spdlog::info("got walk for {}", url);

    auto builder = kj::heapArrayBuilder<kj::Promise<bool>>(workers.size());

    for (auto &worker: workers) {
      auto request = worker.addWalkRequest();
      request.setSite(site);
      request.setUrl(url);
      request.setHits(hits);

      builder.add(request.send().then(
            [] (auto result) {
              return result.getFound();
            },
            [] (auto exception) {
              spdlog::warn("add walk failed: {}", std::string(exception.getDescription()));
              return false;
            }));
    }

    return kj::joinPromises(builder.finish()).then(
        [KJ_CPCAP(context)] (auto results) mutable {
          bool r = false;
          for (auto rr: results) {
            if (rr) {
              r = true;
              break;
            }
          }

          context.getResults().setFound(r);
        });
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

  size_t pageCount;

  size_t iterations;
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

