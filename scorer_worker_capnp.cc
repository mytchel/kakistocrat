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

class ScorerWorkerImpl final: public ScorerWorker::Server,
                              public kj::TaskSet::ErrorHandler {

  class node {
  public:

    std::string url;
    std::vector<std::pair<uint32_t, size_t>> links;

    size_t next_coupons{0};

    size_t coupons{0};
    uint32_t counter{0};

    node(const std::string &url, std::vector<std::pair<uint32_t, size_t>> links)
      : url(url), links(links) {}

    // TODO: have all over finish and swap out counter for
    // a different one that was just calculated.
    void reset(uint32_t K) {
      coupons = K;
      counter = 0;
      next_coupons = 0;
    }

    void add(uint32_t hits) {
      next_coupons += hits;
    }

    bool iterate_finish() {
      counter += next_coupons;
      coupons = next_coupons;
      next_coupons = 0;

      return coupons > 0;
    }

    void iterate(ScorerWorkerImpl &worker) {
      spdlog::info("iterate {}, {} = {}", counter, coupons, url);

      if (links.empty()) {
        return;
      }

      std::vector<uint32_t> ll;
      std::vector<uint32_t> hits;

      for (size_t l = 0; l < links.size(); l++) {
        for (size_t i = 0; i < links[l].second; i++) {
          ll.push_back(l);
          hits.push_back(0);
        }
      }

      for (size_t i = 0; i < coupons; i++) {
        if (util::get_rand() > 1 - worker.param_e) {
          break;
        }

        size_t l = util::get_rand() * hits.size();
        hits[l]++;
      }

      std::vector<uint32_t> link_hits(links.size());

      for (size_t i = 0; i < hits.size(); i++) {
        link_hits[ll[i]] += hits[i];
      }

      for (size_t i = 0; i < link_hits.size(); i++) {
        if (link_hits[i] > 0) {
          worker.addWalk(links[i].first, link_hits[i]);
        }
      }
    }
  };

  struct adaptor {
  public:
    adaptor(kj::PromiseFulfiller<void> &fulfiller,
            ScorerWorkerImpl &worker,
            std::vector<node*> nodes)
      : fulfiller(fulfiller),
        worker(worker),
        nodes(nodes)
    {
      process();
    }

    void process() {
      spdlog::debug("iterate process, active walks {}", worker.active_walks);

      if (worker.active_walks > 0) {
        spdlog::debug("still have active walks {}, waiting", worker.active_walks);
        worker.tasks.add(worker.timer.afterDelay(100 * kj::MILLISECONDS).then(
          [this] () {
            spdlog::debug("continue processing");
            process();
          }
        ));
        return;
      }
 
      while (!nodes.empty()) {
        if (worker.active_walks > 10000) {
          spdlog::debug("active walks {}, waiting", worker.active_walks);
          worker.tasks.add(worker.timer.afterDelay(100 * kj::MILLISECONDS).then(
            [this] () {
              spdlog::debug("continue processing");
              process();
            }
          ));
          return;
        }

        auto node = nodes.back();
        nodes.pop_back();

        node->iterate(worker);
      }

      spdlog::debug("iterate step finished");
      fulfiller.fulfill();
    }

    kj::PromiseFulfiller<void> &fulfiller;
    ScorerWorkerImpl &worker;
    std::vector<node*> nodes;
  };

public:
  ScorerWorkerImpl(kj::AsyncIoContext &io_context, const config &s, Scorer::Client master)
    : settings(s), tasks(*this), timer(io_context.provider->getTimer()),
      master(master)
  {
  }

  kj::Promise<void> getCounter(GetCounterContext context) override {
    auto params = context.getParams();
    std::string url = params.getUrl();

    spdlog::info("get score {}", url);

    uint32_t id = urlToId(url, false);
    if (id == 0) {
      return kj::READY_NOW;
    }

    auto it = nodes.find(id);
    if (it != nodes.end()) {
      spdlog::info("get score {} = {}", url, it->second.counter);
      context.getResults().setCounter(it->second.counter);
    }

    return kj::READY_NOW;
  }

  kj::Promise<void> addSite(AddSiteContext context) override {
    spdlog::info("add site");

    auto sitePath = context.getParams().getSitePath();

    site_map site(sitePath);

    site.load();

    size_t c = 0;

    for (auto p: site.pages) {
      if (p.last_scanned > 0) {
        spdlog::info("add page {}", p.url);

        std::vector<std::pair<uint32_t, size_t>> links;
        for (auto &l: p.links) {
          links.emplace_back(urlToId(l.first, true), l.second);
        }

        nodes.emplace(urlToId(p.url, true), node(p.url, links));
        c++;
      }
    }

    context.getResults().setPageCount(c);

    return kj::READY_NOW;
  }

  kj::Promise<void> setup(SetupContext context) override {
    uint32_t K = context.getParams().getK();
    param_e = context.getParams().getE();

    spdlog::info("setup for scoring {}, {}", K, param_e);

    for (auto &n: nodes) {
      n.second.reset(K);
    }

    return kj::READY_NOW;
  }

  kj::Promise<void> iterate(IterateContext) override {
    std::vector<node *> l_nodes;

    spdlog::debug("iterate setup");
    
    for (auto &node: nodes) {
      l_nodes.push_back(&node.second);
    }
    
    spdlog::debug("iterate start");

    return kj::newAdaptedPromise<void, adaptor>(*this, l_nodes);
  }

  kj::Promise<void> iterateFinish(IterateFinishContext context) override {
    bool r = false;
    for (auto &node: nodes) {
      r |= node.second.iterate_finish();
    }

    context.getResults().setRunning(r);

    return kj::READY_NOW;
  }

  void addWalk(uint32_t id, uint32_t hits) {
    auto it = nodes.find(id);
    if (it != nodes.end()) {
      it->second.add(hits);

    } else {
      auto url = urlFromId(id);
      if (url == nullptr) {
        spdlog::warn("got bad url id somehow: {}", id);
        return;
      }

      auto request = master.addWalkRequest();
      request.setUrl(*url);
      request.setHits(hits);

      active_walks++;

      tasks.add(request.send().then(
            [this] (auto result) {
              active_walks--;
            }));
    }
  }

  kj::Promise<void> addWalk(AddWalkContext context) override {
    auto params = context.getParams();
    auto site = params.getSite();
    std::string url = params.getUrl();
    auto hits = params.getHits();


    uint32_t id = urlToId(url, false);
    if (id == 0) {
      return kj::READY_NOW;
    }

    auto it = nodes.find(id);
    if (it != nodes.end()) {
      spdlog::info("got remote walk for {}", url);
      it->second.add(hits);
    }

    return kj::READY_NOW;
  }

  uint32_t urlToId(const std::string &u, bool make = false) {
    auto it = urls_to_id.find(u);
    if (it == urls_to_id.end()) {
      if (!make) {
        return 0;
      }

      uint32_t id = ++next_url_id;

      urls_to_id.emplace(u, id);
      urls.emplace(id, u);

      return id;

    } else {
      return it->second;
    }
  }

  const std::string * urlFromId(uint32_t id) {
    auto it = urls.find(id);
    if (it != urls.end()) {
      return &it->second;
    } else {
      return nullptr;
    }
  }

  void taskFailed(kj::Exception&& exception) override {
    spdlog::warn("task failed: {}", std::string(exception.getDescription()));
    kj::throwFatalException(kj::mv(exception));
  }

  const config &settings;

  Scorer::Client master;

  kj::TaskSet tasks;
  kj::Timer &timer;

  float param_e;

  std::unordered_map<uint32_t, node> nodes;

  std::unordered_map<uint32_t, std::string> urls;
  std::unordered_map<std::string, uint32_t> urls_to_id;
  uint32_t next_url_id{0};

  size_t active_walks{0};
};

int main(int argc, char *argv[]) {
  spdlog::set_level(spdlog::level::debug);

  std::string scorerAddress = "localhost:1235";

  spdlog::info("read config");
  config settings = read_config();

  // two way vat

  kj::UnixEventPort::captureSignal(SIGINT);
  auto ioContext = kj::setupAsyncIo();

  auto addrPromise = ioContext.provider->getNetwork().parseAddress(scorerAddress)
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

    Scorer::Client master = rpcSystem.bootstrap(hostId).castAs<Scorer>();

    spdlog::info("creating client");

    ScorerWorker::Client worker = kj::heap<ScorerWorkerImpl>(ioContext, settings, master);

    spdlog::info("create request");
    auto request = master.registerScorerWorkerRequest();
    request.setWorker(worker);

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

