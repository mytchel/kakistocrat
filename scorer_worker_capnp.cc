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

    ScorerWorkerImpl &worker;
    std::string url;
    std::vector<std::pair<std::string, size_t>> links;

    size_t next_coupons{0};

    size_t coupons{0};
    uint32_t counter{0};

    node(ScorerWorkerImpl &worker, const page &p)
      : worker(worker), url(p.url), links(p.links.begin(), p.links.end())
    {
      for (auto &l: links) {
        spdlog::info("link {} -> {}", url, l.first);
      }
    }

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

    void iterate() {
      spdlog::info("iterate {}, {} = {}", counter, coupons, url);

      if (links.empty()) {
        return;
      }

      for (size_t i = 0; i < coupons; i++) {
        if (util::get_rand() > 1 - worker.param_e) {
          spdlog::info("iterate early stop after {}", i);
          break;
        }

        // TODO: link frequency
        // and only do add walk at the end with the sum
        size_t l = util::get_rand() * links.size();
        worker.addWalk(links[l].first, 1);
      }
    }
  };

public:
  ScorerWorkerImpl(const config &s, Scorer::Client master)
    : settings(s), tasks(*this), master(master)
  {
  }

  kj::Promise<void> getCounter(GetCounterContext context) override {
    auto params = context.getParams();
    std::string url = params.getUrl();

    spdlog::info("get score {}", url);

    auto it = nodes.find(url);
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
        nodes.emplace(p.url, node(*this, p));
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
    for (auto &node: nodes) {
      node.second.iterate();
    }

    return kj::READY_NOW;
  }

  kj::Promise<void> iterateFinish(IterateFinishContext context) override {
    bool r = false;
    for (auto &node: nodes) {
      r |= node.second.iterate_finish();
    }

    context.getResults().setRunning(r);

    return kj::READY_NOW;
  }

  void addWalk(const std::string &url, uint32_t hits) {
    auto it = nodes.find(url);
    if (it != nodes.end()) {
      it->second.add(hits);

    } else {
      auto request = master.addWalkRequest();
      request.setUrl(url);
      request.setHits(hits);

      tasks.add(request.send().then(
            [this] (auto result) {
              auto r = result.getFound();
              //spdlog::info("remote add walk got {}", r);
            }));
    }
  }

  kj::Promise<void> addWalk(AddWalkContext context) override {
    auto params = context.getParams();
    auto site = params.getSite();
    std::string url = params.getUrl();
    auto hits = params.getHits();

    auto it = nodes.find(url);
    if (it != nodes.end()) {
      spdlog::info("got remote walk for {}", url);
      it->second.add(hits);
    }

    return kj::READY_NOW;
  }

  void taskFailed(kj::Exception&& exception) override {
    spdlog::warn("task failed: {}", std::string(exception.getDescription()));
    kj::throwFatalException(kj::mv(exception));
  }

  const config &settings;

  Scorer::Client master;

  kj::TaskSet tasks;

  float param_e;

  std::unordered_map<std::string, node> nodes;
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

    ScorerWorker::Client worker = kj::heap<ScorerWorkerImpl>(settings, master);

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

