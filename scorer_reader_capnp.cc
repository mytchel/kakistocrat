#include "math.h"

#include <stdio.h>
#include <unistd.h>
#include <fcntl.h>

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
#include <capnp/serialize-packed.h>
#include <iostream>

using nlohmann::json;

class ScoreReaderImpl final: public ScoreReader::Server,
                              public kj::TaskSet::ErrorHandler {

public:
  ScoreReaderImpl(kj::AsyncIoContext &io_context, const config &s, Scorer::Client master)
    : settings(s), tasks(*this), master(master)
  {
  }

  kj::Promise<void> getCounter(GetCounterContext context) override {
    auto params = context.getParams();
    std::string url = params.getUrl();

    uint32_t id = urlToId(url, false);
    if (id != 0) {
      auto it = counters.find(id);
      if (it != counters.end()) {
        spdlog::info("get score {} = {}", url, it->second);
        context.getResults().setCounter(it->second);
      }
    }

    return kj::READY_NOW;
  }

  kj::Promise<void> load(LoadContext context) override {
    counters.clear();

    auto params = context.getParams();
    
    output_path = params.getPath();

    spdlog::debug("load {}", output_path);

    int fd = open(output_path.c_str(), O_RDONLY);
    if (fd < 0) {
      spdlog::warn("failed to open {}", output_path);
      return kj::READY_NOW;
    }

    ::capnp::PackedFdMessageReader message(fd);

    ScoreBlock::Reader reader = message.getRoot<ScoreBlock>();

    std::set<std::string> hosts;

    for (auto n: reader.getNodes()) {
      std::string url = n.getUrl();
      uint32_t counter = n.getCounter();

      hosts.emplace(util::get_host(url));

      counters.emplace(urlToId(url, true), counter);
    }

    // Or earlier?
    close(fd);

    auto results = context.getResults();
    auto resultHosts = results.initHosts(hosts.size());

    size_t i = 0;
    for (auto &h: hosts) {
      resultHosts.set(i++, h);
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

  std::string output_path;

  std::unordered_map<uint32_t, uint32_t> counters;

  std::unordered_map<uint32_t, std::string> urls;
  std::unordered_map<std::string, uint32_t> urls_to_id;
  uint32_t next_url_id{0};
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

    ScoreReader::Client reader = kj::heap<ScoreReaderImpl>(ioContext, settings, master);

    spdlog::info("create request");
    auto request = master.registerScoreReaderRequest();
    request.setReader(reader);

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

