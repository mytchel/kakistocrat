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
#include "tokenizer.h"
#include "index.h"

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

class IndexerImpl final: public Indexer::Server {
public:
  IndexerImpl(const config &s)
    : settings(s) {}

  kj::Promise<void> index(IndexContext context) override {
    spdlog::info("got index request");

    size_t max_usage =
        settings.indexer.thread_max_mem
          - settings.indexer.max_index_part_size
          - settings.crawler.max_page_size;

    spdlog::info("create indexer with {} splits", settings.index_parts);

    search::indexer indexer(
        settings.index_parts,
        settings.indexer.htcap,
        settings.crawler.max_page_size,
        settings.indexer.max_index_part_size);

    spdlog::info("indexer created");

    struct output {
      std::string path;
      std::vector<std::string> sites;
    };

    std::list<output> outputs;
    outputs.emplace_back();

    std::string output_path = context.getParams().getOutputBase();

    auto base_path = fmt::format("{}/", output_path);

    util::make_path(base_path);

    size_t flush_count = 0;

    for (auto path: context.getParams().getSitePaths()) {
      spdlog::info("load  {}", std::string(path));

      if (indexer.usage() > max_usage) {
        auto &o = outputs.back();

        o.path = indexer.flush(
          fmt::format("{}/part.{}", base_path, flush_count++));

        // Setup the next output
        auto &n = outputs.emplace_back();
      }

      site_map site(path);
      site.load();

      spdlog::info("index {}", site.host);
      indexer.index_site(site, 
        [max_usage, &base_path, &path, &flush_count, &indexer, &outputs] () mutable {
          if (indexer.usage() > max_usage) {
            auto &o = outputs.back();

            o.path = indexer.flush(
              fmt::format("{}/part.{}", base_path, flush_count++));

            // Setup the next output
            auto &n = outputs.emplace_back();
            n.sites.emplace_back(path);
          }
        });

      auto &o = outputs.back();
      o.sites.emplace_back(path);

      spdlog::info("done  {}", site.host);
    }

    auto &o = outputs.back();
    if (!o.sites.empty()) {
      o.path = indexer.flush(
              fmt::format("{}/part.{}", base_path, flush_count++));
    } else {
      outputs.pop_back();
    }

    auto resultOutputs = context.getResults().initOutputs(outputs.size());

    size_t i = 0;
    for (auto &o: outputs) {
      auto r = resultOutputs[i++];

      r.setPath(o.path);

      auto s = r.initSites(o.sites.size());
      for (size_t j = 0; j < o.sites.size(); j++) {
        s.set(j, o.sites[j]);
      }
    }

    return kj::READY_NOW;
  }

  const config &settings;
};

int main(int argc, char *argv[]) {
  spdlog::set_level(spdlog::level::debug);

  std::string bindAddress = "localhost:1234";

  spdlog::info("read config");
  config settings = read_config();

  // two way vat

  kj::UnixEventPort::captureSignal(SIGINT);
  auto ioContext = kj::setupAsyncIo();

  auto addrPromise = ioContext.provider->getNetwork().parseAddress(bindAddress)
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

    Indexer::Client indexer = kj::heap<IndexerImpl>(settings);

    spdlog::info("create request");
    auto request = master.registerIndexerRequest();
    request.setIndexer(indexer);

    spdlog::info("send indexer register");

    auto r = request.send().then(
        [] (auto result) {
          spdlog::info("indexer registered");
        },
        [] (auto exception) {
          spdlog::warn("exception registering indexer : {}", std::string(exception.getDescription()));
        });

    spdlog::info("waiting for register");
    r.wait(ioContext.waitScope);

    spdlog::info("waiting for sigint");
    ioContext.unixEventPort.onSignal(SIGINT).wait(ioContext.waitScope);
  }

  return 0;

  // ez rpc

  /*
  spdlog::info("start client");
  capnp::EzRpcClient client(argv[1]);

  auto &waitScope = client.getWaitScope();

  spdlog::info("get master");
  Master::Client master = client.getMain<Master>();

  spdlog::info("make indexer");

  //kj::Own<Indexer::Server> indexer_server = kj::heap<IndexerImpl>();
  //Indexer::Client indexer(kj::mv(indexer_server));// = kj::heap<IndexerImpl>();

  spdlog::info("create request");
  auto request = master.registerIndexerRequest();
  request.setIndexer(kj::heap<IndexerImpl>());

  spdlog::info("send indexer register");

  auto r = request.send().then(
      [] (auto result) {
        spdlog::info("indexer registered");
      },
      [] (auto exception) {
        spdlog::warn("exception registering indexer");
        spdlog::warn("exception registering indexer : {}", std::string(exception.getDescription()));
      });

  spdlog::info("waiting for register");
  r.wait(waitScope);

  spdlog::info("waiting forever");
  kj::NEVER_DONE.wait(waitScope);
  spdlog::info("done");

  return 0;
  */
}

