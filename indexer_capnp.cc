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
  IndexerImpl(const config &s, size_t tid)
    : settings(s),
      indexer(
        fmt::format("{}/{}/part", settings.indexer.parts_path, tid),
        search::get_split_at(settings.index_parts),
        settings.indexer.htcap,
        settings.indexer.thread_max_mem
          - settings.indexer.max_index_part_size
          - settings.crawler.max_page_size,
        settings.indexer.max_index_part_size)
  {
    indexer.on_flush = [this](const std::string &path) {
      output_paths.push_back(path);
    };

    file_buf_len = settings.crawler.max_page_size;
    file_buf = (char *) malloc(file_buf_len);
    if (file_buf == NULL) {
      throw std::bad_alloc();
    }
  }

  ~IndexerImpl() {
    if (file_buf) {
      free(file_buf);
    }
  }

  kj::Promise<void> index(IndexContext context) override {
    spdlog::info("got index request");

    std::string path = context.getParams().getSitePath();

    site_count++;

    spdlog::info("load  {}", path);

    site_map site(path);
    site.load();

    spdlog::info("index {}", site.host);
    indexer.index_site(site, file_buf, file_buf_len);

    spdlog::info("done  {}", site.host);

    return kj::READY_NOW;
  }

  kj::Promise<void> flush(FlushContext context) override {
    spdlog::info("got flush request");

    indexer.flush();

    spdlog::info("return {} paths for {} sites", output_paths.size(), site_count);

    auto paths = context.getResults().initOutputPaths(output_paths.size());

    size_t i = 0;
    for (auto &p: output_paths) {
      paths.set(i++, p);
    }

    indexer.reset();

    site_count = 0;
    output_paths.clear();

    return kj::READY_NOW;
  }

  const config &settings;
  search::indexer indexer;

  size_t file_buf_len;
  char *file_buf;

  size_t site_count;
  std::list<std::string> output_paths;
};

int main(int argc, char *argv[]) {
  spdlog::set_level(spdlog::level::debug);

  if (argc != 3) {
    spdlog::error("bad args");
    return 1;
  }

  spdlog::info("read config");
  config settings = read_config();

  // two way vat

  kj::UnixEventPort::captureSignal(SIGINT);
  auto ioContext = kj::setupAsyncIo();

  auto addrPromise = ioContext.provider->getNetwork().parseAddress(argv[1], 2572)
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

    Indexer::Client indexer = kj::heap<IndexerImpl>(settings, std::stoi(argv[2]));

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

