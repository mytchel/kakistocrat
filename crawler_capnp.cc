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

#include "channel.h"
#include "util.h"
#include "scrape.h"
#include "crawl.h"
#include "crawler.h"
#include "tokenizer.h"

#include "index.h"

#include "curl_kj.h"

#include "indexer.capnp.h"

#include <kj/debug.h>
#include <kj/string.h>
#include <kj/async-io.h>
#include <kj/async-unix.h>
#include <kj/compat/http.h>

#include <capnp/rpc.h>
#include <capnp/rpc-twoparty.h>
#include <capnp/ez-rpc.h>
#include <capnp/message.h>
#include <iostream>

using nlohmann::json;

class CrawlerImpl final: public Crawler::Server,
                         public kj::TaskSet::ErrorHandler {

public:
  /*
  CrawlerImpl(const config &settings, kj::Timer &timer,
      kj::Network &network, kj::Network &tls_network)
    : settings(settings), timer(timer),
  {
    kj::HttpHeaderTable::Builder builder;

    http_accept = builder.add("Accept");
    http_last_modified = builder.add("Last-Modified");
    http_content_type = builder.add("Content-Type");

    http_header_table table(kj::mv(builder));

    http_client = newHttpClient(timer, http_header_table, network, tls_network);
  }
*/

  CrawlerImpl(const config &settings, kj::AsyncIoContext &io_context)
    : settings(settings), tasks(*this),
      curl(io_context, settings.crawler.thread_max_connections)
  {

  }

  kj::Promise<void> crawl(CrawlContext context) override {
    spdlog::info("got crawl request");

    auto params = context.getParams();

    std::string path = params.getSitePath();
    size_t max_pages = params.getMaxPages();

    spdlog::info("load  {}", path);

    crawl::site site(path);
    site.load();

    if (site.pages.empty()) {
      spdlog::info("nothing to do  {}", site.host);
      return kj::READY_NOW;
    }

    size_t buf_max = 1024 * 1024;
    uint8_t *buf = (uint8_t *) malloc(buf_max);

    auto page = site.pages.front();
    return curl.add(page.url, buf, buf_max).then(
          [this, context] (curl_response result) {
            spdlog::info("page done, request finished {} : {} bytes", result.success, result.size);
          });
  }

  void taskFailed(kj::Exception&& exception) override {
    spdlog::warn("task failed: {}", std::string(exception.getDescription()));
    kj::throwFatalException(kj::mv(exception));
  }

  const config &settings;
  kj::TaskSet tasks;

  /*
  kj::Timer &timer;

  HttpHeaderId http_accept;
  HttpHeaderId http_last_modified;
  HttpHeaderId http_content_type;

  kj::HttpHeaderTable http_header_table;
  kj::Own<HttpClient> http_client;
  */

  curl_kj curl;
};

int main(int argc, char *argv[]) {
  spdlog::set_level(spdlog::level::debug);

  if (argc != 2) {
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

    Crawler::Client crawler = kj::heap<CrawlerImpl>(settings, ioContext);

    spdlog::info("create request");
    auto request = master.registerCrawlerRequest();
    request.setCrawler(crawler);

    spdlog::info("send crawler register");

    auto r = request.send().then(
        [] (auto result) {
          spdlog::info("crawler registered");
        },
        [] (auto exception) {
          spdlog::warn("exception registering crawler : {}", std::string(exception.getDescription()));
        });

    spdlog::info("waiting for register");
    r.wait(ioContext.waitScope);

    spdlog::info("waiting for sigint");
    ioContext.unixEventPort.onSignal(SIGINT).wait(ioContext.waitScope);
  }

  return 0;
}

