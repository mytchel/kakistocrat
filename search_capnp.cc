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
#include "index.h"
#include "search.h"

#include "indexer.capnp.h"

#include <kj/debug.h>
#include <kj/string.h>
#include <kj/async-io.h>
#include <kj/async-unix.h>

#include <kj/compat/http.h>
#include <kj/compat/url.h>

#include <capnp/rpc.h>
#include <capnp/rpc-twoparty.h>
#include <capnp/ez-rpc.h>
#include <capnp/message.h>
#include <iostream>

using nlohmann::json;

class SearcherImpl final:
            public Searcher::Server,
            public kj::HttpService,
            public kj::TaskSet::ErrorHandler {

public:
  SearcherImpl(kj::AsyncIoContext &io_context,
      kj::HttpHeaderTable::Builder &builder,
      kj::Own<kj::NetworkAddress> &listenAddr,
      const config &s)
    : settings(s), searcher(s),
      urlBase(kj::Url::parse("http://localhost/")),
      tasks(*this), timer(io_context.provider->getTimer())
  {
    searcher.load();

    hAccept = builder.add("Accept");
    hContentType = builder.add("Content-Type");

    hTable = builder.build();

    server = kj::heap<kj::HttpServer>(timer, *hTable, *this);

    receiver = listenAddr->listen();

    tasks.add(server->listenHttp(*receiver));
  }

  kj::Promise<void> search(SearchContext context) override {
    spdlog::info("got search request");

    auto params = context.getParams();

    std::string query = params.getQuery();

    return kj::READY_NOW;
  }

  kj::Promise<void> request(
      kj::HttpMethod method, kj::StringPtr urlStr, const kj::HttpHeaders& headers,
      kj::AsyncInputStream& requestBody, Response& response) override
  {
    spdlog::info("got http request for {} : {}", method, std::string(urlStr));

    // Definatly move http server out of c++

    auto url = urlBase.parseRelative(urlStr);

    kj::StringPtr query;

    for (auto &u: url.query) {
      if (u.name == "q") {
        query = u.value;
      }
    }

    kj::HttpHeaders respHeaders(*hTable);
    respHeaders.set(hContentType, "text/html");

    if (query == "") {
      auto body = kj::str(fmt::format("Hello there. I need something to work with"));

      auto stream = response.send(200, "OK", respHeaders, body.size());
      auto promise = stream->write(body.begin(), body.size());
      return promise.attach(kj::mv(stream), kj::mv(body));
    }

    char query_c[1024];
    strncpy(query_c, query.cStr(), sizeof(query_c));

    auto results = searcher.search(query_c);

    std::string body = "<h1>got stuff</h1><ul>";
    for (auto &result: results) {
      body += fmt::format("<li>{}:<a href=\"{}\">{}</a>:<a href=\"{}\">{}</a></li>",
          result.score,
          result.url,
          result.title,
          result.url,
          result.path);
    }

    body += "</ul>";

    auto bodyStr = kj::str(body);

    auto stream = response.send(200, "OK", respHeaders, bodyStr.size());
    auto promise = stream->write(bodyStr.begin(), bodyStr.size());
    return promise.attach(kj::mv(stream), kj::mv(bodyStr));
  }

  void taskFailed(kj::Exception&& exception) override {
    spdlog::warn("task failed: {}", std::string(exception.getDescription()));
    kj::throwFatalException(kj::mv(exception));
  }

  //std::string message = "Hello there.";

  const config &settings;

  search::searcher searcher;

  kj::Url urlBase;

  kj::TaskSet tasks;

  kj::Timer &timer;

  kj::HttpHeaderId hAccept;
  kj::HttpHeaderId hContentType;

  kj::Own<kj::HttpHeaderTable> hTable;
  kj::Own<kj::HttpServer> server;
  kj::Own<kj::ConnectionReceiver> receiver;
};

int main(int argc, char *argv[]) {
  spdlog::set_level(spdlog::level::debug);

  std::string bindAddress = "localhost:1234";
  std::string listenAddress = "localhost:8080";

  spdlog::info("read config");
  config settings = read_config();

  // two way vat

  kj::UnixEventPort::captureSignal(SIGINT);
  auto ioContext = kj::setupAsyncIo();

  auto listenAddrPromise = ioContext.provider->getNetwork().parseAddress(listenAddress, 8080);

  auto listenAddr = listenAddrPromise.wait(ioContext.waitScope);
  spdlog::info("listen addr {}", std::string(listenAddr->toString().cStr()));

  auto addrPromise = ioContext.provider->getNetwork().parseAddress(bindAddress)
  .then([](kj::Own<kj::NetworkAddress> addr) {
      spdlog::info("using addr {}", std::string(addr->toString().cStr()));
      return addr->connect().attach(kj::mv(addr));
  });

  auto stream = addrPromise.wait(ioContext.waitScope);

  capnp::TwoPartyVatNetwork network(*stream, capnp::rpc::twoparty::Side::CLIENT);

  auto rpcSystem = capnp::makeRpcClient(network);

  {
    spdlog::info("creating client");

    kj::HttpHeaderTable::Builder builder;

    Searcher::Client searcher = kj::heap<SearcherImpl>(ioContext,
        builder,
        listenAddr,
        settings);

    spdlog::info("client made");

    capnp::MallocMessageBuilder message;
    auto hostId = message.getRoot<capnp::rpc::twoparty::VatId>();
    hostId.setSide(capnp::rpc::twoparty::Side::SERVER);

    Master::Client master = rpcSystem.bootstrap(hostId).castAs<Master>();

    spdlog::info("create request");
    auto request = master.registerSearcherRequest();
    request.setSearcher(searcher);

    spdlog::info("send searcher register");

    auto r = request.send().then(
        [] (auto result) {
          spdlog::info("searcher registered");
        },
        [] (auto exception) {
          spdlog::warn("exception registering searcher : {}", std::string(exception.getDescription()));
        });

    spdlog::info("waiting for register");
    r.wait(ioContext.waitScope);



    spdlog::info("waiting for sigint");
    ioContext.unixEventPort.onSignal(SIGINT).wait(ioContext.waitScope);
  }

  return 0;
}

