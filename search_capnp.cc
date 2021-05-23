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

  struct adaptor {
  public:
    adaptor(kj::PromiseFulfiller<void> &fulfiller,
            SearcherImpl &searcher,
            const std::string &query,
            Response& response)
        : fulfiller(fulfiller),
          searcher(searcher),
          query(query),
          response(response)
    {
      if (query != "") {
        search();
      } else {
        respond();
      }
    }

    void search() {
      char query_c[1024];
      strncpy(query_c, query.c_str(), sizeof(query_c));

      auto matched_pages = searcher.searcher.search(query_c);

      if (matched_pages.empty()) {
        respond();
        return;
      }

      pending = 0;
      for (auto &page: matched_pages) {
        auto request = searcher.master.getPageInfoRequest();

        request.setUrl(page.url);

        searcher.tasks.add(request.send().then(
              [this, page] (auto result) {
                spdlog::info("got page info for {}", page.url);

                float score = result.getScore();
                std::string title = result.getTitle();
                std::string path = result.getPath();

                spdlog::info("got page info for {} : {} '{}' '{}'", page.url, score, title, path);

                if (title != "" && path != "") {
                  results.emplace_back(
                      page.url,
                      result.getTitle(),
                      result.getPath(),
                      page.score,
                      result.getScore());
                }

                pending--;
                if (pending == 0) {
                  respond();
                }
              }));

        pending++;
        if (pending > 10) {
          break;
        }
      }
    }

    void respond() {
      std::string result_body;

      std::sort(results.begin(), results.end(),
          [] (auto &a, auto &b) {
              double aa = a.score * a.rank;
              double bb = b.score * b.rank;

              return aa > bb;
          });

      for (auto &result: results) {
        result_body += fmt::format("<li>{:.2f} : {:.8f}  <a href=\"{}\">{}</a>  <a href=\"{}\">{}</a></li>",
            result.score,
            result.rank,
            result.url,
            result.title,
            result.url,
            result.path);
      }

      body = kj::str(fmt::format(R"HTML(
  <html>
      <head>
          <title>{} | Search</title>
      </head>
      <body>
          <div id="search">
              <form method="GET" action="/">
                  <label for="q">Search</label>
                  <input type="text" id="q" name="q" value="{}"></input>
                  <input type="submit" value="Search"></input>
              </form>
          </div>
          <div id="results">
              <ul>
              {}
              </ul>
          </div>
      </body>
  </html>
          )HTML", std::string(query), std::string(query), result_body));

      kj::HttpHeaders respHeaders(*searcher.hTable);
      respHeaders.set(searcher.hContentType, "text/html");

      stream = response.send(200, "OK", respHeaders, body.size());
      searcher.tasks.add(stream->write(body.begin(), body.size()).then(
            [this] () {
              spdlog::info("finished sending");
              finish();
            }));
    }

    void finish() {
      fulfiller.fulfill();
    }

    kj::PromiseFulfiller<void> &fulfiller;
    SearcherImpl &searcher;
    std::string query;
    Response &response;

    struct search_match {
      std::string url;
      std::string title;
      std::string path;
      float score;
      float rank;

      search_match(const std::string &url,
                   const std::string &title,
                   const std::string &path,
                   float score, float rank)
        : url(url), title(title), path(path),
          score(score), rank(rank)
      {}
    };

    size_t pending;
    std::vector<search_match> results;

    kj::String body;
    kj::Own<kj::AsyncOutputStream> stream;
  };

public:
  SearcherImpl(kj::AsyncIoContext &io_context,
      kj::HttpHeaderTable::Builder &builder,
      kj::Own<kj::NetworkAddress> &listenAddr,
      const config &s, Master::Client master)
    : settings(s), searcher(s), master(master),
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

    std::string query = params.getWord();

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

    return kj::newAdaptedPromise<void, adaptor>(*this, query, response);
  }

  void taskFailed(kj::Exception&& exception) override {
    spdlog::warn("task failed: {}", std::string(exception.getDescription()));
    kj::throwFatalException(kj::mv(exception));
  }

  const config &settings;

  search::searcher searcher;

  Master::Client master;

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
    capnp::MallocMessageBuilder message;
    auto hostId = message.getRoot<capnp::rpc::twoparty::VatId>();
    hostId.setSide(capnp::rpc::twoparty::Side::SERVER);

    Master::Client master = rpcSystem.bootstrap(hostId).castAs<Master>();

    spdlog::info("creating client");

    kj::HttpHeaderTable::Builder builder;

    Searcher::Client searcher = kj::heap<SearcherImpl>(ioContext,
        builder,
        listenAddr,
        settings,
        master);

    spdlog::info("client made");

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

