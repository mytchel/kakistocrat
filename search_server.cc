#include <stdio.h>
#include <unistd.h>
#include <math.h>
#include <signal.h>

#include <cstdlib>
#include <cstring>

#include <vector>
#include <list>
#include <set>
#include <map>
#include <string>
#include <algorithm>
#include <future>
#include <optional>
#include <iostream>
#include <fstream>
#include <sstream>
#include <cstdint>

#include <pistache/http.h>
#include <pistache/router.h>
#include <pistache/endpoint.h>

#include <nlohmann/json.hpp>

extern "C" {

#include "str.h"
#include "x_cocomel/dynamic_array_kv_64.h"
#include "x_cocomel/dynamic_array_kv_32.h"
#include "x_cocomel/dynamic_array_64.h"
#include "x_cocomel/vbyte.h"
#include "x_cocomel/posting.h"
#include "x_cocomel/hash_table.h"

}

#include "util.h"
#include "crawl.h"
#include "scorer.h"
#include "tokenizer.h"
#include "search.h"

using namespace Pistache;
using namespace nlohmann;

class SearchEndpoint {
  public:

  SearchEndpoint(Address addr, search::searcher searcher_, scorer::scores scores_)
        : httpEndpoint(std::make_shared<Http::Endpoint>(addr)),
          searcher(searcher_), scores(scores_) {}

  void init(size_t thr = 2) {
    auto opts = Http::Endpoint::options()
        .threads(static_cast<int>(thr));
    httpEndpoint->init(opts);
    setupRoutes();
  }

  void start() {
    httpEndpoint->setHandler(router.handler());
    httpEndpoint->serveThreaded();
  }

  void shutdown() {
    httpEndpoint->shutdown();
  }

  private:

  void setupRoutes() {
    using namespace Rest;

    Routes::Get(router, "/search", Routes::bind(&SearchEndpoint::search, this));
    Routes::Get(router, "/ready", Routes::bind(&SearchEndpoint::handleReady, this));
  }

  void handleReady(const Rest::Request&, Http::ResponseWriter response) {
    response.send(Http::Code::Ok, "{}");
  }

  void search(const Rest::Request& request, Http::ResponseWriter response) {
    auto query_args = request.query();

    auto query_encoded = query_args.get("q");
    if (query_encoded.isEmpty()) {
      response.send(Http::Code::Ok, "{}");
      return;
    }

    auto query = util::url_decode(query_encoded.get());

    char query_c[1024];
    strncpy(query_c, query.c_str(), sizeof(query_c));

    auto results = searcher.search(query_c, scores);

    json j;

    for (auto &result: results) {
      json jj;
      jj["page_id"] = result.page_id;
      jj["score"] = result.score;
      jj["url"] = result.url;
      jj["title"] = result.title;
      jj["path"] = result.path;

      j.push_back(jj);
    }

    json jj;
    jj["query"] = query;
    jj["results"] = j;

    response.send(Http::Code::Ok, jj.dump());
  }

  std::shared_ptr<Http::Endpoint> httpEndpoint;
  Rest::Router router;
  search::searcher searcher;
  scorer::scores scores;
};

int main(int argc, char *argv[]) {
  sigset_t signals;
  if (sigemptyset(&signals) != 0
      || sigaddset(&signals, SIGTERM) != 0
      || sigaddset(&signals, SIGINT) != 0
      || sigaddset(&signals, SIGHUP) != 0
      || pthread_sigmask(SIG_BLOCK, &signals, nullptr) != 0) {
    printf("install signal handler failed\n");
    return 1;
  }

  scorer::scores index_scores;

  index_scores.load("index.scores");

  search::searcher searcher;

  searcher.load("index.dat");

  Address addr(Ipv4::any(), Port(9081));
  int thr = 2;

  SearchEndpoint server(addr, searcher, index_scores);
  server.init(thr);
  server.start();

  int signal = 0;
  sigwait(&signals, &signal);

  printf("got signal\n");

  server.shutdown();

  printf("shutdown\n");

  return 0;
}

