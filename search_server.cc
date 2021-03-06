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

#include <nlohmann/json.hpp>
#include <crow.h>

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

using namespace nlohmann;

int main(int argc, char *argv[]) {
  scorer::scores index_scores;

  index_scores.load("index.scores");

  search::searcher searcher;

  searcher.load("index.dat");

  crow::SimpleApp app;

  CROW_ROUTE(app, "/json")([&index_scores, &searcher](const crow::request &req){
    auto query = req.url_params.get("q");
    if (query == nullptr) {
      crow::json::wvalue response;
      response["error"] = "nothing given";
      return response;
    }

    CROW_LOG_INFO << "query " << query;

    char query_c[1024];
    strncpy(query_c, query, sizeof(query_c));

    auto results = searcher.search(query_c, index_scores);

    crow::json::wvalue j;

    std::vector<crow::json::wvalue> json_results;
    for (auto &result: results) {
      crow::json::wvalue jj;

      jj["page_id"] = result.page_id;
      jj["score"] = result.score;
      jj["url"] = result.url;
      jj["title"] = result.title;
      jj["path"] = result.path;

      json_results.push_back(std::move(jj));
    }

    crow::json::wvalue response;
    response["query"] = query;
    response["results"] = std::move(json_results);

    return response;
  });

  auto search_page = crow::mustache::load("search.html");

  CROW_ROUTE(app, "/")([&search_page, &index_scores, &searcher](const crow::request &req){
    crow::mustache::context ctx;

    auto query = req.url_params.get("q");
    if (query == nullptr) {
      return search_page.render(ctx);
    }

    CROW_LOG_INFO << "query " << query;

    char query_c[1024];
    strncpy(query_c, query, sizeof(query_c));

    auto results = searcher.search(query_c, index_scores);

    std::vector<crow::json::wvalue> results_r;
    for (auto &result: results) {
      crow::json::wvalue jj;

      jj["page_id"] = result.page_id;
      jj["score"] = result.score;
      jj["url"] = result.url;
      jj["title"] = result.title;
      jj["path"] = result.path;

      results_r.push_back(std::move(jj));
    }

    ctx["query"] = query;
    ctx["results"] = std::move(results_r);

    return search_page.render(ctx);
  });

  app.port(18080).multithreaded().run();

  return 0;
}

