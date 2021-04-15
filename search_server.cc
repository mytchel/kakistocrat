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
#include <spdlog/spdlog.h>
#include <crow.h>

#include "util.h"
#include "config.h"
#include "crawl.h"
#include "scorer.h"
#include "tokenizer.h"
#include "search.h"

using namespace nlohmann;

int main(int argc, char *argv[]) {
  config c = read_config();

  size_t file_buf_len = c.crawler.max_page_size;

  search::searcher searcher(c);

  searcher.load();

  crow::SimpleApp app;

  CROW_ROUTE(app, "/json")([&searcher](const crow::request &req){
    auto query = req.url_params.get("q");
    if (query == nullptr) {
      crow::json::wvalue response;
      response["error"] = "nothing given";
      return response;
    }

    spdlog::info("json query {}", query);

    char query_c[1024];
    strncpy(query_c, query, sizeof(query_c));

    auto results = searcher.search(query_c);

    crow::json::wvalue j;

    std::vector<crow::json::wvalue> json_results;
    for (auto &result: results) {
      if (result.title == "") {
        continue;
      }

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

  CROW_ROUTE(app, "/")([&search_page, &searcher](const crow::request &req){
    crow::mustache::context ctx;

    auto query = req.url_params.get("q");
    if (query == nullptr) {
      return search_page.render(ctx);
    }

    spdlog::info("query {}", query);

    char query_c[1024];
    strncpy(query_c, query, sizeof(query_c));

    auto results = searcher.search(query_c);

    std::vector<crow::json::wvalue> results_r;
    for (auto &result: results) {
      if (result.title == "") {
        continue;
      }

      crow::json::wvalue jj;

      jj["page_id"] = result.page_id;
      jj["score"] = result.score;
      jj["url"] = result.url;
      jj["title"] = result.title;
      jj["path"] = result.path;
      jj["archive_url"] = fmt::format("/archive/?u={}", result.url);

      results_r.push_back(std::move(jj));
    }

    ctx["query"] = query;
    ctx["results"] = std::move(results_r);

    return search_page.render(ctx);
  });

  CROW_ROUTE(app, "/archive/")([&search_page, &searcher, file_buf_len](const crow::request &req){
    crow::mustache::context ctx;

    auto url = req.url_params.get("u");
    if (url == nullptr) {
      crow::response response(400, "bad input");
      return response;
    }

    spdlog::info("archive {}", url);

    auto p = searcher.scores.find_page(url);
    if (p == nullptr) {
      spdlog::info("archive {} not found", url);
      crow::response response(404, fmt::format("archive {} not found", url));
      return response;
    }

    char *file_buf = (char *) malloc(file_buf_len);
    if (file_buf == NULL) {
      throw std::bad_alloc();
    }

    spdlog::info("archive {} read file {}", url, p->path);

    std::ifstream pfile;

    pfile.open(p->path, std::ios::in | std::ios::binary);

    if (!pfile.is_open() || pfile.fail() || !pfile.good() || pfile.bad()) {
      spdlog::warn("error opening file {}", p->path);
      free(file_buf);

      crow::response response(400, "error opening file");
      return response;
    }

    pfile.read(file_buf, file_buf_len);

    size_t len = pfile.gcount();

    crow::response response(std::string(file_buf, file_buf + len));

    free(file_buf);

    return response;
  });

  app.port(18080).multithreaded().run();

  return 0;
}

