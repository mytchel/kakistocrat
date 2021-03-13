#include <stdio.h>
#include <unistd.h>

#include <cstdlib>
#include <cstring>

#include <chrono>
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
#include "tokenizer.h"

#include "hash_table.h"
#include "index.h"

using nlohmann::json;

void merge(std::string out, std::vector<std::string> &in)
{
  spdlog::info("starting {} with {}", out, in.size());

  search::index out_index(out);

  using namespace std::chrono_literals;

  for (auto &s: in) {
    spdlog::info("load {} for merging", s);

    auto start = std::chrono::system_clock::now();

    std::string path = "meta/sites/" + util::host_hash(s) + "/" + s;

    search::index site_index(path);
    site_index.load();

    auto mid = std::chrono::system_clock::now();

    std::chrono::nanoseconds load = mid - start;

    spdlog::info("merge {} index", s);
    out_index.merge(site_index);

    auto done = std::chrono::system_clock::now();

    std::chrono::nanoseconds merge = done - mid;

    spdlog::debug("load  took {:15}", load.count());
    spdlog::debug("merge took {:15}", merge.count());
  }

  spdlog::info("saving {}", out);
  out_index.save();
}

int main(int argc, char *argv[]) {
  crawl::crawler crawler;
  crawler.load();

  auto n_threads = std::thread::hardware_concurrency();

  spdlog::info("starting {} threads", n_threads);

  std::vector<std::string> in[n_threads];

  size_t i = 0;
  for (auto &site: crawler.sites) {
    if (site.last_scanned > 0) {
      in[i++ % n_threads].push_back(site.host);
    }
  }

  std::vector<std::thread> threads;
  std::vector<std::string> parts;

  for (size_t i = 0; i < n_threads; i++) {
    spdlog::info("part {} has {} sites", i, in[i].size());

    std::string out = fmt::format("part.{}", i);

    parts.emplace_back(out);

    auto th = std::thread(
        [](std::string o, std::vector<std::string> in) {
          merge(o, in);
        }, out, std::ref(in[i]));

    threads.emplace_back(std::move(th));
  }

  for (auto &t: threads) {
    t.join();
  }

  spdlog::info("now merge the parts");

  merge("meta/full", parts);

  spdlog::info("done");

  return 0;
}

