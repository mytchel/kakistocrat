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
  printf("starting %s with %i\n", out.c_str(), in.size());

  search::index out_index(out);

  using namespace std::chrono_literals;

  for (auto &s: in) {
    printf("load %s for merging\n", s.c_str());

    auto start = std::chrono::system_clock::now();

    search::index site_index(s);
    site_index.load();

    auto mid = std::chrono::system_clock::now();

    std::chrono::nanoseconds load = mid - start;

    printf("merge %s index\n", s.c_str());
    out_index.merge(site_index);

    auto done = std::chrono::system_clock::now();

    std::chrono::nanoseconds merge = done - mid;

    printf("load  took %15lu\n", load.count());
    printf("merge took %15lu\n", merge.count());
  }

  printf("saving %s\n", out.c_str());
  out_index.save();
}

int main(int argc, char *argv[]) {
  crawl::crawler crawler;
  crawler.load();

  auto n_threads = std::thread::hardware_concurrency();

  printf("starting %i threads\n", n_threads);

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
    printf("part %i has %i sites\n", i, in[i].size());

    char *buf = (char*)malloc(250);
    sprintf(buf, "part.%i", i);

    std::string out(buf);

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

  printf("now merge the parts\n");

  merge("full", parts);

  printf("done\n");

  return 0;
}

