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

int main(int argc, char *argv[]) {
  crawl::crawler crawler;
  crawler.load();

  search::index full_index("full");

  using namespace std::chrono_literals;

  std::chrono::nanoseconds load_total = 0ms;
  std::chrono::nanoseconds merge_total = 0ms;

  auto site = crawler.sites.begin();
  while (site != crawler.sites.end()) {
    if (site->last_scanned == 0) {
      site++;
      continue;
    }

    printf("load %s for merging\n", site->host.c_str());

    auto start = std::chrono::system_clock::now();

    search::index site_index(site->host);
    site_index.load();

    auto mid = std::chrono::system_clock::now();

    std::chrono::nanoseconds load = mid - start;

    printf("merge %s index\n", site->host.c_str());
    full_index.merge(site_index);

    auto done = std::chrono::system_clock::now();

    std::chrono::nanoseconds merge = done - mid;

    printf("load  took %15lu\n", load.count());
    printf("merge took %15lu\n", merge.count());

    load_total += load;
    merge_total += merge;

    site++;
  }

  printf("saving\n");
  full_index.save();

  printf("total load  took %15lu\n", load_total.count());
  printf("total merge took %15lu\n", merge_total.count());

  return 0;
}

