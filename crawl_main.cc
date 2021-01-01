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

#include <curl/curl.h>

#include "util.h"
#include "scrape.h"
#include "crawl.h"

int main(int argc, char *argv[]) {
  std::vector<std::string> blacklist = util::load_list("../mine/blacklist");
  std::vector<std::string> initial_seed = util::load_list("../mine/seed");

  crawl::index index;

  load_index(index, "full_index");

  crawl::insert_site_index_seed(index, initial_seed, blacklist);

  struct level {
    size_t max_sites;
    size_t max_pages;
  };

  curl_global_init(CURL_GLOBAL_DEFAULT);

  std::vector<struct level> levels = {{0, 2000}, {500, 20}, {100, 1}};
  //std::vector<struct level> levels = {{5, 50}, {20, 5}, {50, 1}};
  //std::vector<struct level> levels = {{0, 2}, {50, 2}, {50, 1}};
  size_t level_count = 1;

  crawl::save_index(index, "full_index");

  for (auto level: levels) {
    crawl::run_round(level_count++, levels.size() + 1,
        level.max_sites, level.max_pages, 
        index, blacklist);
  }

  for (int i = 0; i < 5; i++) {
    crawl::score_iteration(index);
    crawl::save_index(index, "full_index");
  }

  curl_global_cleanup();

  return 0;
}

