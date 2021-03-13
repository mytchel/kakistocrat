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

#include <nlohmann/json.hpp>
#include "spdlog/spdlog.h"

#include "util.h"
#include "crawl.h"

int main(int argc, char *argv[]) {
  spdlog::set_level(spdlog::level::debug);

  std::vector<std::string> blacklist = util::load_list("blacklist");
  std::vector<std::string> initial_seed = util::load_list("seed");

  std::vector<crawl::level> levels = {{5000, 10000}, {100, 100}, {1, 0}};
  //std::vector<crawl::level> levels = {{50, 5}, {10, 1}, {1, 0}};
  //std::vector<crawl::level> levels = {{10, 0}};

  crawl::crawler crawler(levels);

  crawler.load();

  crawler.load_blacklist(blacklist);
  crawler.load_seed(initial_seed);

  curl_global_init(CURL_GLOBAL_ALL);

  crawler.crawl();

  curl_global_cleanup();

  return 0;
}

