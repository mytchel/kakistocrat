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
#include "config.h"
#include "crawl.h"

int main(int argc, char *argv[]) {
  spdlog::set_level(spdlog::level::debug);

  config c = read_config();

  std::vector<std::string> blacklist = util::load_list(c.blacklist_path);
  std::vector<std::string> initial_seed = util::load_list(c.seed_path);

  crawl::crawler crawler(c);

  crawler.load();

  crawler.load_blacklist(blacklist);
  crawler.load_seed(initial_seed);

  curl_global_init(CURL_GLOBAL_ALL);

  crawler.crawl();

  curl_global_cleanup();

  return 0;
}

