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
#include "crawl.h"
#include "crawler.h"

int main(int argc, char *argv[]) {
  std::vector<std::string> blacklist = util::load_list("blacklist");
  std::vector<std::string> initial_seed = util::load_list("seed");

  crawl::index index;

  index.load("index.scrape");

  crawl::insert_site_index_seed(index, initial_seed, blacklist);

  curl_global_init(CURL_GLOBAL_ALL);

  //std::vector<crawl::level> levels = {{200, 10}, {5, 5}, {1, 0}};
  std::vector<crawl::level> levels = {{50, 5}, {1, 0}};
  //std::vector<crawl::level> levels = {{50, 0}};

  index.save("index.scrape");

  crawl::crawl(levels, index, blacklist);

  curl_global_cleanup();

  return 0;
}

