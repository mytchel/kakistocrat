#include <stdio.h>
#include <unistd.h>
#include <signal.h>

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
  std::vector<std::string> blacklist = util::load_list("../mine/blacklist");
  std::vector<std::string> initial_seed = util::load_list("../mine/seed");

  crawl::index index;

  index.load("index.scrape");

  crawl::insert_site_index_seed(index, initial_seed, blacklist);

  signal(SIGPIPE, SIG_IGN);
  curl_global_init(CURL_GLOBAL_ALL);

  std::vector<crawl::level> levels = {{200, 50}, {5, 5}, {1, 0}};

  index.save("index.scrape");

  crawl::crawl(levels, 1000, index, blacklist);

  curl_global_cleanup();

  return 0;
}

