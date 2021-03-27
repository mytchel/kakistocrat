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

#include <nlohmann/json.hpp>
#include "spdlog/spdlog.h"

#include "channel.h"
#include "util.h"
#include "scrape.h"
#include "crawl.h"
#include "tokenizer.h"

#include "index.h"

using nlohmann::json;

int main(int argc, char *argv[]) {
  crawl::crawler crawler;
  crawler.load();

  search::indexer indexer("meta/single", search::get_split_at());

  char *file_buf = (char *) malloc(scrape::max_file_size);
  if (file_buf == NULL) {
    throw std::bad_alloc();
  }

  auto site = crawler.sites.begin();
  while (site != crawler.sites.end()) {
    if (site->last_scanned == 0) {
      site++;
      continue;
    }

    site->load();

    indexer.index_site(*site, file_buf, scrape::max_file_size);

    site->flush();
    site++;
  }

  free(file_buf);

  indexer.save();

  return 0;
}

