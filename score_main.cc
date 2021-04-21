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

#include "util.h"
#include "config.h"
#include "crawl.h"
#include "scorer.h"

int main(int argc, char *argv[]) {
  config c = read_config();

  crawl::crawler crawler(c);
  crawler.load();

  scorer::scores scores(c.scores_path, crawler.sites);

  for (int i = 0; i < 10; i++) {
    spdlog::debug("score iteration {}", i);
    scores.iteration();
  }

  scores.save();

  return 0;
}

