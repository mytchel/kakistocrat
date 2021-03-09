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

#include "util.h"
#include "crawl.h"
#include "scorer.h"

int main(int argc, char *argv[]) {
  crawl::crawler crawler;
  crawler.load();

  printf("score initial from index\n");
  scorer::scores scores(crawler);

  for (int i = 0; i < 10; i++) {
    printf("score iteration %i\n", i);
    scores.iteration();
  }

  scores.save("scores.json");

  return 0;
}

