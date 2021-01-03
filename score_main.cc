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
#include "scorer.h"

int main(int argc, char *argv[]) {
  crawl::index index;
  scorer::scores scores;

  index.load("index.scrape");

  printf("score initial from index\n");

  scores.init(index);

  for (int i = 0; i < 10; i++) {
    printf("score iteration %i\n", i);
    scores.iteration();
  }

  scores.save("index.scores");

  return 0;
}

