#include <stdio.h>
#include <unistd.h>
#include <math.h>

#include <cstdlib>
#include <cstring>

#include <vector>
#include <list>
#include <set>
#include <map>
#include <string>
#include <algorithm>
#include <future>
#include <optional>
#include <iostream>
#include <fstream>
#include <sstream>
#include <cstdint>

#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>

#include "util.h"
#include "config.h"
#include "crawl.h"
#include "scorer.h"
#include "search.h"

int main(int argc, char *argv[]) {
  spdlog::set_level(spdlog::level::debug);

  config c = read_config();

  search::searcher searcher(c);

  searcher.load();

	// Accept input
	char line[1024];
	while (true) {
    printf("enter search: ");

    if (fgets(line, sizeof(line), stdin) == NULL) break;

		auto results = searcher.search(line);

    printf("found %zu results\n", results.size());

    size_t i = 0;
		for (auto &result: results) {
		  printf("%f %llu %s\n", result.score, result.page_id, result.path.c_str());
			printf("    %s\n", result.url.c_str());
			printf("    %s\n", result.title.c_str());

      if (++i > 10) {
        break;
      }
		}

    printf("\n");
	}

  return 0;
}

