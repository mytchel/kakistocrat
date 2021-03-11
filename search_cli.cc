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

#include "util.h"
#include "crawl.h"
#include "scorer.h"
#include "search.h"

int main(int argc, char *argv[]) {
  search::searcher searcher("scores.json", "meaningness.com");

  searcher.load();

	// Accept input
	char line[1024];
	while (true) {
    printf("enter search: ");

    if (fgets(line, sizeof(line), stdin) == NULL) break;

		auto results = searcher.search(line);

		for (auto &result: results) {
		  printf("%f %llu %s\n", result.score, result.page_id, result.path.c_str());
			printf("    %s\n", result.url.c_str());
			printf("    %s\n", result.title.c_str());
		}

    printf("\n");
	}

  return 0;
}

