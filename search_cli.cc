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

extern "C" {

#include "str.h"
#include "x_cocomel/dynamic_array_kv_64.h"
#include "x_cocomel/dynamic_array_kv_32.h"
#include "x_cocomel/dynamic_array_64.h"
#include "x_cocomel/vbyte.h"
#include "x_cocomel/posting.h"
#include "x_cocomel/hash_table.h"

}

#include "util.h"
#include "crawl.h"
#include "scorer.h"
#include "tokenizer.h"
#include "search.h"

int main(int argc, char *argv[]) {
  scorer::scores index_scores;

  index_scores.load("index.scores");

  search::searcher searcher;

  searcher.load("index.dat");

	// Accept input
	char line[1024];
	while (true) {
    printf("enter search: ");

    if (fgets(line, sizeof(line), stdin) == NULL) break;

		auto results = searcher.search(line, index_scores);

		for (auto &result: results) {
		  printf("%f %llu %s\n", result.score, result.page_id, result.path.c_str());
			printf("    %s\n", result.url.c_str());
			printf("    %s\n", result.title.c_str());
		}

    printf("\n");
	}

  return 0;
}

