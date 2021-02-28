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

		struct dynamic_array_kv_64 *results = searcher.search(line, index_scores);

		if (results == NULL) {
			printf("Error searching\n");
			continue;
		}

		for (size_t i = 0; i < results->length && i < 10; i++) {
	    uint64_t page_id = dynamic_array_kv_64_at(results, i)[0];
			double score = *(double *)&dynamic_array_kv_64_at(results, i)[1];

      auto page = index_scores.find_page(page_id);
      if (page == NULL) {
        printf("failed to find page id: %llu\n", page_id);
        return 1;
      }

		  printf("%i %f %llu %s\n", i, score, page_id, page->path.c_str());
			printf("    %s\n", page->url.c_str());
			printf("    %s\n", page->title.c_str());
		}

    printf("\n");

    // TODO: free(results);
	}

  return 0;
}

