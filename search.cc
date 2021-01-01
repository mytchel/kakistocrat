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

#include "util.h"
#include "scrape.h"
#include "crawl.h"

extern "C" {

#include "x_cocomel/dynamic_array_kv_64.h"
#include "x_cocomel/dynamic_array_kv_32.h"
#include "x_cocomel/posting.h"
#include "x_cocomel/hash_table.h"
#include "x_cocomel/tokenizer.h"
#include "x_cocomel/search.h"

}

crawl::page* find_page(crawl::index &index, uint64_t id) {
  uint32_t site_id = (uint32_t) (id >> 32);
  uint32_t page_id = (uint32_t) (id & 0xffffffff);

  for (auto &site: index.sites) {
    if (site.id != site_id) continue;
    for (auto &page: site.pages) {
      if (page.id == page_id) {
        return &page;
      }
    }

    return NULL;
  }

  return NULL;
}

static void results_sort(struct dynamic_array_kv_64 *results) {
	for (size_t i = 1; i < results->length; i++) {
		uint64_t tmp_a = dynamic_array_kv_64_at(results, i)[0];
		// non-negative floats sort like ints. avoid the cast here
		uint64_t tmp_b = dynamic_array_kv_64_at(results, i)[1];

		size_t j = i;
		while (j > 0 && tmp_b > dynamic_array_kv_64_at(results, j-1)[1]) {
			dynamic_array_kv_64_at(results, j)[0] = dynamic_array_kv_64_at(results, j-1)[0];
			dynamic_array_kv_64_at(results, j)[1] = dynamic_array_kv_64_at(results, j-1)[1];
			j--;
		}
		dynamic_array_kv_64_at(results, j)[0] = tmp_a;
		dynamic_array_kv_64_at(results, j)[1] = tmp_b;
	}
}

int main(int argc, char *argv[]) {
  crawl::index index;
  crawl::load_index(index, "full_index");

  char *search_index = (char *) malloc(1024*1024*1024);

  std::ifstream file;

  file.open("index.dat", std::ios::in | std::ios::binary);

  if (!file.is_open() || file.fail() || !file.good() || file.bad()) {
    fprintf(stderr, "error opening file %s\n",  "index.dat");
    return 1;
  }

  file.read(search_index, 1024*1024*1024);

	// Decode index
	struct dynamic_array_kv_32 docNos;
	dynamic_array_kv_32_init(&docNos);
	docNos.length = ((uint32_t *)search_index)[1];
	docNos.store = (uint32_t *)&search_index[2 * sizeof(uint32_t)];

  printf("enter search: ");

	// Accept input
	char line[1024];
	if (fgets(line, sizeof(line), stdin) != NULL) {
		struct dynamic_array_kv_64 *result_list = search(search_index, line);

		if (result_list == NULL) {
			printf("No results\n");
			exit(0);
		}

		dynamic_array_kv_64 result_rescored;
    dynamic_array_kv_64_init(&result_rescored);

		for (size_t i = 0; i < result_list->length; i++) {
			size_t docId = dynamic_array_kv_64_at(result_list, i)[0] - 1;
			double rsv = *(double *)&dynamic_array_kv_64_at(result_list, i)[1];

      size_t offset = dynamic_array_kv_32_at(&docNos, docId)[0];
      uint64_t page_id = *((uint64_t *) (search_index + offset));

      auto page = find_page(index, page_id);
      if (page == NULL) {
        printf("failed to find page id: %llu\n", page_id);
        continue;
      }

      double score = rsv * page->score;

      dynamic_array_kv_64_append(&result_rescored, page_id, *(uint64_t *) &score);
    }

    results_sort(&result_rescored);

		for (size_t i = 0; i < result_rescored.length; i++) {
	    uint64_t page_id = dynamic_array_kv_64_at(&result_rescored, i)[0];
			double score = *(double *)&dynamic_array_kv_64_at(&result_rescored, i)[1];

      auto page = find_page(index, page_id);
      if (page == NULL) {
        printf("failed to find page id: %llu\n", page_id);
        return 1;
      }

		  printf("%i %f %llu %s\n", i, score, page_id, page->path.c_str());
			printf("    %s\n", page->url.c_str());
		}

	}

  return 0;
}

