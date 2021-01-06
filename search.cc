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
#include <thread>
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

static inline void string_tolower(char *str) {
	while ((*str = tolower(*str)))
		++str;
}

static struct dynamic_array_kv_64 *intersect_postings(struct dynamic_array_64 *postings) {
	struct dynamic_array_kv_64 *result = (struct dynamic_array_kv_64 *) malloc(sizeof(struct dynamic_array_kv_64));
	dynamic_array_kv_64_init(result);

	size_t *indexes = (size_t *)malloc(sizeof(size_t) * postings->length);
	for (size_t i = 0; i < postings->length; i++)
		indexes[i] = 0;

	while (indexes[0] < ((struct dynamic_array_kv_64 *)postings->store[0])->length) {
		size_t id = dynamic_array_kv_64_at((struct dynamic_array_kv_64 *)postings->store[0], indexes[0])[0];
		int canAdd = 1;
		for (size_t i = 1; i < postings->length; i++) {
			while (indexes[i] < ((struct dynamic_array_kv_64 *)postings->store[i])->length && 
                        dynamic_array_kv_64_at((struct dynamic_array_kv_64 *)postings->store[i], indexes[i])[0] < id)
				indexes[i]++;

			if (indexes[i] == ((struct dynamic_array_kv_64 *)postings->store[i])->length)
				return result;
			if (dynamic_array_kv_64_at((struct dynamic_array_kv_64 *)postings->store[i], indexes[i])[0] != id)
				canAdd = 0;
		}

        if (canAdd) {
			double rsv = 0;
			for (size_t i = 0; i < postings->length; i++) {
				double w = *(double *)&dynamic_array_kv_64_at((struct dynamic_array_kv_64 *)postings->store[i], indexes[i])[1];
                rsv += w;
            }

			dynamic_array_kv_64_append(result, id, *(uint64_t *)&rsv);
		}
		indexes[0]++;
	}

	return result;
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


/*
 * Okapi BM25 from Trec-3? Has some issues with numbers going negative
 *
static void rank(struct dynamic_array_kv_64 *posting, struct dynamic_array_kv_32 *docNos, double avgdl) {
	double wt = log2((docNos->length - posting->length + 0.5) / (posting->length + 0.5));
	for (size_t i = 0; i < posting->length; i++) {
		size_t docId = dynamic_array_kv_64_at(posting, i)[0] - 1;
		size_t tf = dynamic_array_kv_64_at(posting, i)[1];
		size_t docLength = (size_t)dynamic_array_kv_32_at(docNos, docId)[1];
		double K = 1.2 * (0.25 + 0.75 * (double) docLength / avgdl);
		double w = wt * 2.2 * tf / (K + tf);
		dynamic_array_kv_64_at(posting, i)[1] = *(uint64_t *)&w;
	}
}
*/


/*
 * Atire BM25
 * Trotman, A., X. Jia, M. Crane, Towards an Efficient and Effective Search Engine,
 * SIGIR 2012 Workshop on Open Source Information Retrieval, p. 40-47
 */
static void rank(struct dynamic_array_kv_64 *posting, struct dynamic_array_kv_32 *docNos, double avgdl) {
	// IDF = ln(N/df_t)
	double wt = log(docNos->length / posting->length);
	for (size_t i = 0; i < posting->length; i++) {
		size_t docId = dynamic_array_kv_64_at(posting, i)[0] - 1;
		size_t tf = dynamic_array_kv_64_at(posting, i)[1];                   // term frequency / tf_td
		double docLength = (size_t)dynamic_array_kv_32_at(docNos, docId)[1]; // L_d
		//                   (k_1 + 1) * tf_td
		// IDF * ----------------------------------------- (over)
		//       k_1 * (1 - b + b * (L_d / L_avg)) + tf_td
		double k1 = 0.9;
		double b = 0.4;
		double dividend = (k1 + 1.0) * tf;
		double divisor = k1 * (1 - b + b * (docLength / avgdl) + tf);
		double rsv = wt * dividend / divisor;                                // retrieval status value
		dynamic_array_kv_64_at(posting, i)[1] = *(uint64_t *)&rsv;
	}
}

struct dynamic_array_kv_64 *search(char *index, char *line) {
	double avgdl = 0;

	// Decode index
	struct dynamic_array_kv_32 docNos;
	dynamic_array_kv_32_init(&docNos);
	docNos.length = ((uint32_t *)index)[1];
	docNos.store = (uint32_t *)&index[2 * sizeof(uint32_t)];

	size_t dict_offset = ((uint32_t *)index)[0];
	struct hash_table dictionary;
	hash_table_init(&dictionary);
	hash_table_read(&dictionary, &index[dict_offset]);

	// Find average document length
	for (size_t i = 0; i < docNos.length; i++)
		avgdl += dynamic_array_kv_32_at(&docNos, i)[1];
	avgdl /= docNos.length;

	char tok_buffer_store[260]; // Provide underlying storage for tok_buffer
	struct str tok_buffer;
	str_init(&tok_buffer, tok_buffer_store, sizeof(tok_buffer_store));

	tokenizer::token_type token;
  tokenizer::tokenizer tok;

	// Perform the search
	struct dynamic_array_64 terms;
	dynamic_array_64_init(&terms);
	tok.init(line, strlen(line));
	do {
		token = tok.next(&tok_buffer);
    if (token == tokenizer::WORD) {
		  string_tolower(str_c(&tok_buffer));
		  dynamic_array_64_append(&terms, (uint64_t)str_dup_c(&tok_buffer));
		}
	} while (token != tokenizer::END);

	if (terms.length == 0) {
    return NULL;
  }

	struct dynamic_array_64 postings;
	dynamic_array_64_init(&postings);

	// Find results for strings
	for (size_t i = 0; i < terms.length; i++) {
		struct posting *post_compressed = hash_table_find(&dictionary, (char *)terms.store[i]);
		if (post_compressed == NULL) {
      printf("hash find failed for '%s'\n", terms.store[i]);
			return NULL;
    } else {
	    struct dynamic_array_kv_64 *post = posting_decompress(post_compressed);
			rank(post, &docNos, avgdl);
			dynamic_array_64_append(&postings, (uint64_t)post);
		}
	}

	struct dynamic_array_kv_64 *result_list = intersect_postings(&postings);

	results_sort(result_list);

	return result_list;
}

int main(int argc, char *argv[]) {
  scorer::scores index_scores;

  index_scores.load("index.scores");

  char *index = (char *) malloc(1024*1024*1024);

  std::ifstream file;

  printf("load index.dat\n");

  file.open("index.dat", std::ios::in | std::ios::binary);

  if (!file.is_open() || file.fail() || !file.good() || file.bad()) {
    fprintf(stderr, "error opening file %s\n",  "index.dat");
    return 1;
  }

  file.read(index, 1024*1024*1024);

	// Decode index
	struct dynamic_array_kv_32 docNos;
	dynamic_array_kv_32_init(&docNos);
	docNos.length = ((uint32_t *)index)[1];
	docNos.store = (uint32_t *)&index[2 * sizeof(uint32_t)];

  printf("docnos len %i\n", docNos.length);

	// Accept input
	char line[1024];
	for (;;) {
    printf("enter search: ");
    if (fgets(line, sizeof(line), stdin) == NULL) break;

		struct dynamic_array_kv_64 *result_list = search(index, line);

		if (result_list == NULL) {
			printf("No results\n");
			continue;
		}

		dynamic_array_kv_64 result_rescored;
    dynamic_array_kv_64_init(&result_rescored);

		for (size_t i = 0; i < result_list->length; i++) {
			size_t docId = dynamic_array_kv_64_at(result_list, i)[0] - 1;
			double rsv = *(double *)&dynamic_array_kv_64_at(result_list, i)[1];

      size_t offset = dynamic_array_kv_32_at(&docNos, docId)[0];
      uint64_t page_id = *((uint64_t *) (index + offset));

      auto page = index_scores.find_page(page_id);
      if (page == NULL) {
        printf("failed to find page id: %llu\n", page_id);
        continue;
      }

      double score = rsv  + rsv * page->score;

      if (signbit(score)) {
        score = 0;
      }

      dynamic_array_kv_64_append(&result_rescored, page_id, *(uint64_t *) &score);
    }

    results_sort(&result_rescored);

		for (size_t i = 0; i < result_rescored.length && i < 20; i++) {
	    uint64_t page_id = dynamic_array_kv_64_at(&result_rescored, i)[0];
			double score = *(double *)&dynamic_array_kv_64_at(&result_rescored, i)[1];

      auto page = index_scores.find_page(page_id);
      if (page == NULL) {
        printf("failed to find page id: %llu\n", page_id);
        return 1;
      }

		  printf("%i %f %llu %s\n", i, score, page_id, page->path.c_str());
			printf("    %s\n", page->url.c_str());
		}

    printf("\n");
	}

  return 0;
}

