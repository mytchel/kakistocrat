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

namespace search {

static struct dynamic_array_kv_64 *intersect_postings(struct dynamic_array_64 *postings) {
	struct dynamic_array_kv_64 *result =
    (struct dynamic_array_kv_64 *) malloc(sizeof(struct dynamic_array_kv_64));

	dynamic_array_kv_64_init(result);

  if (postings->length == 0) {
    return result;
  }

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

static struct dynamic_array_kv_64 *search_c(
    struct dynamic_array_kv_32 *docNos,
    struct hash_table *dictionary,
    struct hash_table *dictionary_pair,
    struct hash_table *dictionary_trine,
    char *line)
{
	double avgdl = 0;

	// Find average document length
	for (size_t i = 0; i < docNos->length; i++)
		avgdl += dynamic_array_kv_32_at(docNos, i)[1];
	avgdl /= docNos->length;

  const size_t buf_len = 512;

  char tok_buffer_store[buf_len]; // Provide underlying storage for tok_buffer
	struct str tok_buffer;
	str_init(&tok_buffer, tok_buffer_store, sizeof(tok_buffer_store));

  char pair_buffer[buf_len * 2 + 1];
	struct str tok_buffer_pair;
	str_init(&tok_buffer_pair, pair_buffer, sizeof(pair_buffer));

  char trine_buffer[buf_len * 3 + 2];
	struct str tok_buffer_trine;
	str_init(&tok_buffer_trine, trine_buffer, sizeof(trine_buffer));

	tokenizer::token_type token;
  tokenizer::tokenizer tok;

	// Perform the search
	struct dynamic_array_64 terms;
	dynamic_array_64_init(&terms);

  struct dynamic_array_64 term_pairs;
	dynamic_array_64_init(&term_pairs);

  struct dynamic_array_64 term_trines;
	dynamic_array_64_init(&term_trines);

  tok.init(line, strlen(line));

	do {
		token = tok.next(&tok_buffer);
    if (token == tokenizer::WORD) {
		  str_tolower(&tok_buffer);
		  str_tostem(&tok_buffer);

      dynamic_array_64_append(&terms, (uint64_t)str_dup_c(&tok_buffer));

      if (str_length(&tok_buffer_trine) > 0) {
        str_cat(&tok_buffer_trine, " ");
        str_cat(&tok_buffer_trine, str_c(&tok_buffer));

        dynamic_array_64_append(&term_trines, (uint64_t)str_dup_c(&tok_buffer_trine));

        str_resize(&tok_buffer_trine, 0);
      }

      if (str_length(&tok_buffer_pair) > 0) {
        str_cat(&tok_buffer_pair, " ");
        str_cat(&tok_buffer_pair, str_c(&tok_buffer));

        dynamic_array_64_append(&term_pairs, (uint64_t)str_dup_c(&tok_buffer_pair));

        str_cat(&tok_buffer_trine, str_c(&tok_buffer_pair));
      }

      str_resize(&tok_buffer_pair, 0);
      str_cat(&tok_buffer_pair, str_c(&tok_buffer));
		}
	} while (token != tokenizer::END);

	if (terms.length == 0) {
    return NULL;
  }

	struct dynamic_array_64 postings;
	dynamic_array_64_init(&postings);

	// Find results for strings
	for (size_t i = 0; i < terms.length; i++) {
		struct posting *post_compressed = hash_table_find(dictionary, (char *) terms.store[i]);
		if (post_compressed == NULL) {
      printf("hash find failed for '%s'\n", terms.store[i]);
			continue;

    } else {
	    struct dynamic_array_kv_64 *post = posting_decompress(post_compressed);
			rank(post, docNos, avgdl);
			dynamic_array_64_append(&postings, (uint64_t)post);
		}
	}

  // Find results for strings
	for (size_t i = 0; i < term_pairs.length; i++) {
		struct posting *post_compressed = hash_table_find(dictionary_pair, (char *) term_pairs.store[i]);
		if (post_compressed == NULL) {
      printf("hash find failed for '%s'\n", term_pairs.store[i]);
			continue;

    } else {
	    struct dynamic_array_kv_64 *post = posting_decompress(post_compressed);
			rank(post, docNos, avgdl);
			dynamic_array_64_append(&postings, (uint64_t)post);
		}
	}

  // Find results for strings
	for (size_t i = 0; i < term_trines.length; i++) {
		struct posting *post_compressed = hash_table_find(dictionary_trine, (char *) term_trines.store[i]);
		if (post_compressed == NULL) {
      printf("hash find failed for '%s'\n", term_trines.store[i]);
			continue;

    } else {
	    struct dynamic_array_kv_64 *post = posting_decompress(post_compressed);
			rank(post, docNos, avgdl);
			dynamic_array_64_append(&postings, (uint64_t)post);
		}
	}

	struct dynamic_array_kv_64 *result_list = intersect_postings(&postings);

	results_sort(result_list);

	return result_list;
}

void searcher::load(std::string path)
{
  index = (char *) malloc(1024*1024*1024);

  std::ifstream file;

  printf("load %s\n", path.c_str());

  file.open(path.c_str(), std::ios::in | std::ios::binary);

  if (!file.is_open() || file.fail() || !file.good() || file.bad()) {
    fprintf(stderr, "error opening file %s\n",  path.c_str());
    return;
  }

  file.read(index, 1024*1024*1024);

	// Decode index
	dynamic_array_kv_32_init(&docNos);
	docNos.length = ((uint32_t *)index)[0];
	docNos.store = (uint32_t *)&index[4 * sizeof(uint32_t)];

  printf("docnos len %i\n", docNos.length);

 	size_t dict_offset = ((uint32_t *)index)[1];
	hash_table_init(&dictionary);
	hash_table_read(&dictionary, &index[dict_offset]);

 	size_t dict_pair_offset = ((uint32_t *)index)[2];
	hash_table_init(&dictionary_pair);
	hash_table_read(&dictionary_pair, &index[dict_pair_offset]);

  size_t dict_trine_offset = ((uint32_t *)index)[3];
	hash_table_init(&dictionary_trine);
	hash_table_read(&dictionary_trine, &index[dict_trine_offset]);
}

struct dynamic_array_kv_64 *searcher::search(
      char *line, scorer::scores &index_scores)
{
  dynamic_array_kv_64 *result_rescored =
    (struct dynamic_array_kv_64 *) malloc(sizeof(struct dynamic_array_kv_64));

  dynamic_array_kv_64_init(result_rescored);

  struct dynamic_array_kv_64 *result_list = search_c(&docNos,
        &dictionary, &dictionary_pair, &dictionary_trine, line);

  if (result_list == NULL) {
    printf("No results\n");
    return result_rescored;
  }

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

    dynamic_array_kv_64_append(result_rescored, page_id, *(uint64_t *) &score);
  }

  results_sort(result_rescored);

  // TODO: free(result_list);

  return result_rescored;
}

}

