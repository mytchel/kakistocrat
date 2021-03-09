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

extern "C" {

#include "str.h"

}

#include "util.h"
#include "scorer.h"
#include "tokenizer.h"
#include "search.h"

namespace search {


/*
 * Atire BM25
 * Trotman, A., X. Jia, M. Crane, Towards an Efficient and Effective Search Engine,
 * SIGIR 2012 Workshop on Open Source Information Retrieval, p. 40-47
 */
  /*
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
*/

/*
 * Atire BM25
 * Trotman, A., X. Jia, M. Crane, Towards an Efficient and Effective Search Engine,
 * SIGIR 2012 Workshop on Open Source Information Retrieval, p. 40-47
 */
static void rank(std::vector<std::pair<uint64_t, uint64_t>> &postings)
  //  struct dynamic_array_kv_32 *docNos, double avgdl)
{
  //  TODO: get doc lengths
  return;
/*
	// IDF = ln(N/df_t)
	double wt = log(docNos->length / posting.size());
	for (auto &p: postings) {
		uint64_t &page_id = p.first;
		uint64_t &tf = p.second;
		size_t tf = dynamic_array_kv_64_at(posting, i)[1];                   // term frequency / tf_td
		double docLength = 100;//(size_t)dynamic_array_kv_32_at(docNos, docId)[1]; // L_d
		//                   (k_1 + 1) * tf_td
		// IDF * ----------------------------------------- (over)
		//       k_1 * (1 - b + b * (L_d / L_avg)) + tf_td
		double k1 = 0.9;
		double b = 0.4;
		double dividend = (k1 + 1.0) * tf;
		double divisor = k1 * (1 - b + b * (docLength / avgdl) + tf);
		double rsv = wt * dividend / divisor;                                // retrieval status value

    // int of double, somehow
    tf = *(uint64_t *)&rsv;
	}
  */
}

static std::vector<std::pair<uint64_t, double>> intersect_postings(
    std::vector<std::vector<std::pair<uint64_t, double>>> &postings)
{
  std::vector<std::pair<uint64_t, double>> result;

  if (postings.size() == 0) {
    return result;
  }

  std::vector<size_t> indexes(postings.size(), 0);

	while (indexes[0] < postings[0].size()) {
		size_t id = postings[0][indexes[0]].first;
		int canAdd = 1;
		for (size_t i = 1; i < postings.size(); i++) {
			while (indexes[i] < postings[i].size() && postings[i][indexes[i]].first < id)
				indexes[i]++;

			if (indexes[i] == postings[i].size())
				return result;

			if (postings[i][indexes[i]].first != id)
				canAdd = 0;
		}

    if (canAdd) {
	    double rsv = 0;
			for (size_t i = 0; i < postings.size(); i++) {
				double w = postings[i][indexes[i]].second;
        rsv += w;
      }

      result.emplace_back(id, rsv);
		}

		indexes[0]++;
	}

	return result;
}

void searcher::load(std::string scores_path, std::string dat_path)
{
  scores.load(scores_path);

  char *index = (char *) malloc(1024*1024*1024);

  std::ifstream file;

  printf("load %s\n", dat_path.c_str());

  file.open(dat_path.c_str(), std::ios::in | std::ios::binary);

  if (!file.is_open() || file.fail() || !file.good() || file.bad()) {
    fprintf(stderr, "error opening file %s\n",  dat_path.c_str());
    return;
  }

  file.read(index, 1024*1024*1024);

  printf("load dict\n");
  dict.load((uint8_t *) index);
  printf("loaded dict\n");
}

std::vector<std::string> split_terms(char *line)
{
  const size_t buf_len = 512;

  char tok_buffer_store[buf_len]; // Provide underlying storage for tok_buffer
	struct str tok_buffer;
	str_init(&tok_buffer, tok_buffer_store, sizeof(tok_buffer_store));

  tokenizer::token_type token;
  tokenizer::tokenizer tok;

  tok.init(line, strlen(line));

  std::vector<std::string> terms;

	do {
		token = tok.next(&tok_buffer);
    if (token == tokenizer::WORD) {
		  str_tolower(&tok_buffer);
		  str_tostem(&tok_buffer);

      terms.push_back(std::string(str_c(&tok_buffer)));
    }
	} while (token != tokenizer::END);


  return terms;
}

std::vector<search_entry> searcher::search(char *line)
{
  std::vector<search_entry> results;

  printf("search for %s\n", line);

  auto terms = split_terms(line);

  if (terms.empty()) {
    return results;
  }

  std::vector<std::vector<std::pair<uint64_t, double>>> postings;

	// Find results for strings
	for (auto &term: terms) {
    printf("get posting for term '%s'\n", term.c_str());

		posting *post = dict.find(term);
    if (post != NULL) {
      printf("found posting\n");
      std::vector<std::pair<uint64_t, double>> pairs;

      pairs.reserve(post->counts.size());
      for (auto &p: post->counts) {
        printf("%s   %lu  %u\n", term.c_str(), p.first, p.second);
        // TODO: Rank
        pairs.emplace_back(p.first, p.second / 100);
      }

      postings.push_back(pairs);
		}
	}

  printf("intersect\n");
  auto results_raw = intersect_postings(postings);

  printf("to results\n");
  for (auto &result: results_raw) {
    printf("to result for %lu %f\n", result.first, result.second);
    uint64_t page_id = result.first;

    auto page = scores.find_page(page_id);
    if (page == NULL) {
      printf("failed to find page id: %llu\n", page_id);
      continue;
    }

    double score = result.second + result.second * page->score;
    if (signbit(score)) {
      score = 0;
    }

    results.emplace_back(score, page_id, page->url, page->title, page->path);
  }

  std::sort(results.begin(), results.end(),
      [](const search_entry &a, const search_entry &b) {
        return a.score > b.score;
      });

  return results;
}

}

