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

#include "util.h"
#include "scorer.h"
#include "tokenizer.h"
#include "search.h"

namespace search {

/* TODO: The rank uses the doc length to bias. Should it use seperate
 * doc lengths for words, pairs, and trines? */

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

// This only returns documents that match all the terms.
// Which may not be the best?
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
		bool canAdd = true;
		for (size_t i = 1; i < postings.size(); i++) {
			while (indexes[i] < postings[i].size() && postings[i][indexes[i]].first < id)
				indexes[i]++;

			if (indexes[i] == postings[i].size())
				return result;

			if (postings[i][indexes[i]].first != id)
				canAdd = false;
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

void searcher::load()
{
  scores.load(score_path);
  index.load();
}

std::vector<search_entry> searcher::search(char *line)
{
  std::vector<search_entry> results;

  printf("search for %s\n", line);

  auto postings = index.find_matches(line);
  printf("got postings %i\n", postings.size());

  auto results_raw = intersect_postings(postings);
  printf("got results %i\n", results_raw.size());

  for (auto &result: results_raw) {
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

