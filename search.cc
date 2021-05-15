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

#include "spdlog/spdlog.h"

#include "search.h"

namespace search {

void searcher::load()
{
  //scores.load();
  index.load();
}

// This only returns documents that match all the terms.
// Which may not be the best?
// It is certainly not what I want.
static std::list<std::pair<std::string, double>> intersect_postings(
    std::vector<std::vector<std::pair<std::string, double>>> &postings)
{
  std::list<std::pair<std::string, double>> result;

  spdlog::info("interset {}", postings.size());

  try {
  if (postings.size() == 0) {
    return result;
  }

  std::vector<size_t> indexes(postings.size(), 0);

	while (indexes[0] < postings[0].size()) {
		auto url = postings[0][indexes[0]].first;
		bool canAdd = true;
		for (size_t i = 1; i < postings.size(); i++) {
			while (indexes[i] < postings[i].size() && postings[i][indexes[i]].first < url)
				indexes[i]++;

			if (indexes[i] == postings[i].size()) {
				return result;
      }

			if (postings[i][indexes[i]].first != url)
				canAdd = false;
		}

    if (canAdd) {
	    double rsv = 0;
			for (size_t i = 0; i < postings.size(); i++) {
				double w = postings[i][indexes[i]].second;
        rsv += w;
      }

      result.emplace_back(url, rsv);
		}

		indexes[0]++;
	}
  } catch (const std::exception& e) {
    spdlog::warn("failed to intersect");
    }

	return result;
}

std::list<search_entry> searcher::search(char *line)
{
  std::list<search_entry> results;

  spdlog::info("search for {}", line);

  auto postings = index.find_matches(line);
  spdlog::debug("got postings {}", postings.size());

  for (auto &v: postings) {
    for (auto &p: v) {
      spdlog::trace("have {}", p.first);
    }
  }

  auto results_raw = intersect_postings(postings);
  spdlog::debug("got results {}", results_raw.size());

  for (auto &result: results_raw) {
    /*
    auto page = scores.find_page(page_id);
    if (page == NULL) {
      spdlog::debug("failed to find page id: {}", page_id);
      continue;
    }
*/
    double score = result.second;// + result.second * page->score;
    if (signbit(score)) {
      score = 0;
    }

    //results.emplace_back(score, page->url, page->title, page->path);
    results.emplace_back(score, result.first, "something", "somewhere");
  }

  results.sort(
      [](const search_entry &a, const search_entry &b) {
        return a.score > b.score;
      });

  return results;
}

}

