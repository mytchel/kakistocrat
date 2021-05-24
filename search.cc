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

