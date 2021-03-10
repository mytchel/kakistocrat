#ifndef SEARCH_H
#define SEARCH_H

#include "index.h"
#include "scorer.h"

namespace search {

struct search_entry {
  search_entry(double s, uint64_t id, std::string u, std::string t, std::string p)
      : score(s), page_id(id), url(u), title(t), path(p) {}

  double score;
  uint64_t page_id;
  std::string url;
  std::string title;
  std::string path;
};

struct searcher {
  scorer::scores scores;
  search::index index;

  std::string score_path;

  searcher(std::string s, std::string i) :
    index(i),
    score_path(s) {}

  void load();

  std::vector<search_entry> search(char *str);
};

}

#endif

