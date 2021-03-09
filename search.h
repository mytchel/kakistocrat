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
  search::index dict_words;
  search::index dict_pairs;
  search::index dict_trines;

  searcher() :
    dict_words(search::words),
    dict_pairs(search::pairs),
    dict_trines(search::trines)  {}

  void load(std::string s, std::string a, std::string b, std::string c);

  std::vector<search_entry> search(char *str);
};

}

#endif

