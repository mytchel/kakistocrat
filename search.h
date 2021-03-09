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
  search::index dict;

  searcher() {}

  void load(std::string s, std::string d);

  std::vector<search_entry> search(char *str);

  //struct dynamic_array_kv_32 docNos;
  //hash_table dictionary_pair;
  //hash_table dictionary_trine;
};

}

#endif

