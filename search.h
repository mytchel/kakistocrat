#ifndef SEARCH_H
#define SEARCH_H

#include "config.h"
#include "index.h"
//#include "scorer.h"

namespace search {

struct search_entry {
  search_entry(double s, std::string u, std::string t, std::string p)
      : score(s), url(u), title(t), path(p) {}

  double score;
  std::string url;
  std::string title;
  std::string path;
};

struct searcher {
  //scorer::scores scores;
  search::index index;

  searcher(config &c)
    : index(c.merger.meta_path, c.merger.htcap) {}//,
      //scores(c.scores_path)
  //{}

  void load();

  std::list<search_entry> search(char *str);
};

}

#endif

