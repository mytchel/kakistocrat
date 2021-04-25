#ifndef SCORER_H
#define SCORER_H

#include "crawl.h"
#include "crawler.h"

namespace scorer {

struct page {
  uint64_t id;
  size_t level;
  double score;
  std::string url;
  std::string path;
  std::string title;

  // For calculations. Not for saving
  std::vector<std::pair<uint64_t, size_t>> links;

  page() {}

  page(uint64_t i, size_t l, double s,
      std::string u, std::string p, std::string t)
      : id(i), level(l), score(s), url(u), path(p), title(t) {}
};

void to_json(nlohmann::json &j, const page &p);
void from_json(const nlohmann::json &j, page &p);

struct scores {
  std::map<uint64_t, page> pages;

  std::string path;

  page* find_page(uint64_t id);
  page* find_page(const std::string &url);

  scores(std::string p) : path(p) {}
  scores(std::string p, std::list<crawl::site> &sites);
  void iteration();

  void save();
  void load();
};

}

#endif

