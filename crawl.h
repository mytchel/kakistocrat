#ifndef CRAWL_H
#define CRAWL_H

#include "scrape.h"

namespace crawl {

struct page_id {
  std::uint32_t site;
  std::uint32_t page;

  bool operator<(const page_id &a) const {
    if (site == a.site) {
      return page < a.page;
    } else {
      return site < a.site;
    }
  }

  uint64_t to_value() {
    return (((uint64_t ) site) << 32) | ((uint64_t ) page);
  }

  page_id() {}

  page_id(uint32_t s, uint32_t p) :
    site(s),
    page(p) { }

  page_id(uint64_t v) :
    site(v >> 32),
    page(v & 0xffffffff) { }
};

void to_json(nlohmann::json &j, const page_id &s);
void from_json(const nlohmann::json &j, page_id &s);

struct page {
  std::uint32_t id;
  size_t level;
  std::string url;
  std::string path;
  std::string title{"unknown"};

  time_t last_scanned{0};
  bool valid{false};

  std::vector<page_id> links;

  page() {}

  page(uint32_t i, size_t l, std::string u, std::string p)
    : id(i), level(l), url(u), path(p) {}

  page(uint32_t i, size_t l, std::string u, std::string p,
      std::string tt, time_t t, bool v)
    : id(i), level(l), url(u), path(p),
      title(tt), last_scanned(t), valid(v) {}

  page(uint32_t i, size_t l, std::string u, std::string p,
      std::string tt, time_t t, bool v,
      std::vector<page_id> li)
    : id(i), level(l), url(u), path(p),
      title(tt), last_scanned(t), valid(v),
      links(li) {}
};

void to_json(nlohmann::json &j, const page &s);
void from_json(const nlohmann::json &j, page &s);

struct site {
  bool loaded{false};
  bool scraped{false};
  bool scraping{false};
  bool enabled{false};

  std::uint32_t id;
  std::string host;

  time_t last_scanned{0};
  std::uint32_t next_id{1};
  std::vector<page> pages;
  size_t level;

  void load();
  void unload();
  void save();

  // For loading from json

  site() {}
  site(uint32_t i, size_t l, std::string h, time_t ls, bool e) :
    id(i), level(l), host(h), last_scanned(ls), enabled(e) {
      scraped = last_scanned > 0;
  }

  // For creating new sites
  site(uint32_t i, size_t l, std::string h) :
    id(i), level(l), host(h), loaded(true) {}

  page* find_page(uint32_t id);
  page* find_page(std::string url);
  page* find_page_by_path(std::string path);
};

void to_json(nlohmann::json &j, const site &s);
void from_json(const nlohmann::json &j, site &s);

struct level {
  size_t max_pages;
  size_t max_add_sites;
};

struct crawler {
  std::list<site> sites;
  std::uint32_t next_id{1};
  std::vector<std::string> blacklist;

  crawler() {}

  site* find_site(std::string host);
  site* find_site(uint32_t id);

  page* find_page(uint64_t id);
  page* find_page(page_id id);

  void load_seed(std::vector<std::string> seed);

  void load_blacklist(std::vector<std::string> &b) {
    blacklist = b;
  }

  bool check_blacklist(std::string host);

  bool have_next_site();
  site* get_next_site();

  size_t insert_site(
    site *isite,
    size_t max_add_sites,
    std::list<scrape::index_url> &page_list);

  void crawl(std::vector<level> levels);

  void save();
  void load();
};

}
#endif

