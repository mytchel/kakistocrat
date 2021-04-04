#ifndef CRAWL_H
#define CRAWL_H

#include <nlohmann/json.hpp>

#include <thread>
#include <vector>
#include <list>

#include "scrape.h"
#include "config.h"

namespace crawl {

struct page_id {
  uint32_t site;
  uint32_t page;

  uint64_t to_value() {
    return (((uint64_t ) site) << 32) | ((uint64_t ) page);
  }

  page_id() {}

  page_id(uint32_t s, uint32_t p)
    : site(s), page(p) { }

  page_id(uint64_t v) :
    site((uint32_t) (v >> 32)),
    page((uint32_t) (v & 0xffffffff)) { }

  bool operator<(const page_id &a) const {
    if (site == a.site) {
      return page < a.page;
    } else {
      return site < a.site;
    }
  }

  bool operator==(const page_id &o) const {
    return site == o.site && page == o.page;
  }
};

void to_json(nlohmann::json &j, const page_id &s);
void from_json(const nlohmann::json &j, page_id &s);

struct page {
  uint32_t id;
  std::string url;
  std::string path;
  std::string title{"unknown"};

  time_t last_scanned{0};

  std::list<std::pair<page_id, size_t>> links;

  page() {}

  page(uint32_t i, std::string u, std::string p)
    : id(i), url(u), path(p) {}

  page(uint32_t i, std::string u, std::string p,
      std::string tt, time_t t)
    : id(i), url(u), path(p),
      title(tt), last_scanned(t) {}
};

void to_json(nlohmann::json &j, const page &s);
void from_json(const nlohmann::json &j, page &s);

std::string site_path(std::string sites_meta_path, std::string h);

struct site {
  std::uint32_t id;
  std::string host;
  std::string path;

  // Scanned data
  time_t last_scanned{0};
  std::uint32_t next_id{1};
  std::list<page> pages;

  // For crawler to manage
  bool scraped{false};
  bool scraping{false};
  size_t max_pages{0};
  size_t level;

  bool loaded{false};
  bool changed{false};

  void flush() {
    if (changed) {
      save();
      changed = false;
    }

    if (loaded) {
      loaded = false;
      pages.clear();
    }
  }

  void load();
  void save();

  site(std::string p, std::string h) : path(p), host(h) {}

  site(std::string p, uint32_t i, std::string h, size_t l, size_t m, time_t ls)
    : path(p), id(i), host(h),
      level(l), max_pages(m),
      last_scanned(ls)
  {
      scraped = last_scanned > 0;
  }

  // For creating new sites
  site(std::string p, uint32_t i, std::string h, size_t l)
    : path(p), id(i), host(h),
      level(l),
      loaded(true),
      changed(true) {}

  page* find_page(uint32_t id);
  page* find_page(std::string url);
  page* find_page_by_path(std::string path);
};

void to_json(nlohmann::json &j, const site &s);
void from_json(const nlohmann::json &j, site &s);

struct crawler {
  std::string site_data_path;
  std::string site_meta_path;
  std::string sites_path;

  size_t n_threads;
  size_t thread_max_sites;
  size_t thread_max_con;
  size_t max_site_part_size;
  size_t max_page_size;

  std::vector<crawl_level> levels;

  std::uint32_t next_id{1};

  std::list<site> sites;

  std::vector<std::string> blacklist;

  crawler(config &c)
    : site_data_path(c.crawler.site_data_path),
      site_meta_path(c.crawler.site_meta_path),
      sites_path(c.crawler.sites_path),
      levels(c.crawler.levels),
      thread_max_sites(5),
      thread_max_con(c.crawler.thread_max_connections),
      max_site_part_size(c.crawler.max_site_part_size),
      max_page_size(c.crawler.max_page_size)
  {
    if (c.crawler.n_threads) {
      n_threads = *c.crawler.n_threads;
    } else {
      n_threads = std::thread::hardware_concurrency();
    }
  }

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

  void update_site(site *isite,
      std::list<scrape::page> &page_list);

  void enable_references(
    site *isite,
    size_t max_add_sites,
    size_t next_max_page);

  void crawl();

  void save();
  void load();
};

}
#endif

