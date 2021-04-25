#ifndef CRAWLER_H
#define CRAWLER_H

#include <nlohmann/json.hpp>

#include <thread>
#include <vector>
#include <list>

#include "crawl.h"

namespace crawl {

struct crawler {
  std::string site_data_path;
  std::string site_meta_path;
  std::string sites_path;

  std::vector<crawl_level> levels;

  std::uint32_t next_id{1};

  std::list<site> sites;

  std::vector<std::string> blacklist;

  crawler(const config &c)
    : site_data_path(c.crawler.site_data_path),
      site_meta_path(c.crawler.site_meta_path),
      sites_path(c.crawler.sites_path),
      levels(c.crawler.levels)
  {}

  site* find_site(const std::string &host);
  site* find_site(uint32_t id);

  page* find_page(uint64_t id);
  page* find_page(page_id id);

  void load_seed(std::vector<std::string> seed);

  void load_blacklist(std::vector<std::string> &b) {
    blacklist = b;
  }

  bool check_blacklist(const std::string &host);

  bool have_next_site();
  site* get_next_site();

  void update_site(site *isite, std::list<scrape::page *> &page_list);

  void enable_references(
    site *isite,
    size_t max_add_sites,
    size_t next_max_page);

  std::string get_data_path(const std::string &host);

  scrape::site make_scrape_site(site *s,
    size_t site_max_con, size_t max_site_part_size, size_t max_page_size);

  void save();
  void load();
};


}
#endif

