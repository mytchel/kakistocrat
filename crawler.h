#ifndef CRAWLER_H
#define CRAWLER_H

#include <nlohmann/json.hpp>

#include <thread>
#include <vector>
#include <list>

#include "site.h"
#include "config.h"

namespace crawl {

struct site : public site_map {
  size_t level;

  time_t last_scanned{0};
  size_t max_pages{0};

  bool scraped{false};
  bool scraping{false};

  site() {}

  site(const std::string &p, const std::string &h, size_t l)
    : site_map(p, h), level(l)
  {}

  page* find_add_page(const std::string &url, size_t level, const std::string &path = "");
};

void to_json(nlohmann::json &j, const site &s);
void from_json(const nlohmann::json &j, site &s);

struct crawler {
  std::string site_data_path;
  std::string site_meta_path;
  std::string sites_path;

  std::vector<crawl_level> levels;

  std::list<site> sites;
  std::unordered_map<std::string, site*> sites_host_map;
  std::unordered_map<std::string, site*> sites_path_map;

  std::vector<std::string> blacklist;

  crawler(const config &c)
    : site_data_path(c.crawler.site_data_path),
      site_meta_path(c.crawler.site_meta_path),
      sites_path(c.crawler.sites_path),
      levels(c.crawler.levels)
  {}

  site* find_site(const std::string &host);
  site* find_site_by_path(const std::string &path);
  site* add_site(const std::string &host, size_t level);

  void load_seed(std::vector<std::string> seed);

  void load_blacklist(std::vector<std::string> &b) {
    blacklist = b;
  }

  bool check_blacklist(const std::string &host);

  bool have_next_site();
  site* get_next_site();

  void expand(
    site *isite,
    size_t max_add_sites,
    size_t next_max_page);

  std::string get_meta_path(const std::string &host);
  std::string get_data_path(const std::string &host);

  //scrape::site make_scrape_site(site *s,
 //   size_t site_max_con, size_t max_site_part_size, size_t max_page_size);

  void save();
  void load();
};


}
#endif

