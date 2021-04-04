#ifndef SCRAPE_H
#define SCRAPE_H

#include <set>

#include "channel.h"

namespace scrape {

struct page {
  std::string url;
  std::string path;
  std::string title{""};

  time_t last_scanned{0};

  std::list<std::pair<std::string, size_t>> links;

  page(std::string u, std::string p) :
    url(u), path(p) {}

  page(std::string u, std::string p, time_t t) :
    url(u), path(p), last_scanned(t) {}
};

struct sitemap_url {
  std::string url;
};

struct site {
  std::string host;

  std::list<page> url_pending;
  std::list<page> url_scanning;

  std::list<page> url_scanned;
  std::list<page> url_unchanged;
  std::list<page> url_bad;

  std::set<std::string> disallow_path;
  bool getting_robots{false};
  bool got_robots{false};

  std::set<std::string> sitemap_url_pending;
  std::set<std::string> sitemap_url_getting;
  std::set<std::string> sitemap_url_got;
  size_t sitemap_count{0};

  std::string output_dir;
  size_t max_pages;
  size_t max_part_size;
  size_t max_page_size;

  // This probably doesn't work very well.
  size_t max_active{5};

  size_t fail{0};

  site(std::string h, std::list<page> s,
      std::string n_output,
      size_t n_max_pages,
      size_t n_max_part_size,
      size_t n_max_page_size)
    : host(h), url_pending(s),
      output_dir(n_output),
      max_pages(n_max_pages),
      max_part_size(n_max_part_size),
      max_page_size(n_max_page_size)
  {}

  void init_paths();

  void process_sitemap_entry(std::string url, std::optional<time_t> lastmod);
  void add_disallow(std::string &path);

  void finish(page *u, std::list<std::string> &links, std::string &title);
  void finish_unchanged(page *u);
  void finish_bad(page *u, bool actually_bad);

  bool should_finish();
  bool finished();

  std::optional<page*> get_next();

  bool disallow_url(std::string u);
};

bool want_proto(std::string proto);

bool bad_suffix(std::string path);

bool bad_prefix(std::string path);

void
scraper(int i,
    Channel<site*> &in,
    Channel<site*> &out,
    Channel<bool> &stat,
    size_t max_sites,
    size_t max_con);

}

#endif
