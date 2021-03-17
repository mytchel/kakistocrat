#ifndef SCRAPE_H
#define SCRAPE_H

#include "channel.h"

namespace scrape {

const size_t max_file_size = 1024 * 1024 * 10;

struct index_url {
  std::string url;
  std::string path;
  std::string title{""};

  time_t last_scanned{0};
  bool ok{false};

  std::set<std::string> links;

  index_url() : url(""), path("") {}

  index_url(std::string u, std::string p) :
    url(u), path(p) {}

  index_url(std::string u, std::string p, time_t t, bool v) :
    url(u), path(p), last_scanned(t), ok(v) {}
};

struct sitemap_url {
  std::string url;
};

struct site {
  std::string host;

  std::list<index_url> url_pending;
  std::list<index_url> url_scanning;

  std::list<index_url> url_scanned;
  std::list<index_url> url_unchanged;
  std::list<index_url> url_bad;

  std::list<std::string> disallow_path;
  bool getting_robots{false};
  bool got_robots{false};

  std::set<std::string> sitemap_url_pending;
  std::set<std::string> sitemap_url_getting;
  std::set<std::string> sitemap_url_got;
  size_t sitemap_count{0};

  size_t max_pages;
  size_t max_active{5};
  size_t fail{0};

  site() : host(""), max_pages(0) {}
  site(std::string h, size_t m) : host(h), max_pages(m) {}
  site(std::string h, size_t m, std::list<index_url> s)
    : host(h), max_pages(m), url_pending(s) {}

  void init_paths();

  void process_sitemap_entry(std::string url, std::optional<time_t> lastmod);
  void add_disallow(std::string path);

  void finish(index_url u, std::list<std::string> links, std::string title);

  void finish_unchanged(index_url u);
  void finish_bad(index_url u, bool actually_bad);

  bool should_finish();
  bool finished();

  std::optional<index_url> get_next();

  bool disallow_url(std::string u);
};

bool want_proto(std::string proto);

bool bad_suffix(std::string path);

bool bad_prefix(std::string path);

void
scraper(Channel<site*> &in, Channel<site*> &out, Channel<bool> &stat,
    int i, size_t max_con);

}

#endif
