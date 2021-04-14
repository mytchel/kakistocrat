#ifndef SCRAPE_H
#define SCRAPE_H

#include <set>

#include <curl/curl.h>

#include "channel.h"

namespace scrape {

struct site;

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

struct site_op {
  site *m_site;
  uint8_t *buf;
  size_t buf_max;
  size_t size{0};

  site_op(site *s, uint8_t *b, size_t max)
    : m_site(s), buf(b), buf_max(max)
  {}

  virtual ~site_op();

  virtual void setup_handle(CURL *) = 0;

  virtual void finish(std::string effective_url) = 0;
  virtual void finish_bad(CURLcode res, int http_code) = 0;
};

struct site_op_page : public site_op {
  page *m_page;
  bool unchanged{false};

  site_op_page(site *s, uint8_t *b, size_t max, page *p)
    : site_op(s, b, max),
      m_page(p)
  {}

  ~site_op_page() {};

  void save();

  void process_page(std::string page_url,
      std::list<std::string> &links,
      std::string &title);

  void setup_handle(CURL *);

  void finish(std::string effective_url);
  void finish_bad(CURLcode res, int http_code);
};

struct site_op_robots : public site_op {
  site_op_robots(site *s, uint8_t *b, size_t max)
    : site_op(s, b, max)
  {}

  ~site_op_robots() {};

  void setup_handle(CURL *);

  void finish(std::string effective_url);
  void finish_bad(CURLcode res, int http_code);
};

struct site_op_sitemap : public site_op {
  std::string m_url;

  site_op_sitemap(site *s, uint8_t *b, size_t max, std::string u)
    : site_op(s, b, max),
      m_url(u)
  {}

  ~site_op_sitemap() {};

  void setup_handle(CURL *);

  void finish(std::string effective_url);
  void finish_bad(CURLcode res, int http_code);
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

  size_t fail{0};

  std::vector<uint8_t *> free_bufs;
  std::vector<uint8_t *> using_bufs;
  size_t buf_max;

  site(std::string h, std::list<page> s,
      std::string n_output,
      size_t n_max_connections,
      size_t n_max_pages,
      size_t n_max_part_size,
      size_t n_max_page_size)
    : host(h), url_pending(s),
      output_dir(n_output),
      max_pages(n_max_pages),
      max_part_size(n_max_part_size),
      max_page_size(n_max_page_size)
  {
    buf_max = n_max_page_size;

    for (size_t i = 0; i < n_max_connections; i++) {
      uint8_t *buf = (uint8_t *) malloc(buf_max);
      if (buf == nullptr) {
        throw std::bad_alloc();
      }

      free_bufs.push_back(buf);
    }
  }

  ~site() {
    for (auto b: free_bufs) {
      free(b);
    }
  }

  uint8_t *pop_buf() {
    if (free_bufs.empty()) {
      return nullptr;
    }

    uint8_t *b = free_bufs.back();
    free_bufs.pop_back();

    using_bufs.push_back(b);

    spdlog::debug("give out  {}", (size_t) b);

    return b;
  }

  void push_buf(uint8_t *b) {
    spdlog::debug("take back {}", (size_t) b);

    if (b == nullptr) {
      throw std::invalid_argument("push_buf with null");
    }

    free_bufs.push_back(b);

    auto it = using_bufs.begin();
    while (it != using_bufs.end()) {
      if (*it == b) {
        using_bufs.erase(it);
        break;
      }

      it++;
    }
  }

  void init_paths();

  void process_sitemap_entry(std::string url, std::optional<time_t> lastmod);
  void add_disallow(std::string &path);

  void finish(page *u, std::list<std::string> &links, std::string &title);
  void finish_unchanged(page *u);
  void finish_bad(page *u, bool actually_bad);

  bool should_finish();
  bool finished();

  std::optional<site_op*> get_next();

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
