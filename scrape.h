
namespace scrape {

struct index_url {
  std::string url;
  std::string path;

  time_t last_scanned{0};
  bool ok{false};

  std::set<std::string> links;

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

  std::list<index_url> url_seed;

  std::list<index_url> url_scanning;

  std::list<index_url> url_scanned;
  std::list<index_url> url_unchanged;
  std::list<index_url> url_bad;

  std::list<std::string> disallow_path;

  size_t max_pages;
  size_t active{0};
  size_t fail_net{0};
  size_t fail_web{0};

  site() : host(""), max_pages(0) {}
  site(std::string h, size_t m) : host(h), max_pages(m) {}
  site(std::string h, size_t m, std::list<index_url> s)
    : host(h), max_pages(m), url_seed(s) {}

//  void finish_robot(std::list<std::string> disallow);
//  void finish_sitemap(std::list<std::string> urls);

  void finish(index_url u, std::list<std::string> links);

  void finish_unchanged(index_url u);
  void finish_bad_http(index_url u, int code);
  void finish_bad_net(index_url u, bool actually_bad);

  bool finished();

  bool have_next(size_t max_active) {
    return url_scanning.size() > 0
      && active < max_active
      && url_scanned.size() + active < max_pages;
  }

  index_url pop_next();
};

struct curl_data {
  char *buf{NULL};
  size_t size{0};
  size_t max;

  site *m_site;
  index_url url;

  bool unchanged{false};

  curl_data(site *s, index_url u, size_t m) :
      m_site(s), url(u), max(m) {}

  ~curl_data() {
    if (buf) free(buf);
  }

  void save();
};

size_t curl_cb_buffer_write(void *contents, size_t sz, size_t nmemb, void *ctx);

size_t curl_cb_header_write(char *buffer, size_t size, size_t nitems, void *userdata);

bool has_suffix(std::string const &s, std::string const &suffix);

bool has_prefix(std::string const &s, std::string const &prefix);

bool want_proto(std::string proto);

bool bad_suffix(std::string path);

bool bad_prefix(std::string path);

}

