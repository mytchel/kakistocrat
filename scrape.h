
namespace scrape {

struct index_url {
  std::string url;
  std::string path;
  std::set<std::string> links;

  index_url(std::string u, std::string p) :
    url(u), path(p) {}
};

bool has_suffix(std::string const &s, std::string const &suffix);

bool has_prefix(std::string const &s, std::string const &prefix);

bool want_proto(std::string proto);

bool bad_suffix(std::string path);

bool bad_prefix(std::string path);

struct site {
  std::string host;
  std::list<std::string> url_bad;
  std::list<index_url> url_scanned;
  std::list<index_url> url_scanning;

  size_t max_pages;
  size_t active{0};
  size_t fail_net{0};
  size_t fail_web{0};

  site() : host(""), max_pages(0) {}
  site(std::string h, size_t m) : host(h), max_pages(m) {}

  void finish(index_url u, std::list<std::string> links);
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
  size_t max;
  size_t size{0};

  index_url url;
  site *m_site;

  curl_data(site *s, index_url u, size_t m) :
      m_site(s), url(u), max(m) {}

  ~curl_data()
  {
    if (buf) free(buf);
  }

  void reset() { size = 0; };

  void save();
};

size_t curl_cb_buffer_write(void *contents, size_t sz, size_t nmemb, void *ctx);

size_t curl_cb_header_write(char *buffer, size_t size, size_t nitems, void *userdata);

}

