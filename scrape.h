
namespace scrape {

struct index_url {
  std::string url;
  std::string path;
  std::set<std::string> links;

  index_url(std::string u, std::string p) :
    url(u), path(p) {}
};

struct curl_buffer {
  char *buf;
  size_t max;
  size_t size{0};

  curl_buffer(char *b, size_t m) :
      buf(b), max(m) {}

  void reset() { size = 0; };
};


size_t curl_cb_buffer_write(void *contents, size_t sz, size_t nmemb, void *ctx);

size_t curl_cb_header_write(char *buffer, size_t size, size_t nitems, void *userdata);

bool has_suffix(std::string const &s, std::string const &suffix);

bool has_prefix(std::string const &s, std::string const &prefix);

bool want_proto(std::string proto);

bool bad_suffix(std::string path);

bool bad_prefix(std::string path);

void save_file(std::string path, std::string url, curl_buffer *buf);

struct site {
  std::string host;
  std::list<std::string> url_bad;
  std::list<index_url> url_scanned;
  std::list<index_url> url_scanning;

  size_t max_pages;
  size_t fail_net{0};
  size_t fail_web{0};

  site() : host(""), max_pages(0) {}
  site(std::string h, size_t m) : host(h), max_pages(m) {}

  void finish(index_url u, std::list<std::string> links);

  void finish_bad_http(index_url u, int code) {
    url_bad.push_back(u.url);

    switch (code) {
      case 404:
      case 301:
        break;
      default:
        printf("miss %d %s\n", code, u.url.c_str());
        fail_web++;
        break;
    }
  }

  void finish_bad_net(index_url u, bool actually_bad) {
    url_bad.push_back(u.url);
    if (actually_bad) {
      fail_net++;
    }
  }

  bool finished();
  index_url pop_next();
};

}

