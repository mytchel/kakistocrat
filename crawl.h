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

  page_id(uint32_t s, uint32_t p) :
    site(s),
    page(p) { }

  page_id(uint64_t v) :
    site(v >> 32),
    page(v & 0xffffffff) { }
};

struct page {
  std::uint32_t id;
  std::string url;
  std::string path;
  std::string title{"unknown"};

  time_t last_scanned{0};
  bool valid{false};
  bool scraped{false};

  std::vector<page_id> links;

  page(uint32_t i, std::string u, std::string p) :
    id(i), url(u), path(p) {}

  page(uint32_t i, std::string u, std::string p, std::string tt, time_t t, bool v, bool s) :
    id(i), url(u), path(p), title(tt), last_scanned(t), valid(v), scraped(s) {}

  page(uint32_t i, std::string u, std::string p, std::string tt, time_t t, bool v, bool s,
        std::vector<page_id> l) :
    id(i), url(u), path(p), title(tt), last_scanned(t), valid(v), scraped(s), links(l) {}
};

struct site {
  std::uint32_t id;
  std::string host;
  size_t level;

  time_t last_scanned{0};

  bool scraped{false};
  bool scraping{false};

  std::uint32_t next_id{1};
  std::list<page> pages;

  page* find_page(uint32_t id);
  page* find_page(std::string url);
  page* find_page_by_path(std::string path);

  site(uint32_t i, std::string h, size_t l) :
    id(i), host(h), level(l) {}

  site(uint32_t i, std::string h, size_t l, time_t s) :
    id(i), host(h), level(l), last_scanned(s) {}
};

struct index {
  std::list<site> sites;
  std::uint32_t next_id{1};

  site* find_host(std::string host);

  page* find_page(uint64_t id);
  page* find_page(page_id id);

  void save(std::string path);
  void load(std::string path);
};

}
