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
  bool scraped;
  std::uint32_t id;
  std::string url;
  std::string path;
  std::vector<page_id> links;
};

struct site {
  std::uint32_t id;
  std::string host;
  size_t level;
  bool scraped{false};

  std::uint32_t next_id{1};
  std::list<page> pages;

  page* find_page(uint32_t id);
  page* find_page(std::string url);
  page* find_page_by_path(std::string path);
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
