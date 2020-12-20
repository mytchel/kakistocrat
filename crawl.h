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
};

struct page {
  std::uint32_t id;
  std::string url;
  std::string path;
  double score{0};
  std::vector<page_id> links;
};

struct site {
  std::uint32_t id;
  std::string host;
  size_t level;
  bool scraped{false};

  std::uint32_t next_id{1};
  std::list<page> pages;
};

struct index {
  std::list<site> sites;
  std::uint32_t next_id{1};
};

void save_index(index &index, std::string path);
void load_index(index &index, std::string path);

site * index_find_host(
        index &index,
        std::string host);

page * index_find_page(site *site, std::string url);

}
