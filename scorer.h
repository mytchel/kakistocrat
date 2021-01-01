namespace scorer {

struct page {
  uint64_t id;
  size_t level;
  double score;
  std::string url;
  std::string path;
  std::vector<uint64_t> links;

  page(uint64_t i, size_t l, double s, std::string u, std::string p, std::vector<uint64_t> li) :
      id(i), level(l), score(s), url(u), path(p), links(li) {}
};

struct scores {
  std::map<uint64_t, page> pages;
  size_t n_pages{0};

  page* find_page(uint64_t id);

  void init(crawl::index &index);
  void iteration();

  void save(std::string path);
  void load(std::string path);
};

}
