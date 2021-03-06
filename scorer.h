namespace scorer {

struct page {
  uint64_t id;
  size_t level;
  double score;
  std::string url;
  std::string path;
  std::string title;
  std::vector<uint64_t> links;

  page() {}

  page(uint64_t i, size_t l, double s,
      std::string u, std::string p, std::string t,
      std::vector<uint64_t> li) :
      id(i), level(l), score(s), url(u), path(p), title(t), links(li) {}
};

void to_json(nlohmann::json &j, const page &p);
void from_json(const nlohmann::json &j, page &p);

struct scores {
  std::map<uint64_t, page> pages;

  page* find_page(uint64_t id);

  void init(crawl::index &index);
  void iteration();

  void save(std::string path);
  void load(std::string path);
};

}
