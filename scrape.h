
namespace scrape {

struct index_url {
  std::uint32_t id;
  std::string url;
  std::string path;
  std::set<std::uint32_t> linked_by;
};

struct other_url {
  std::uint32_t id;
  std::string url;
  std::set<std::uint32_t> linked_by;
};

void
scrape(int max_pages, 
    const std::string host, 
    const std::vector<std::string> url_scanning,
    std::list<index_url> &url_index,
    std::list<other_url> &url_other);

}

