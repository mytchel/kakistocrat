
namespace scrape {

struct index_url {
  std::uint32_t id;
  std::string url;
  std::string path;
  std::set<std::uint32_t> links;
  std::set<std::string> ext_links;
};

void
scrape(int max_pages, 
    const std::string host, 
    std::list<index_url> &url_index);

}

