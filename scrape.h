
namespace scrape {

struct index_url {
  std::string url;
  std::string path;
  std::set<std::string> links;
};

void
scrape(int max_pages, 
    const std::string host, 
    std::list<index_url> &url_index);

}

