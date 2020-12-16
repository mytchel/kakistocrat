
namespace scrape {

struct index_url {
  int count;
  std::string url;
  std::string path;
};

struct other_url {
  int count;
  std::string url;
};

void
scrape(int max_pages, 
    const std::string host, 
    const std::vector<std::string> url_scanning,
    std::list<struct index_url> &url_index,
    std::list<struct other_url> &url_other);

}

