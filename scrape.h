

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
    std::string host, 
    std::vector<std::string> url_scanning,
    std::vector<struct index_url> &url_index,
    std::vector<struct other_url> &url_other);

