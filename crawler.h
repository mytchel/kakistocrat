namespace crawl {

void insert_site_index_seed(
    index &index,
    std::vector<std::string> url,
    std::vector<std::string> &blacklist);

struct level {
  size_t max_pages;
  size_t max_add_sites;
};

void crawl(std::vector<level> levels,
    size_t max_threads,
    index &index,
    std::vector<std::string> &blacklist);

}

