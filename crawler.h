namespace crawl {

void insert_site_index_seed(
    index &index,
    std::vector<std::string> url,
    std::vector<std::string> &blacklist);

void run_round(size_t level, size_t max_level,
    size_t max_sites, size_t max_pages,
    size_t max_threads,
    index &index,
    std::vector<std::string> &blacklist);

}

