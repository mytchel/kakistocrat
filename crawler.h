namespace crawl {

struct level {
  size_t max_pages;
  size_t max_add_sites;
};

void crawl(std::vector<level> levels, index &index);

}

