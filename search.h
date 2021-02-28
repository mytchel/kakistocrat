namespace search {

struct search_entry {
  search_entry(double s, uint64_t id, std::string u, std::string t, std::string p)
      : score(s), page_id(id), url(u), title(t), path(p) {}

  double score;
  uint64_t page_id;
  std::string url;
  std::string title;
  std::string path;
};

class searcher {
  public:

  void load(std::string path);

  std::vector<search_entry> search(char *str, scorer::scores &index_scores);

  private:

  char *index;
  struct dynamic_array_kv_32 docNos;

  struct hash_table dictionary;
  struct hash_table dictionary_pair;
  struct hash_table dictionary_trine;
};

}

