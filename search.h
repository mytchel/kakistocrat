namespace search {

class searcher {
  public:

  void load(std::string path);

  struct dynamic_array_kv_64 *search(char *str, scorer::scores &index_scores);

  private:

  char *index;
  struct dynamic_array_kv_32 docNos;

  struct hash_table dictionary;
  struct hash_table dictionary_pair;
  struct hash_table dictionary_trine;
};

}

