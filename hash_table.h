const size_t HTCAP = (1 << 16);

struct hash_table {
  bst *store[HTCAP];

  hash_table()
  {
    for (size_t i = 0; i < HTCAP; i++) {
      store[i] = {};
    }
  }

  ~hash_table()
  {
    for (size_t i = 0; i < HTCAP; i++) {
      if (store[i]) delete store[i];
    }
  }

  void insert(std::string key, uint32_t val);

  posting *find(char *key);

  size_t save(char *buffer);
  size_t load(char *buffer);
};

