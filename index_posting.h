const size_t ITCAP = (1 << 16);

struct index {
  std::pair<std::string, posting> *store[ITCAP];

  index()
  {
    for (size_t i = 0; i < ITCAP; i++) {
      store[i] = {};
    }
  }

  ~index()
  {
    for (size_t i = 0; i < ITCAP; i++) {
      if (store[i]) delete store[i];
    }
  }

  size_t load(uint8_t *buffer);

  posting *find(std::string key);
};

size_t index_save(hash_table &t, uint8_t *buffer);



