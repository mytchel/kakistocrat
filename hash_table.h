#ifndef HASH_TABLE_H
#define HASH_TABLE_H

#include "bst.h"

const size_t HTCAP = (1 << 17);

struct hash_table {
  bst *store[HTCAP];
  size_t n_postings{0};

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

  void insert(std::string key, uint64_t val);

  std::vector<std::pair<std::string, posting>> get_postings();
};

uint32_t hash(std::string key);
uint32_t hash(const char *key);
uint32_t hash(const char *key, size_t l);

#endif
