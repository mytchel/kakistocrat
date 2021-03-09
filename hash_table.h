#ifndef HASH_TABLE_H
#define HASH_TABLE_H

#include "bst.h"

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

  void insert(std::string key, uint64_t val);

  std::map<std::string, posting> get_postings();
};

uint32_t hash(std::string key);

#endif
