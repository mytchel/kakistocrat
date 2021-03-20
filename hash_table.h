#ifndef HASH_TABLE_H
#define HASH_TABLE_H

#include "bst.h"

const size_t HTCAP = (1 << 16);

struct hash_table {
  std::vector<bst*> store;
  size_t n_postings{0};

  hash_table() : store(HTCAP, {}) {}

  ~hash_table()
  {
    for (auto b: store) {
      if (b) delete b;
    }
  }

  void clear()
  {
    n_postings = 0;
    for (size_t i = 0; i < HTCAP; i++) {
      if (store[i] != NULL) {
        delete store[i];
        store[i] = NULL;
      }
    }
  }

  void insert(std::string key, uint32_t val);

  std::list<std::pair<std::string, posting>> get_postings();
};

uint32_t hash(std::string key);
uint32_t hash(const char *key);
uint32_t hash(const char *key, size_t l);

#endif
