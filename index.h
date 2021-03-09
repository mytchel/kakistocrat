#ifndef INDEX_H
#define INDEX_H

namespace search {

#include <stdint.h>

#include "posting.h"
#include "hash_table.h"

const size_t ITCAP = (1 << 16);

enum index_type{words, pairs, trines};

// TODO: need the bst.

struct index {
  std::pair<std::string, posting> *store[ITCAP];
  index_type type;


  index(index_type t) : type(t)
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

}
#endif

