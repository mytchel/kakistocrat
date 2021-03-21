#ifndef BST_H
#define BST_H

#include "posting.h"

struct bst {
  std::string key;
	posting store;
  bst *left{NULL}, *right{NULL};

  ~bst() {
    if (left) delete left;
    if (right) delete right;
  }

  bst(std::string &k) : key(k) {}

  size_t insert(std::string &key, uint32_t val);

  void get_postings(std::list<std::pair<std::string, posting>> &postings);
};

#endif

