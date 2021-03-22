#ifndef BST_H
#define BST_H

#include "key.h"
#include "posting.h"
#include "buf_list.h"

struct bst {
  key k;
	posting store;

  bst *left{NULL}, *right{NULL};

  ~bst() {
    if (left) delete left;
    if (right) delete right;
  }

  bst(key kk) : k(kk) {}

  size_t insert(std::string &key, uint32_t val, buf_list &b);

  void get_postings(std::list<std::pair<key, posting>> &postings);
};

#endif

