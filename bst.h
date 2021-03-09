#ifndef BST_H
#define BST_H

#include "posting.h"

struct bst {
  std::string key;
  bst *left, *right;
	posting store;

  ~bst() {
    if (left) delete left;
    if (right) delete right;
  }

  bst(std::string &key, uint64_t val);

  void insert(std::string &key, uint64_t val);

  void get_postings(std::map<std::string, posting> &postings);
};

#endif

