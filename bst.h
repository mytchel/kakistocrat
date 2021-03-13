#ifndef BST_H
#define BST_H

#include "posting.h"

struct bst {
  std::string key;
	posting store;
  bst *left, *right;

  ~bst() {
    if (left) delete left;
    if (right) delete right;
  }

  bst(std::string &key, uint64_t val);

  bool insert(std::string &key, uint64_t val);

  void get_postings(std::vector<std::pair<std::string, posting>> &postings);
};

#endif

