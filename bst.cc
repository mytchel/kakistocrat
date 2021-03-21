#include <string>
#include <cstring>
#include <vector>
#include <map>

#include "bst.h"

size_t bst::insert(std::string &k, uint32_t val)
{
  bst *b = this;
	for (;;) {
		int cmp = b->key.compare(k);

		if (cmp < 0) {
      if (b->left) {
        b = b->left;
      } else {
				b->left = new bst(k);
        size_t l = b->left->store.append(val);
				return sizeof(bst) + k.size() + l;;
      }

    } else if (cmp > 0) {
			if (b->right) {
			  b = b->right;
      } else {
				b->right = new bst(k);
        size_t l = b->right->store.append(val);
				return sizeof(bst) + k.size() + l;;
      }

    } else {
      return b->store.append(val);
		}
	}
}

void bst::get_postings(std::list<std::pair<std::string, posting>> &postings)
{
  postings.emplace_back(key, store);
  if (left) left->get_postings(postings);
  if (right) right->get_postings(postings);
}
