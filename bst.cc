#include <string>
#include <cstring>
#include <vector>
#include <map>

#include "bst.h"

bst::bst(std::string &k, uint64_t val) {
	left = right = {};
	key = k;

	store.append(val);
}

void bst::insert(std::string &k, uint64_t val)
{
  bst *b = this;
	for (;;) {
		int cmp = b->key.compare(k);

		if (cmp < 0) {
      if (b->left) {
        b = b->left;
      } else {
				b->left = new bst(k, val);
				return;
      }

    } else if (cmp > 0) {
			if (b->right) {
			  b = b->right;
      } else {
				b->right = new bst(k, val);
				return;
      }

    } else {
      b->store.append(val);
			return;
		}
	}
}

void bst::get_postings(std::map<std::string, posting> &postings)
{
  postings.emplace(key, store);
  if (left) left->get_postings(postings);
  if (right) right->get_postings(postings);
}
