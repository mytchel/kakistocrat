#include <string>
#include <cstring>
#include <vector>
#include <map>

#include "bst.h"

bst::bst(std::string &k, uint32_t val) : store(val) {
	left = right = {};
	key = k;
}

bool bst::insert(std::string &k, uint32_t val)
{
  bst *b = this;
	for (;;) {
		int cmp = b->key.compare(k);

		if (cmp < 0) {
      if (b->left) {
        b = b->left;
      } else {
				b->left = new bst(k, val);
				return true;
      }

    } else if (cmp > 0) {
			if (b->right) {
			  b = b->right;
      } else {
				b->right = new bst(k, val);
				return true;
      }

    } else {
      b->store.append(val);
			return false;
		}
	}
}

void bst::get_postings(std::list<std::pair<std::string, posting>> &postings)
{
  postings.emplace_back(key, store);
  if (left) left->get_postings(postings);
  if (right) right->get_postings(postings);
}
