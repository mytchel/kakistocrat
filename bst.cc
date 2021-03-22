#include <string>
#include <cstring>
#include <vector>
#include <map>

#include "bst.h"
#include "buf_list.h"

size_t bst::insert(std::string &k, uint32_t val, buf_list &bl)
{
  bst *b = this;
	for (;;) {
		int cmp = b->k.compare(k);

		if (cmp < 0) {
      if (b->left) {
        b = b->left;
      } else {
        key kk(bl.get(key_size(k)), k);
				b->left = new bst(kk);
        size_t l = b->left->store.append(val);
				return sizeof(bst) + kk.size() + l;
      }

    } else if (cmp > 0) {
			if (b->right) {
			  b = b->right;
      } else {
        key kk(bl.get(key_size(k)), k);
				b->right = new bst(kk);
        size_t l = b->right->store.append(val);
				return sizeof(bst) + kk.size() + l;
      }

    } else {
      return b->store.append(val);
		}
	}
}

void bst::get_postings(std::list<std::pair<key, posting>> &postings)
{
  postings.emplace_back(k, store);
  if (left) left->get_postings(postings);
  if (right) right->get_postings(postings);
}
