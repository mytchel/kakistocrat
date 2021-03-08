#include <string>
#include <cstring>

#include "posting.h"
#include "bst.h"

bst::bst(std::string &k, uint32_t val) {
	left = right = {};
	key = k;

	store.append(val);
}

void bst::insert(std::string &k, uint32_t val)
{
  bst *b = this;
	for (;;) {
		int cmp = key.compare(k);

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

char *bst::save(char *start, char *ptr_buffer, char *val_buffer)
{
  bst *b = this;
	while (b) {
		if (b->left == NULL) {
			((uint32_t *)ptr_buffer)[0] = val_buffer - start;
			ptr_buffer += sizeof(uint32_t);
      strcpy(val_buffer, b->key.c_str());
			val_buffer += b->key.size() + 1;

			((uint32_t *)ptr_buffer)[0] = val_buffer - start;
			ptr_buffer += sizeof(uint32_t);
			val_buffer += b->store.save(val_buffer);

			b = b->right;

		} else {
			bst *temp = b->left;
			b->left = temp->right;
			temp->right = b;
			b = temp;
		}
	}

	return val_buffer;
}

char *bst::load(char *start, char *ptr_buffer, char *val_buffer)
{
  return NULL;
}
