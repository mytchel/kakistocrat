#include <string>
#include <cstring>
#include <vector>
#include <map>

#include "posting.h"
#include "bst.h"
#include "hash_table.h"

uint32_t hash(const char *key)
{
	uint32_t result = 0;

  while (*key != 0)
		result = (*key++ + 31 * result);

	return result & (HTCAP - 1);
}

uint32_t hash(const char *key, size_t l)
{
	uint32_t result = 0;

  while (l-- > 0)
		result = (*key++ + 31 * result);

	return result & (HTCAP - 1);
}

uint32_t hash(std::string key)
{
	uint32_t result = 0;

  for (auto &c: key)
		result = (c + 31 * result);

	return result & (HTCAP - 1);
}

size_t hash_table::insert(std::string s, uint32_t val)
{
  if (s.size() == 0 || s.size() > key_max_len) return 0;

	uint32_t index = hash(s);

  if (store[index]) {
		return store[index]->insert(s, val, keys);
  } else {
    key k(keys.get(key_size(s)), s);
		store[index] = new bst(k);
    return sizeof(bst) + k.size() + store[index]->store.append(val);
  }
}

std::list<std::pair<key, posting>> hash_table::get_postings()
{
  std::list<std::pair<key, posting>> postings;

  //postings.reserve(n_postings);

  for (size_t i = 0; i < HTCAP; i++) {
    if (store[i]) {
      store[i]->get_postings(postings);
    }
  }

  return postings;
}

