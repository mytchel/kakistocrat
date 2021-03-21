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

size_t hash_table::insert(std::string key, uint32_t val)
{
  if (key.size() > 255) return 0;

	uint32_t index = hash(key);

  if (store[index]) {
		return store[index]->insert(key, val);
  } else {
		store[index] = new bst(key);
    return sizeof(bst) + key.size() + store[index]->store.append(val);
  }
}

std::list<std::pair<std::string, posting>> hash_table::get_postings()
{
  std::list<std::pair<std::string, posting>> postings;

  //postings.reserve(n_postings);

  for (size_t i = 0; i < HTCAP; i++) {
    if (store[i]) {
      store[i]->get_postings(postings);
    }
  }

  return postings;
}

