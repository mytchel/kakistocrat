#include <string>
#include <cstring>
#include <vector>
#include <map>

#include "posting.h"
#include "bst.h"
#include "hash_table.h"

uint32_t hash(std::string key)
{
	uint32_t result = 0;

  for (auto &c: key)
		result = (c + 31 * result);

	return result & (HTCAP - 1);
}

void hash_table::insert(std::string key, uint64_t val)
{
	uint32_t index = hash(key);

  if (store[index]) {
		store[index]->insert(key, val);
  } else {
		store[index] = new bst(key, val);
  }
}

std::map<std::string, posting> hash_table::get_postings()
{
  std::map<std::string, posting> postings;

  printf("get postings\n");

  for (size_t i = 0; i < HTCAP; i++) {
    if (store[i]) {
      store[i]->get_postings(postings);
    }
  }

  printf("have %i postings\n", postings.size());

  return postings;
}

