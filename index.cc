#include <string>
#include <cstring>
#include <map>

#include "posting.h"
#include "bst.h"
#include "hash_table.h"
#include "index.h"

namespace search {

size_t index_save(hash_table &t, uint8_t *buffer)
{
	auto postings = t.get_postings();

	((uint32_t *) buffer)[0] = postings.size();

  size_t offset = sizeof(uint32_t);

	for (auto &p: postings) {
    memcpy(buffer + offset, p.first.c_str(), p.first.size());
    offset += p.first.size();
    buffer[offset++] = 0;

    offset += p.second.save(buffer + offset);
	}

  return offset;
}

size_t index::load(uint8_t *buffer)
{
  uint32_t count = ((uint32_t *)buffer)[0];

  size_t offset = sizeof(uint32_t);

  printf("load, have count %i\n", count);

  for (size_t i = 0; i < count; i++) {
		uint8_t *c_key = buffer + offset;
		std::string key((char *) c_key);

    printf("key %s\n", key.c_str());

    offset += key.size() + 1;

    unsigned int index = hash(key);
    for (size_t j = 0; j < ITCAP; j++) {
      if (store[index + j] == NULL) {
        store[index + j] = new std::pair<std::string, posting>(key, posting());
        offset += store[index + j]->second.load(buffer + offset);
        printf("posting loaded at %i, used %i bytes\n", index + j, offset);
        break;
      }
    }
  }

  return offset;
}

posting *index::find(std::string key)
{
  uint32_t index = hash(key);
  if (store[index] == NULL) {
    return NULL;
  }

	for (size_t i = 0; i < ITCAP; i++) {
    if (store[index + i]) {
      if (store[index + i]->first == key) {
        return &store[index + i]->second;

      }
    } else {
      break;
    }
  }

  return NULL;
}

}
