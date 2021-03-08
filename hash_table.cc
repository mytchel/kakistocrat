#include <string>
#include <cstring>

#include "posting.h"
#include "bst.h"
#include "hash_table.h"

static uint32_t hash(const char *key)
{
	uint32_t result = 0;

	while (*key != '\0')
		result = (*key++ + 31 * result);

	return result & (HTCAP - 1);
}

void hash_table::insert(std::string key, uint32_t val)
{
	uint32_t index = hash(key.c_str());

  if (store[index]) {
		store[index]->insert(key, val);
  } else {
		store[index] = new bst(key, val);
  }
}

struct posting *hash_table::find(char *key)
{
  uint32_t index = hash(key);
  if (store[index] == NULL) {
    return NULL;
  }

  uint32_t length = *(uint32_t *)store[index];

	uint32_t *vec = (uint32_t *)((char *)store[index] + sizeof(uint32_t));

	for (size_t i = 0; i < length; i++) {
    char *s = (char *) store[index] + vec[i*2];
	  if (strcmp(key, (char *) store[index] + vec[i*2]) == 0) {
	    return (struct posting *) ((char *) store[index] + vec[i*2+1]);
    }
  }

  return NULL;
}

size_t hash_table::save(char *buffer)
{
  /*
  uint32_t count = 0;
  for (size_t i = 0; i < HTCAP; i++)
    if (store[i] != NULL)
	    count++;

  ((uint32_t *)buffer)[0] = count;
  char *at = buffer + sizeof(uint32_t) + sizeof(uint32_t) * count; // Ptrs to stores

  size_t offset = 1; // First index for length
  for (size_t i = 0; i < HTCAP; i++)
    if (store[i]) {
      char *start = at;

		  ((uint32_t *)at)[0] = lengths[i];
		  at += sizeof(uint32_t);
			char *ptr_store = at; // All key, and val ptrs
			at += lengths[i] * 2 * sizeof(uint32_t); // Keys and postings
			at = store[i]->write(start, ptr_store, at);

      ((uint32_t *)buffer)[offset++] = start - buffer;
    }

  return at - buffer;
  */
  return 0;
}

size_t hash_table::load(char *buffer)
{
  uint32_t count = ((uint32_t *)buffer)[0];

  uint32_t *stores = &((uint32_t *)buffer)[1];

  for (size_t i = 0; i < count; i++) {
    char *cell = buffer + stores[i];
	  char *key = cell + ((uint32_t *)cell)[1];
		unsigned int index = hash(key);
		store[index] = (bst *) cell;
  }

  return 0;
}

