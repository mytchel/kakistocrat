#include <stdlib.h>
#include <string.h>

#include <cstdio>

#include <vector>

#include "vbyte.h"

#include "posting.h"

posting::posting()
{
	id = 0;
	id_capacity = 256;
	id_length = 0;
	id_store = (uint8_t *) malloc(id_capacity);
}

posting::~posting()
{
  free(id_store);
}

void posting::append(uint32_t i)
{
	if (i == id) {
    auto &count = counts.back();
    if (count < 255)
      count++;

  } else {
    // Max bytes vbyte can use for 32bit int
		if (id_capacity - id_length < 5) {
      printf("hit limit %i\n", id_capacity);
			id_capacity *= 2;
			id_store = (uint8_t *) realloc(id_store, id_capacity);
		}

    id_length += vbyte_store(&id_store[id_length], i - id);

    counts.push_back(1);

		id = i;
	}
}

size_t posting::write(char *buffer)
{
	size_t offset = 2 * sizeof(uint32_t);

	memcpy(&buffer[offset], id_store, id_length);
	offset += id_length;

  for (auto &c: counts) {
    buffer[offset] = c;
    offset += sizeof(uint8_t);
  }

	((uint32_t *)buffer)[0] = id_length;
	((uint32_t *)buffer)[1] = counts.size();

	return offset;
}

std::vector<std::pair<uint64_t, uint64_t>> posting::decompress()
{
  std::vector<std::pair<uint64_t, uint64_t>> out;
/*
	size_t id_length = ((uint32_t *)p)[0];
	size_t count_length = ((uint32_t *)p)[1];
	uint8_t *id_store = (uint8_t *)p + 2 * sizeof(uint32_t);
	uint8_t *count_store = id_store + id_length;

	size_t prevI = 0;
	uint32_t docI = 0;
	size_t di = 0;
	size_t ci = 0;

	while (ci < count_length && di < id_length) {
		di += vbyte_read(&id_store[di], &docI);
		docI += prevI;
		prevI = docI;
		size_t count = count_store[ci];
    out.emplace_back(docI, count);
		ci++;
	}
*/
	return out;
}

