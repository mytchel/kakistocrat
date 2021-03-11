#include <stdlib.h>
#include <string.h>

#include <cstdio>

#include <vector>
#include <algorithm>

#include "vbyte.h"

#include "posting.h"

void posting::append(uint64_t id)
{
  auto &b = counts.back();
	if (counts.size() > 0 && b.first == id) {
    if (b.second < 255)
      b.second++;

  } else {
    counts.emplace_back(id, 1);
	}
}

size_t posting::save(uint8_t *buffer)
{
  std::sort(counts.begin(), counts.end(),
      [](auto &a, auto &b) {
        return a.first < b.first;
      }
  );

	size_t offset = 2 * sizeof(uint32_t);

  uint8_t *id_store = buffer + offset;
  size_t id_length = 0;

  uint64_t o = 0;
  for (auto &c: counts) {
    //offset += vbyte_store(buffer + offset, c.first - o);
    memcpy(id_store + id_length, &c.first, sizeof(uint64_t));
    id_length += sizeof(uint64_t);
    o = c.first;
  }

  offset += id_length;

  for (auto &c: counts) {
    buffer[offset] = c.second;
    offset += sizeof(uint8_t);
  }

	((uint32_t *)buffer)[0] = id_length;
	((uint32_t *)buffer)[1] = counts.size();

	return offset;
}

size_t posting::load(uint8_t *buffer)
{
  size_t id_length = ((uint32_t *) buffer)[0];
	size_t count_length = ((uint32_t *) buffer)[1];

	uint64_t *id_store = (uint64_t *) (buffer + 2 * sizeof(uint32_t));
	uint8_t *count_store = (uint8_t *) id_store + id_length;

  counts.clear();
  counts.reserve(count_length);

  size_t i;
  for (i = 0; i < count_length; i++) {
    uint64_t id;
    memcpy(&id, id_store + i, sizeof(uint64_t));
    uint8_t c = count_store[i];

    counts.emplace_back(id, c);
  }

  return sizeof(uint32_t) * 2 + id_length + count_length;
  /*
	size_t id_length = ((uint32_t *) buffer)[0];
	size_t count_length = ((uint32_t *) buffer)[1];
	uint8_t *id_store = (uint8_t *) buffer + 2 * sizeof(uint32_t);
	uint8_t *count_store = id_store + id_length;

	size_t prevI = 0;
	uint32_t docI = 0;
	size_t di = 0;
	size_t ci = 0;

  counts.clear();

	while (ci < count_length && di < id_length) {
	//	di += vbyte_read(&id_store[di], &docI);
		docI += prevI;
		prevI = docI;
		size_t count = count_store[ci];

    counts.emplace_back(docI, count);
		ci++;
	}

  return sizeof(uint32_t) * 2 + id_length + count_length;
  */
}

void posting::merge(posting &other)
{
  for (auto &p: other.counts) {
    counts.emplace_back(p.first, p.second);
  }
}

std::vector<std::pair<uint64_t, uint64_t>> posting::decompress()
{
  std::vector<std::pair<uint64_t, uint64_t>> out;

  for (auto &c: counts) {
    out.emplace_back(c.first, c.second);
  }

	return out;
}

