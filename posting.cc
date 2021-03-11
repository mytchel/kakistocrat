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

void posting::decompress()
{
	size_t id_length = ((uint32_t *) backing)[0];
	size_t count_length = ((uint32_t *) backing)[1];

  uint8_t *id_store = backing + 2 * sizeof(uint32_t);
	uint8_t *count_store = id_store + id_length;

	uint64_t prevI = 0;
	uint64_t docI = 0;
	size_t di = 0;
	size_t ci = 0;

  counts.clear();
  counts.reserve(count_length);

	while (ci < count_length && di < id_length) {
		di += vbyte_read(&id_store[di], &docI);
		docI += prevI;
		prevI = docI;

    counts.emplace_back(docI, count_store[ci]);
		ci++;
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
    id_length += vbyte_store(id_store + id_length, c.first - o);
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

size_t posting::backing_size()
{
  size_t id_length = ((uint32_t *) backing)[0];
	size_t count_length = ((uint32_t *) backing)[1];

  return sizeof(uint32_t) * 2 + id_length + count_length;
}

void posting::merge(posting &other)
{
  for (auto &p: other.counts) {
    counts.emplace_back(p.first, p.second);
  }
}


