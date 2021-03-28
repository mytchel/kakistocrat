#include <stdlib.h>
#include <string.h>

#include <cstdio>

#include <vector>
#include <algorithm>

#include "spdlog/spdlog.h"

#include "vbyte.h"
#include "posting.h"

posting::posting(uint8_t *b)
{
	ids_len = ((uint32_t *) b)[0];
	counts_len = ((uint32_t *) b)[1];

  if (ids_len > min_backing || counts_len > min_backing) {
    backing = b + sizeof(uint32_t) * 2;

    // indicate to use len
    ids_max = 255;
    counts_max = 255;

  } else {
    backing = NULL;

    memcpy(((uint8_t *) &backing) + 0,
        b + sizeof(uint32_t) * 2,
        ids_len);

    memcpy(((uint8_t *) &backing) + from_size(0),
        b + sizeof(uint32_t) * 2 + ids_len,
        counts_len);

    ids_max = 0;
    counts_max = 0;
  }
}

std::vector<std::pair<uint32_t, uint8_t>>
posting::decompress() const
{
	uint32_t prevI = 0;
	uint32_t docI = 0;
	size_t di = 0;
	size_t ci = 0;

  uint8_t *ids, *counts;

  if (ids_max == 255 || counts_max == 255) {
    ids = backing;
    counts = backing + ids_len;

  } else if (ids_max > 0 || counts_max > 0) {
    ids = backing;
    counts = backing + from_size(ids_max);

  } else {
    ids = (uint8_t *) &backing;
    counts = ((uint8_t *) &backing) + from_size(0);
  }

  std::vector<std::pair<uint32_t, uint8_t>> pairs;

  pairs.reserve(counts_len);

	while (ci < counts_len && di < ids_len) {
		di += vbyte_read(&ids[di], &docI);
		docI += prevI;
		prevI = docI;

    pairs.emplace_back(docI, counts[ci++]);
	}

  return pairs;
}

size_t posting::save(uint8_t *buffer) const
{
  ((uint32_t *)buffer)[0] = ids_len;
	((uint32_t *)buffer)[1] = counts_len;

  uint8_t *buf_ids = buffer + sizeof(uint32_t) * 2;
  uint8_t *buf_cnt = buf_ids + ids_len;

  uint8_t *ids, *counts;

  if (ids_max > 0 || counts_max > 0) {
    ids = backing;
    counts = backing + from_size(ids_max);

  } else {
    ids = (uint8_t *) &backing;
    counts = ((uint8_t *) &backing) + from_size(0);
  }

  memcpy(buf_ids, ids, ids_len);
  memcpy(buf_cnt, counts, counts_len);

	return size();
}

void posting::reserve(size_t id, size_t cnt,
    std::function<uint8_t* (size_t)> allocator)
{
  if (ids_max == 255 || counts_max == 255) {
    throw std::invalid_argument("posting reserve attempt for size 255");
  }

  size_t n_ids_max = ids_max;
  size_t n_counts_max = counts_max;

  while (ids_len + id > from_size(n_ids_max)) {
    n_ids_max++;
  }

  while (counts_len + cnt > from_size(n_counts_max)) {
    n_counts_max++;
  }

  if (n_ids_max != ids_max || n_counts_max != counts_max) {
    uint8_t *ids, *counts;

    if (ids_max > 0 || counts_max > 0) {
      ids = backing;
      counts = backing + from_size(ids_max);

    } else {
      ids = (uint8_t *) &backing;
      counts = ((uint8_t *) &backing) + from_size(0);
    }

    uint8_t *n_backing = allocator(from_size(n_ids_max) + from_size(n_counts_max));

    memcpy(n_backing, ids, ids_len);
    memcpy(n_backing + from_size(n_ids_max), counts, counts_len);

    ids_max = n_ids_max;
    counts_max = n_counts_max;
    backing = n_backing;
  }
}

void posting::merge(posting &other, uint32_t id_add,
    std::function<uint8_t* (size_t)> allocator)
{
  auto pairs = other.decompress();

  reserve(5 + other.ids_len, other.counts_len, allocator);

  for (auto &p: pairs) {
    append(p.first + id_add, p.second, allocator);
  }
}

void posting::append(uint32_t id, uint8_t count,
    std::function<uint8_t* (size_t)> allocator)
{
  if (counts_len > 0 && id == last_id) {

    uint8_t *counts;
    if (ids_max > 0 || counts_max > 0) {
      counts = backing + from_size(ids_max);
    } else {
      counts = ((uint8_t *) &backing) + from_size(0);
    }

    if (counts[counts_len - 1] < 255 - count) {
      counts[counts_len - 1] += count;
    } else {
      counts[counts_len - 1] = 255;
    }

    return;
  }

  uint32_t value = id - last_id;

  reserve(vbyte_len(value), 1, allocator);

  uint8_t *ids, *counts;

  if (ids_max > 0 || counts_max > 0) {
    ids = backing;
    counts = backing + from_size(ids_max);

  } else {
    ids = (uint8_t *) &backing;
    counts = ((uint8_t *) &backing) + from_size(0);
  }

  ids_len += vbyte_store(ids + ids_len, value);
  counts[counts_len++] = count;
  last_id = id;
}

