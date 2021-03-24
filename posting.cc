#include <stdlib.h>
#include <string.h>

#include <cstdio>

#include <vector>
#include <algorithm>

#include "spdlog/spdlog.h"

#include "vbyte.h"
#include "posting.h"

posting::posting(uint8_t *backing)
{
	ids_len = ((uint32_t *) backing)[0];
	counts_len = ((uint32_t *) backing)[1];

  ids = backing + 2 * sizeof(uint32_t);
  counts = ids + ids_len;
}

std::vector<std::pair<uint32_t, uint8_t>>
posting::decompress() const
{
	uint32_t prevI = 0;
	uint32_t docI = 0;
	size_t di = 0;
	size_t ci = 0;

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

size_t posting::save(uint8_t *buffer)
{
  /*
  std::sort(counts.begin(), counts.end(),
      [](auto &a, auto &b) {
        return a.first < b.first;
      }
  );
*/

  ((uint32_t *)buffer)[0] = ids_len;
	((uint32_t *)buffer)[1] = counts_len;

  uint8_t *buf_ids = buffer + sizeof(uint32_t) * 2;
  uint8_t *buf_cnt = buf_ids + ids_len;

  memcpy(buf_ids, ids, ids_len);
  memcpy(buf_cnt, counts, counts_len);

	return size();
}

void posting::reserve(size_t id, size_t cnt,
    std::function<uint8_t* (size_t)> allocator)
{
  if (ids_len + id > ids_max) {
    ids_max = (ids_len + id) * 2;

    uint8_t *n_ids = allocator(ids_max);
    memcpy(n_ids, ids, ids_len);
    ids = n_ids;
  }

  if (counts_len + cnt > counts_max) {
    counts_max = (counts_len + cnt) * 2;

    uint8_t *n_counts = allocator(counts_max);
    memcpy(n_counts, counts, counts_len);
    counts = n_counts;
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
    if (counts[counts_len-1] < 255 - count) {
      counts[counts_len-1] += count;
    } else {
      counts[counts_len-1] = 255;
    }

    return;
  }

  reserve(5, 1, allocator);

  ids_len += vbyte_store(ids + ids_len, id - last_id);
  counts[counts_len++] = count;
  last_id = id;
}

