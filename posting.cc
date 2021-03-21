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

size_t posting::size()
{
  return sizeof(uint32_t) * 2 + ids_len + counts_len;
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

void posting::reserve(size_t id, size_t cnt) {
  if (ids_len + id > ids_max) {
    ids_max = (ids_len + id) * 2;
    ids = (uint8_t *) realloc(ids, ids_max);
    if (ids == NULL) {
      throw std::bad_alloc();
    }
  }

  if (counts_len + cnt > counts_max) {
    counts_max = (counts_len + cnt) * 2;
    counts = (uint8_t *) realloc(counts, counts_max);
    if (counts == NULL) {
      throw std::bad_alloc();
    }
  }
}

uint32_t get_last_id(uint8_t *ids, size_t ids_len)
{
	uint32_t prevI = 0;
	uint32_t docI = 0;
	size_t di = 0;
	size_t ci = 0;

	while (di < ids_len) {
		di += vbyte_read(&ids[di], &docI);
		docI += prevI;
		prevI = docI;
	}

  return docI;
}

void posting::merge(posting &other, uint32_t id_add)
{
  reserve(5 + other.ids_len, other.counts_len);

  if (counts_len > 0 && last_id == 0) {
    last_id = get_last_id(ids, ids_len);
  }

  uint32_t first_id;
  size_t offset;
  
  offset = vbyte_read(other.ids, &first_id);

  ids_len += vbyte_store(ids + ids_len, first_id + id_add - last_id);

  memcpy(ids + ids_len, other.ids + offset, other.ids_len - offset);

  ids_len += other.ids_len - offset;

  memcpy(counts + counts_len, other.counts, other.counts_len);
  counts_len += other.counts_len;

  last_id = get_last_id(other.ids, other.ids_len) + id_add;
}

size_t posting::append(uint32_t id, uint8_t count)
{
  if (counts_len > 0 && id == last_id) {
    counts[counts_len-1] += count;
    return 0;
  }

  reserve(5, 1);

  size_t l = vbyte_store(ids + ids_len, id - last_id);
  counts[counts_len++] = count;
  last_id = id;

  ids_len += l;

  return l + 1;
}

