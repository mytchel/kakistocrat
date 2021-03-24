#ifndef POSTING_H
#define POSTING_H

#include <vector>
#include <list>
#include <stdint.h>

struct posting {
  uint32_t last_id{0};

  uint8_t *ids{NULL};
  uint32_t ids_len{0}, ids_max{0};

  uint8_t *counts{NULL};
  uint32_t counts_len{0}, counts_max{0};

  posting() {}

  posting(uint8_t *b);

  posting(const posting &p, std::function<uint8_t* (size_t)> allocator) {
    ids_max = p.ids_len;
    ids_len = p.ids_len;
    ids = allocator(ids_max);

    memcpy(ids, p.ids, ids_len);

    counts_max = p.counts_len;
    counts_len = p.counts_len;
    counts = allocator(counts_max);

    memcpy(counts, p.counts, counts_len);
  }

  posting(posting &&p) {
    ids = p.ids;
    ids_len = p.ids_len;
    ids_max = p.ids_max;

    counts = p.counts;
    counts_len = p.counts_len;
    counts_max = p.counts_max;
  }

  size_t save(uint8_t *buffer);

  inline size_t usage()
  {
    return sizeof(posting) + ids_max + counts_max;
  }

  inline size_t size()
  {
    return sizeof(uint32_t) * 2 + ids_len + counts_len;
  }

  std::vector<std::pair<uint32_t, uint8_t>> decompress() const;

  void reserve(size_t id, size_t cnt, std::function<uint8_t* (size_t)> allocator);

  void append(uint32_t id, uint8_t count, std::function<uint8_t* (size_t)> allocator);
  void merge(posting &other, uint32_t id_add, std::function<uint8_t* (size_t)> allocator);

  bool only_one() {
    if (counts_len == 0) return true;
    if (counts_len > 1) return false;
    return counts[0] == 1;
  }
};

#endif

