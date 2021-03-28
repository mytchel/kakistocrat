#ifndef POSTING_H
#define POSTING_H

#include <vector>
#include <list>
#include <stdint.h>

const size_t min_backing = (sizeof(uint8_t *) / 2);

struct posting {
  uint32_t last_id{0};

  uint8_t *backing{NULL};

  uint32_t ids_len{0}, counts_len{0};

  uint8_t ids_max{0}, counts_max{0};

  posting() {}

  posting(uint8_t *b);

  size_t from_size(uint8_t s) const {
    return min_backing << s;
  }

  posting(const posting &p, std::function<uint8_t* (size_t)> allocator) {
    ids_len = p.ids_len;
    ids_max = p.ids_max;

    counts_len = p.counts_len;
    counts_max = p.counts_max;

    if (ids_max > 0 || counts_max > 0) {
      backing = allocator(from_size(ids_max) + from_size(counts_max));
      memcpy(backing, p.backing, ids_len);
      memcpy(backing + from_size(ids_max), p.backing + from_size(p.ids_max), counts_len);

    } else {
      backing = p.backing;
    }
  }

  posting(posting &&p) {
    ids_len = p.ids_len;
    ids_max = p.ids_max;

    counts_len = p.counts_len;
    counts_max = p.counts_max;

    backing = p.backing;
  }

  size_t save(uint8_t *buffer) const;

  inline size_t usage() const
  {
    return sizeof(posting) + from_size(ids_max) + from_size(counts_max);
  }

  inline size_t size() const
  {
    return sizeof(uint32_t) * 2 + ids_len + counts_len;
  }

  std::vector<std::pair<uint32_t, uint8_t>> decompress() const;

  void reserve(size_t id, size_t cnt, std::function<uint8_t* (size_t)> allocator);

  void append(uint32_t id, uint8_t count, std::function<uint8_t* (size_t)> allocator);
  void merge(posting &other, uint32_t id_add, std::function<uint8_t* (size_t)> allocator);
};

#endif

