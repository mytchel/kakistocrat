#ifndef POSTING_H
#define POSTING_H

#include <vector>
#include <list>
#include <stdint.h>

struct posting {
  uint64_t last_id{0};

  uint8_t *ids{NULL};
  size_t ids_len{0}, ids_max{0};

  uint8_t *counts{NULL};
  size_t counts_len{0}, counts_max{0};

  posting(uint64_t i) {
    append(i);
  }

  ~posting() {
    if (ids_max > 0) {
      free(ids);
      free(counts);
    }
  }

  posting(uint8_t *b);

  posting(const posting &p) {
    ids_max = p.ids_len;
    ids_len = p.ids_len;
    ids = (uint8_t *) malloc(ids_max);
    if (ids == NULL) {
      throw std::bad_alloc();
    }

    memcpy(ids, p.ids, ids_len);

    counts_max = p.counts_len;
    counts_len = p.counts_len;
    counts = (uint8_t *) malloc(counts_max);
    if (counts == NULL) {
      throw std::bad_alloc();
    }

    memcpy(counts, p.counts, counts_len);
  }

  posting(posting &&p) {
    ids = p.ids;
    ids_len = p.ids_len;
    ids_max = p.ids_max;

    p.ids_max = 0;

    counts = p.counts;
    counts_len = p.counts_len;
    counts_max = p.counts_max;

    p.counts_max = 0;
  }

  size_t save(uint8_t *buffer);

  size_t size();

  std::vector<std::pair<uint64_t, uint8_t>> decompress() const;

  void reserve(size_t id, size_t cnt);

  void append(uint64_t id);
  void merge(posting &other);

  bool only_one() {
    if (counts_len == 0) return true;
    if (counts_len > 1) return false;
    return counts[0] == 1;

  }
};

#endif

