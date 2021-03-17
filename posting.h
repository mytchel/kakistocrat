#ifndef POSTING_H
#define POSTING_H

#include <vector>
#include <list>
#include <stdint.h>

struct posting {
  std::vector<std::pair<uint64_t, uint8_t>> counts;
  uint8_t *backing{NULL};

  posting(uint64_t i) {
    append(i);
  }

  posting(uint8_t *b) : backing{b} {}

  posting(const posting &p) {
    if (p.counts.empty()) {
      counts = p.decompress();
    } else {
      counts = p.counts;
    }
  }

  posting(posting &&p) {
    counts = std::move(p.counts);
    backing = p.backing;
  }

  size_t save_backing(uint8_t *buffer);
  size_t save(uint8_t *buffer);

  size_t backing_size();

  std::vector<std::pair<uint64_t, uint8_t>> decompress() const;

  void append(uint64_t id);
  void merge(posting &other);
};

#endif

