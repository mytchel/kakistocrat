#ifndef POSTING_H
#define POSTING_H

#include <vector>
#include <stdint.h>

struct posting {
  std::vector<std::pair<uint64_t, uint8_t>> counts;
  uint8_t *backing{NULL};

  posting(uint64_t i) {
    append(i);
  }

  posting(uint8_t *b) : backing{b} {}

  void append(uint64_t id);

  size_t save(uint8_t *buffer);

  size_t backing_size();

  void unload() {
    counts.clear();
  }

  void decompress();

  std::vector<std::pair<uint64_t, uint8_t>> to_pairs() {
    if (counts.empty()) {
      decompress();
    }

    return counts;
  }

  void merge(posting &other);
};

#endif

