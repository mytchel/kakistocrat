#ifndef POSTING_H
#define POSTING_H

#include <vector>
#include <list>
#include <stdint.h>

struct posting {
  std::list<std::pair<uint64_t, uint8_t>> counts;
  uint8_t *backing{NULL};

  posting(uint64_t i) {
    append(i);
  }

  posting(uint8_t *b) : backing{b} {}

  posting(const posting &p) {
    if (p.counts.empty()) {
      decompress(p.backing);
    } else {
      counts = p.counts;
    }
  }

  size_t save_backing(uint8_t *buffer);
  size_t save(uint8_t *buffer);

  size_t backing_size();

  void unload() {
    counts.clear();
  }

  void decompress(const uint8_t *backing);

  std::list<std::pair<uint64_t, uint8_t>> & to_pairs() {
    if (counts.empty()) {
      decompress(backing);
    }

    return counts;
  }

  void append(uint64_t id);
  void merge(posting &other);
};

#endif

