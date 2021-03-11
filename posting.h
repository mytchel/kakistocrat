#ifndef POSTING_H
#define POSTING_H

#include <vector>
#include <stdint.h>

struct posting {
  std::vector<std::pair<uint64_t, uint8_t>> counts;

  posting() {}

  void append(uint64_t id);

  size_t save(uint8_t *buffer);
  size_t load(uint8_t *buffer);

  std::vector<std::pair<uint64_t, uint64_t>> decompress();

  void merge(posting &other);

  std::vector<std::pair<uint64_t, uint64_t>> to_pairs() {
    std::vector<std::pair<uint64_t, uint64_t>> pairs;

    pairs.reserve(counts.size());
    for (auto &p: counts) {
      pairs.emplace_back(p.first, p.second);
    }

    return pairs;
  }
};

#endif

