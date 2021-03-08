#include <vector>
#include <stdint.h>

struct posting {
  std::vector<std::pair<uint64_t, uint8_t>> counts;

  posting() {}

  void append(uint64_t id);

  size_t save(uint8_t *buffer);
  size_t load(uint8_t *buffer);

  std::vector<std::pair<uint64_t, uint64_t>> decompress();
};

