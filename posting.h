#include <vector>
#include <stdint.h>

struct posting {
	uint32_t id;
	uint32_t id_capacity;
	uint32_t id_length;
	uint8_t *id_store;
  std::vector<uint8_t> counts;

  posting();
  ~posting();

  void append(uint32_t id);

  std::vector<std::pair<uint64_t, uint64_t>> decompress();

  size_t save(char *buffer);
  size_t load(char *buffer);
};

