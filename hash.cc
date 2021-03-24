#include <string>
#include <cstring>

#include "hash.h"

uint32_t hash(const char *key)
{
	uint32_t result = 0;

  while (*key != 0)
		result = (*key++ + 31 * result);

	return result & (HTCAP - 1);
}

uint32_t hash(const char *key, size_t l)
{
	uint32_t result = 0;

  while (l-- > 0)
		result = (*key++ + 31 * result);

	return result & (HTCAP - 1);
}

uint32_t hash(std::string key)
{
	uint32_t result = 0;

  for (auto &c: key)
		result = (c + 31 * result);

	return result & (HTCAP - 1);
}



