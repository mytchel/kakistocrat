#include <string>
#include <cstring>

#include "hash.h"

uint32_t hash(const char *key, size_t htcap)
{
	uint32_t result = 0;

  while (*key != 0)
		result = (*key++ + 31 * result);

	return result & (htcap - 1);
}

uint32_t hash(const char *key, size_t l, size_t htcap)
{
	uint32_t result = 0;

  while (l-- > 0)
		result = (*key++ + 31 * result);

	return result & (htcap - 1);
}

uint32_t hash(std::string key, size_t htcap)
{
	uint32_t result = 0;

  for (auto &c: key)
		result = (c + 31 * result);

	return result & (htcap - 1);
}



