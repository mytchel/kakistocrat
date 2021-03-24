#ifndef HASH_H
#define HASH_H

const size_t HTCAP = (1 << 16);

uint32_t hash(std::string key);
uint32_t hash(const char *key);
uint32_t hash(const char *key, size_t l);

#endif
