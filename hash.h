#ifndef HASH_H
#define HASH_H

uint32_t hash(std::string key, size_t cap);
uint32_t hash(const char *key, size_t cap);
uint32_t hash(const char *key, size_t l, size_t cap);

#endif
