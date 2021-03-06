#ifndef DYNAMIC_ARRAY_64_H
#define DYNAMIC_ARRAY_64_H

#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>

struct dynamic_array_64
	{
	uint32_t capacity;
	uint32_t length;
	uint64_t *store;
	};

static inline void dynamic_array_64_init(struct dynamic_array_64 *a)
	{
	a->capacity = 256;
	a->length = 0;
	a->store = (uint64_t *) malloc(a->capacity * sizeof(uint64_t));
  if (a->store == NULL) {
	  fprintf(stderr, "ERROR: memory_alloc() Memory exhausted.");
		exit(1);
  }
	}

static inline void dynamic_array_64_append(struct dynamic_array_64 *a, uint64_t val)
	{
	if (a->length == a->capacity)
		{
		a->capacity *= 2;
		a->store = (uint64_t *) realloc(a->store, a->capacity * sizeof(uint64_t));
    if (a->store == NULL) {
      fprintf(stderr, "ERROR: memory_alloc() Memory exhausted.");
      exit(1);
    }
		}
	a->store[a->length] = val;
	a->length++;
	}

static inline uint64_t *dynamic_array_64_back(struct dynamic_array_64 *a)
	{
	return &a->store[a->length-1];
	}

#endif

