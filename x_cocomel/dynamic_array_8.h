#ifndef DYNAMIC_ARRAY_8_H
#define DYNAMIC_ARRAY_8_H

#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>

struct dynamic_array_8
	{
	uint32_t capacity;
	uint32_t length;
	uint8_t *store;
	};

static inline void dynamic_array_8_init(struct dynamic_array_8 *a)
	{
	a->capacity = 256;
	a->length = 0;
	a->store = (uint8_t *) malloc(a->capacity);
  if (a->store == NULL) {
	  fprintf(stderr, "ERROR: memory_alloc() Memory exhausted.");
		exit(1);
  }
	}

static inline void dynamic_array_8_append(struct dynamic_array_8 *a, uint8_t val)
	{
	if (a->length == a->capacity)
		{
		a->capacity *= 2;
		a->store = (uint8_t *) realloc(a->store, a->capacity);
    if (a->store == NULL) {
      fprintf(stderr, "ERROR: memory_alloc() Memory exhausted.");
      exit(1);
    }
		}
	a->store[a->length] = val;
	a->length++;
	}

static inline uint8_t *dynamic_array_8_back(struct dynamic_array_8 *a)
	{
	return &a->store[a->length-1];
	}

#endif

