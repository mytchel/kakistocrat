/* ctype functions taken from musl */
/*
		STR.H
		-----
		Copyright (c) 2017 Vaughan Kitchen
		Released under the MIT license (https://opensource.org/licenses/MIT)
*/
/*!
	@file
	@brief Define a String type more useful than cstrings and helpers around that type
	@author Vaughan Kitchen
	@copyright 2017 Vaughan Kitchen
*/

#include <stdint.h>
#include <string.h>
#include <ctype.h>
#include <assert.h>
#include "x_cocomel/memory.h"
#include "str.h"

void str_init(struct str *s, char *buf, size_t max) {
  s->max = max;
  s->store = buf;
  s->len = 0;
}

uint32_t str_max(struct str *s) {
	return s->max;
}

uint32_t str_length(struct str *s) {
  return s->len;
}

void str_resize(struct str *s, uint32_t size) {
  assert(size < s->max);
  s->len = size;
  s->store[s->len] = '\0';
}

char *str_c(struct str *s) {
  return s->store;
}

void str_tolower(struct str *s) {
  char *c = s->store;
	while ((*c = tolower(*c)))
		++c;
}

void str_tostem(struct str *s) {
  if (s->len > 1 && strcmp(s->store + s->len - 1, "s") == 0) {
    s->len -= 1;
    s->store[s->len] = '\0';
  }
}

void str_cat(struct str *s, const char *c) {
  size_t l = strlen(c);
  if (s->len + l + 1 < s->max) {
    memcpy(&s->store[s->len], c, l);
    s->len += l;
    s->store[s->len] = '\0';
  }
}

