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
  s->len = size;
}

char *str_c(struct str *s) {
	return s->store;
}

char *str_dup_c(struct str *s) {
	uint32_t len = str_length(s);
	char *dest = (char*) memory_alloc(len + 1);
	memcpy(dest, s->store, len);
    dest[len] = '\0';
	return dest;
}

