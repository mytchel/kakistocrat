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

#ifndef STR_H
#define STR_H

#include <stdint.h>
#include <string.h>
#include <ctype.h>
#include "memory.h"

struct str {
  size_t max;
  size_t len;
	char *store;
};

void str_init(struct str *s, char *buf, size_t max);

uint32_t str_max(struct str *s);

uint32_t str_length(struct str *s);

void str_resize(struct str *s, uint32_t size);

char *str_c(struct str *s);

char *str_dup_c(struct str *s);

#endif

