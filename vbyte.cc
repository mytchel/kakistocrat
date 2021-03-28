/*
 * Copyright (C) 2005-2016 Christoph Rupp (chris@crupp.de).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <stdint.h>
#include "vbyte.h"

size_t vbyte_len(uint32_t value)
{
	if (value < (1lu << 7)) {
		return 1;
  }

  if (value < (1lu << 14)) {
		return 2;
  }

  if (value < (1lu << 21)) {
		return 3;
  }

  if (value < (1lu << 28)) {
		return 4;
  }

  return 5;
}

int vbyte_read(const uint8_t *in, uint32_t *out)
{
	*out = in[0] & 0x7Flu;
	if (in[0] < 128)
		return 1;
	*out = ((in[1] & 0x7Flu) << 7) | *out;
	if (in[1] < 128)
		return 2;
	*out = ((in[2] & 0x7Flu) << 14) | *out;
	if (in[2] < 128)
		return 3;
	*out = ((in[3] & 0x7Flu) << 21) | *out;
	if (in[3] < 128)
		return 4;
	*out = ((in[4] & 0x7Flu) << 28) | *out;
//	if (in[4] < 128)
		return 5;
}

int vbyte_read(const uint8_t *in, uint64_t *out)
{
	*out = in[0] & 0x7Flu;
	if (in[0] < 128)
		return 1;
	*out = ((in[1] & 0x7Flu) << 7) | *out;
	if (in[1] < 128)
		return 2;
	*out = ((in[2] & 0x7Flu) << 14) | *out;
	if (in[2] < 128)
		return 3;
	*out = ((in[3] & 0x7Flu) << 21) | *out;
	if (in[3] < 128)
		return 4;
	*out = ((in[4] & 0x7Flu) << 28) | *out;
	if (in[4] < 128)
		return 5;
	*out = ((in[5] & 0x7Flu) << 35) | *out;
	if (in[5] < 128)
		return 6;
	*out = ((in[6] & 0x7Flu) << 42) | *out;
	if (in[6] < 128)
		return 7;
	*out = ((in[7] & 0x7Flu) << 49) | *out;
	if (in[7] < 128)
		return 8;
	*out = ((in[8] & 0x7Flu) << 56) | *out;
		return 9;
}

int vbyte_store(uint8_t *p, uint32_t value)
{
	if (value < (1lu << 7)) {
    *p = value & 0x7Fu;
		return 1;
  }

	*p = (value & 0x7Fu) | (1u << 7);
  ++p;

  if (value < (1lu << 14)) {
		*p = value >> 7;
		return 2;
  }

  *p = ((value >> 7) & 0x7Fu) | (1u << 7);
  ++p;

  if (value < (1lu << 21)) {
		*p = value >> 14;
		return 3;
  }

  *p = ((value >> 14) & 0x7Fu) | (1u << 7);
  ++p;

  if (value < (1lu << 28)) {
		*p = value >> 21;
		return 4;
  }

  *p = ((value >> 21) & 0x7Fu) | (1u << 7);
	++p;

//  if (value < (1lu << 35)) {
		*p = value >> 28;
		return 5;
//	}
}

int vbyte_store(uint8_t *p, uint64_t value)
{
	if (value < (1lu << 7)) {
    *p = value & 0x7Fu;
		return 1;
  }

	*p = (value & 0x7Fu) | (1u << 7);
  ++p;

  if (value < (1lu << 14)) {
		*p = value >> 7;
		return 2;
  }

  *p = ((value >> 7) & 0x7Fu) | (1u << 7);
  ++p;

  if (value < (1lu << 21)) {
		*p = value >> 14;
		return 3;
  }

  *p = ((value >> 14) & 0x7Fu) | (1u << 7);
  ++p;

  if (value < (1lu << 28)) {
		*p = value >> 21;
		return 4;
  }

  *p = ((value >> 21) & 0x7Fu) | (1u << 7);
	++p;

  if (value < (1lu << 35)) {
		*p = value >> 28;
		return 5;
	}

  *p = ((value >> 28) & 0x7Fu) | (1u << 7);
	++p;

  if (value < (1lu << 42)) {
		*p = value >> 35;
		return 6;
	}

  *p = ((value >> 35) & 0x7Fu) | (1u << 7);
	++p;

  if (value < (1lu << 49)) {
		*p = value >> 42;
		return 7;
	}

  *p = ((value >> 42) & 0x7Fu) | (1u << 7);
	++p;

  if (value < (1lu << 56)) {
		*p = value >> 49;
		return 8;
	}

  *p = ((value >> 49) & 0x7Fu) | (1u << 7);
	++p;

  // Are we dropping a bit? No?
  //if (value < (1lu << 63)) {
		*p = value >> 56;
		return 9;
	//}
}

