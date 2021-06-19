#include <stdint.h>
#include <optional>
#include <chrono>
#include <assert.h>
#include <sys/stat.h>
#include <sys/types.h>

#include "index.h"

namespace search {

std::vector<key_entry> key_block_reader::load(read_backing &m, read_backing &d) {
  if (items == 0) {
    return {};
  }

  uint8_t *b = m.get_data(offset);

  auto lens = get_lens(b);
  auto offsets = get_offsets(b);
  auto ids = get_ids(b);

  std::vector<key_entry> entries;
  entries.reserve(items);

  for (size_t i = 0; i < items; i++) {
    uint8_t *data = d.get_data(offsets[i]);
    entries.emplace_back(data, lens[i], ids[i]);
  }

  return entries;
}

std::optional<uint32_t> key_block_reader::find(read_backing &m, read_backing &d, const std::string &s)
{
  if (items == 0) {
    return {};
  }

  uint8_t *b = m.get_data(offset);

  auto lens = get_lens(b);
  auto offsets = get_offsets(b);
  auto ids = get_ids(b);

  size_t len = s.size();
  const uint8_t *s_data = (const uint8_t *) s.data();

  for (size_t i = 0; i < items; i++) {
    if (lens[i] == len) {
      const uint8_t *data = d.get_data(offsets[i]);

      if (memcmp(s_data, data, len) == 0) {
        return ids[i];
      }
    } else if (lens[i] > len) {
      break;
    }
  }

  return {};
}

std::vector<key_entry> key_block_writer::load(write_backing &m, write_backing &d) {
  if (items == 0) {
    return {};
  }

  uint8_t *b = m.get_data(offset);

  auto lens = get_lens(b);
  auto offsets = get_offsets(b);
  auto ids = get_ids(b);

  std::vector<key_entry> entries;
  entries.reserve(items);

  for (size_t i = 0; i < items; i++) {
    uint8_t *data = d.get_data(offsets[i]);
    entries.emplace_back(data, lens[i], ids[i]);
  }

  return entries;
}

std::optional<uint32_t> key_block_writer::find(write_backing &m, write_backing &d, const std::string &s)
{
  if (items == 0) {
    return {};
  }

  uint8_t *b = m.get_data(offset);

  auto lens = get_lens(b);
  auto offsets = get_offsets(b);
  auto ids = get_ids(b);

  size_t len = s.size();
  const uint8_t *s_data = (const uint8_t *) s.data();

  for (size_t i = 0; i < items; i++) {
    if (lens[i] == len) {
      const uint8_t *data = d.get_data(offsets[i]);

      if (memcmp(s_data, data, len) == 0) {
        return ids[i];
      }
    } else if (lens[i] > len) {
      break;
    }
  }

  return {};
}

void key_block_writer::add(write_backing &m, write_backing &d, const std::string &s, uint32_t posting_id)
{
  assert(s.size() < key_max_len);

  size_t loc = 0;
  uint8_t *lens;
  uint32_t *offsets;
  uint32_t *ids;

  size_t s_len = s.size();

  if (items > 0) {
    uint8_t *b = m.get_data(offset);

    uint8_t *old_lens = get_lens(b);
    uint32_t *old_offsets = get_offsets(b);
    uint32_t *old_ids = get_ids(b);

    while (loc < items && old_lens[loc] <= s_len)
      loc++;

    if (items + 1 >= max_items) {
      max_items *= 2;

      backing_piece back = m.alloc(max_items * (sizeof(uint8_t) + sizeof(uint32_t) * 2));
      offset = back.offset;

      uint8_t *n = back.buf;

      lens = get_lens(n);
      offsets = get_offsets(n);
      ids = get_ids(n);

      if (loc > 0) {
        memmove(lens, old_lens, loc * sizeof(uint8_t));
        memmove(offsets, old_offsets, loc * sizeof(uint32_t));
        memmove(ids, old_ids, loc * sizeof(uint32_t));
      }

      if (loc < items) {
        memmove(lens + loc + 1, old_lens + loc, (items - loc) * sizeof(uint8_t));
        memmove(offsets + loc + 1, old_offsets + loc, (items - loc) * sizeof(uint32_t));
        memmove(ids + loc + 1, old_ids + loc, (items - loc) * sizeof(uint32_t));
      }

      // TODO: free

    } else {
      lens = old_lens;
      offsets = old_offsets;
      ids = old_ids;

      if (loc < items) {
        memmove(lens + loc + 1, old_lens + loc, (items - loc) * sizeof(uint8_t));
        memmove(offsets + loc + 1, old_offsets + loc, (items - loc) * sizeof(uint32_t));
        memmove(ids + loc + 1, old_ids + loc, (items - loc) * sizeof(uint32_t));
      }
    }

  } else {
    max_items = 8;

    backing_piece back = m.alloc(max_items * (sizeof(uint8_t) + sizeof(uint32_t) * 2));
    offset = back.offset;

    uint8_t *n = back.buf;

    lens = get_lens(n);
    offsets = get_offsets(n);
    ids = get_ids(n);
  }

  backing_piece b = d.alloc(s_len);

  memcpy(b.buf, s.data(), s_len);

  lens[loc] = s_len;
  offsets[loc] = b.offset;
  ids[loc] = posting_id;

  items++;
}

}
