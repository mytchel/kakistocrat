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

  auto lens = get_lens(b, max_items);
  auto offsets = get_offsets(b, max_items);
  auto ids = get_ids(b, max_items);

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

  auto lens    = get_lens(b, max_items);
  auto offsets = get_offsets(b, max_items);
  auto ids     = get_ids(b, max_items);

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

void key_block_writer::add(write_backing &m, write_backing &d,
          const std::string &s, uint32_t posting_id,
          uint16_t base_items)
{
  assert(s.size() < key_max_len);

  size_t loc = 0;
  size_t s_len = s.size();

  uint8_t *lens;
  uint32_t *offsets;
  uint32_t *ids;

  if (items > 0) {
    uint8_t *b = m.get_data(offset);

    lens    = get_lens(b, max_items);
    offsets = get_offsets(b, max_items);
    ids     = get_ids(b, max_items);

    while (loc < items && lens[loc] <= s_len)
      loc++;

    if (items + 1 >= max_items) {
      backing_piece back = m.alloc(4 * max_items * (sizeof(uint8_t) + sizeof(uint32_t) * 2));

      uint8_t *n = back.buf;
      uint32_t new_offset = back.offset;
      uint32_t new_max_items = back.size / (sizeof(uint8_t) + sizeof(uint32_t) * 2);

      auto new_lens    = get_lens(n, new_max_items);
      auto new_offsets = get_offsets(n, new_max_items);
      auto new_ids     = get_ids(n, new_max_items);

      if (loc > 0) {
        memcpy(new_lens, lens,
               loc * sizeof(uint8_t));

        memcpy(new_offsets, offsets,
               loc * sizeof(uint32_t));

        memcpy(new_ids, ids,
               loc * sizeof(uint32_t));
      }

      if (loc < items) {
        memcpy(new_lens + loc + 1,
               lens + loc,
               (items - loc) * sizeof(uint8_t));

        memcpy(new_offsets + loc + 1,
               offsets + loc,
               (items - loc) * sizeof(uint32_t));

        memcpy(new_ids + loc + 1,
               ids + loc,
               (items - loc) * sizeof(uint32_t));
      }

/*
      memmove(new_lens, lens, items * sizeof(uint8_t));
      memmove(new_offsets, offsets, items * sizeof(uint32_t));
      memmove(new_ids, ids, items * sizeof(uint32_t));
*/

      // freeing doesn't make much sense here.
      // eveything gets filled out and then grows.
      // the free'd blocks do not get used.
      //m.free_block(offset, max_len);

      lens    = new_lens;
      offsets = new_offsets;
      ids     = new_ids;

      offset    = new_offset;
      max_items = new_max_items;

      spdlog::trace("key meta grow {} > {}", back.size, max_items);

    } else {
      if (loc < items) {
        // memmove because overlapping buffers

        memmove(lens + loc + 1,
                lens + loc,
                (items - loc) * sizeof(uint8_t));

        memmove(offsets + loc + 1,
                offsets + loc,
                (items - loc) * sizeof(uint32_t));

        memmove(ids + loc + 1,
                ids + loc,
                (items - loc) * sizeof(uint32_t));
      }
    }

  } else {
    backing_piece back = m.alloc(base_items * (sizeof(uint8_t) + sizeof(uint32_t) * 2));

    uint8_t *b = back.buf;
    offset    = back.offset;
    max_items = back.size / (sizeof(uint8_t) + sizeof(uint32_t) * 2);

    lens    = get_lens(b, max_items);
    offsets = get_offsets(b, max_items);
    ids     = get_ids(b, max_items);
  }

  backing_piece b = d.alloc(s_len);

  memcpy(b.buf, s.data(), s_len);

  lens[loc] = s_len;
  offsets[loc] = b.offset;
  ids[loc] = posting_id;

  items++;
}

}
