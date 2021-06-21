#include <stdint.h>
#include <optional>
#include <chrono>
#include <assert.h>
#include <sys/stat.h>
#include <sys/types.h>

using namespace std::chrono_literals;

#include "index.h"

namespace search {

std::vector<post> posting_reader::decompress(read_backing &p)
{
  uint8_t *b = p.get_data(offset);

  std::vector<post> posts;

  uint32_t id, prev_id = 0;
  uint32_t o = 0;

  while (o < len) {
    o += vbyte_read(&b[o], &id);
    id += prev_id;
    prev_id = id;

    uint8_t count = b[o];
    o++;

    posts.emplace_back(id, count);
  }

  return posts;
}

uint8_t * posting_writer::ensure_size(write_backing &p, uint32_t need)
{
  if (need < max_len) {
    return p.get_data(offset);
  }

  uint32_t n_max = max_len;

  if (n_max < 32) {
    n_max = 32;
  }

  do {
    n_max = n_max * 4;
  } while (n_max <= need);

  backing_piece b = p.alloc(n_max);

  if (len > 0) {
    uint8_t *old = p.get_data(offset);
    memcpy(b.buf, old, len);
    p.free_block(offset, max_len);
  }

  offset = b.offset;
  max_len = b.size;

  return b.buf;
}

void posting_writer::append(write_backing &p, uint32_t id, uint8_t count)
{
  if (id == last_id && len > 0) {
    uint8_t *b = p.get_data(offset);
    if (b[len-1] < 255) {
      b[len-1]++;
    }

    return;
  }

  uint8_t *b = ensure_size(p, len + 5 + 1);

  uint32_t value = id - last_id;

  len += vbyte_store(b + len, value);
  b[len++] = count;
  last_id = id;
}

void posting_writer::merge(write_backing &p, posting_reader &other, read_backing &op, uint32_t id_offset)
{
  auto posts = other.decompress(op);

  ensure_size(p, len + other.len + 5);

  for (auto &post: posts) {
    append(p, post.id + id_offset, post.count);
  }
}

}


