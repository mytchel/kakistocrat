#include <string>
#include <cstring>
#include <cmath>
#include <map>
#include <utility>
#include <algorithm>
#include <vector>
#include <iostream>
#include <fstream>
#include <sstream>
#include <cstdint>
#include <chrono>

#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <fcntl.h>

#include "spdlog/spdlog.h"

#include "index.h"

namespace search {

backing_piece backing_block::alloc(uint32_t size)
{
  assert(head + size < block_size);

  backing_piece back;

  back.offset = base_offset + head;
  back.size = size;
  back.buf = buf + head;

  head += size;

  return back;
}

std::optional<backing_piece> write_backing::get_free(uint32_t size)
{
  uint32_t b = 0;
  uint32_t m = free_pieces.size();

  for (uint32_t b = 0; b < m; b++) {
    uint32_t bs = (1 << (7 + b * 2));

    if (bs >= size && !free_pieces[b].empty()) {
      auto offset = free_pieces[b].back();
      free_pieces[b].pop_back();

      backing_piece back;
      back.offset = offset;
      back.size = bs;
      back.buf = get_data(offset);

      spdlog::trace("{} using freed block {} {}", name, offset, bs);

      return back;
    }
  }

  return {};
}

void write_backing::free_block(uint32_t offset, uint32_t size)
{
  uint32_t b = 0;
  uint32_t m = free_pieces.size();

  for (uint32_t b = 0; b < m; b++) {
    // To match the posting growth rate.
    uint32_t bs = (1 << (7 + b * 2));

    if (bs == size) {
      free_pieces[b].push_back(offset);

    } else if (bs > size) {
      break;
    }
  }
}

backing_piece write_backing::alloc_big(uint32_t s)
{
  if (s > block_size * 10) {
    throw std::runtime_error(fmt::format("over sized piece {}", s));
  }

  backing_piece back;

  back.size = ceil(s / block_size) * block_size;

  spdlog::warn("alloc BIG block {} kb for {}", name, back.size / 1024);

  back.offset = blocks.size() * block_size;
  back.buf = (uint8_t *) malloc(back.size);

  if (back.buf == nullptr) {
    spdlog::warn("bad alloc adding BIG block ({} kb) for {} with {} blocks using {} kb",
      name, back.size / 1024, blocks.size(), usage() / 1024);

    throw std::bad_alloc();
  }

  bool own = true;
  for (uint32_t off = 0; off < back.size; off += block_size) {
    blocks.emplace_back(name, blocks.size() * block_size, block_size, own, back.buf + off);
    own = false;
  }

  return back;
}

backing_piece write_backing::alloc(uint32_t s)
{
  if (s > block_size) {
    return alloc_big(s);
  }

  auto maybe = get_free(s);
  if (maybe) {
    return *maybe;
  }

  if (!blocks.empty() && blocks.back().can_alloc(s)) {
    return blocks.back().alloc(s);
  }
    
  try {
    auto &block = blocks.emplace_back(name, blocks.size() * block_size, block_size);

    if (blocks.size() > 1 && blocks.size() % 10 == 0) {
      spdlog::debug("{} now have blocks {}", name, blocks.size());
    }

    return block.alloc(s);

  } catch (std::bad_alloc &exception) {
    spdlog::warn("bad alloc adding block for {} with {} blocks using {} kb",
      name, blocks.size(), usage() / 1024);

    throw exception;
  }
}

}
