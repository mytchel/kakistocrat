#include <string>
#include <cstring>
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
  assert(used + size < block_size);

  backing_piece b;

  b.offset = base_offset + used;
  b.size = size;
  b.buf = buf + used;

//  spdlog::info("alloc {} at {}", size, base_offset + used);

  used += size;

  return b;
}

backing_piece write_backing::alloc(uint32_t s)
{
  if (s > block_size) {
    throw std::runtime_error(fmt::format("over sized piece {}", s));
  }

  if (!blocks.empty() && blocks.back().can_alloc(s)) {
    return blocks.back().alloc(s);
  }

  auto &block = blocks.emplace_back(blocks.size() * block_size, block_size);

  if (blocks.size() > 1) {
    spdlog::info("{} now have blocks {}", name, blocks.size());
  }

  return block.alloc(s);
}

}
