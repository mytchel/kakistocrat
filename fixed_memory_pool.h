#ifndef FIXED_MEMORY_POOL_H
#define FIXED_MEMORY_POOL_H

#include <stdint.h>

#include <foonathan/memory/container.hpp>
#include <foonathan/memory/memory_pool.hpp>

#include "foonathan/memory/detail/align.hpp"
#include "foonathan/memory/detail/debug_helpers.hpp"
#include "foonathan/memory/detail/assert.hpp"

using namespace foonathan::memory;

struct fixed_memory_pool {
  size_t node_size, chunk_size;

  size_t usage{0};
  size_t offset{0};
  uint8_t *head{nullptr};
  std::list<void*> chunks;

  fixed_memory_pool(size_t ns, size_t cs)
    : node_size(ns), chunk_size(cs)
  {}

  fixed_memory_pool(fixed_memory_pool &&o)
    : chunks(std::move(o.chunks)),
      node_size(o.node_size), chunk_size(o.chunk_size),
      usage(o.usage), offset(o.offset),
      head(o.head)
  {}

  ~fixed_memory_pool()
  {
    for (auto c: chunks) {
      free(c);
    }
  }

  void clear()
  {
    head = nullptr;
    offset = 0;
    usage = 0;

    for (auto c: chunks) {
      free(c);
    }

    chunks.clear();
  }

  allocator_info info() const noexcept
  {
    return {"::fixed_memory_pool", this};
  }

  void alloc_chunk()
  {
    offset = 0;
    head = (uint8_t *) malloc(node_size * chunk_size);
    if (head == nullptr) {
      throw std::bad_alloc();
    }

    usage += node_size * chunk_size;

    chunks.push_back(head);
  }

  void* allocate_node(size_t size, size_t alignment)
  {
    detail::check_allocation_size<bad_node_size>(size, node_size, info());
    detail::check_allocation_size<bad_alignment>(
        alignment, [&] { return node_size; }, info());

    if (head == nullptr || offset >= chunk_size * node_size) {
      alloc_chunk();
    }

    void *r = &head[offset];

    offset += node_size;

    return r;
  }

  void deallocate_node(void *node, size_t size, size_t alignment) noexcept
  {}
};

#endif

