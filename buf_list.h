#ifndef BUF_LIST_H
#define BUF_LIST_H

#include <list>

struct buf_list {
  size_t usage{0};

  std::list<uint8_t *> old;
  uint8_t *cur{NULL};
  size_t off{0};

  size_t buf_size{1024};

  buf_list(size_t s = 1024) : buf_size(s) {}

  buf_list(buf_list &&b)
    : old(std::move(b.old))
  {
    buf_size = b.buf_size;

    off = b.off;
    cur = b.cur;
    b.cur = NULL;
  }

  ~buf_list() {
    clear();
  }

  void clear() {
    if (cur) {
      free(cur);
      cur = NULL;
    }

    for (auto b: old) {
      free(b);
    }

    old.clear();

    off = 0;
    usage = 0;
  }

  uint8_t *get(size_t l) {
    if (l > buf_size) {
      spdlog::warn("{} too big for buf list", l);
      uint8_t *b = (uint8_t *) malloc(l);
      old.push_back(b);

      usage += l;

      return b;
    }

    if (cur == NULL || off + l >= buf_size) {
      if (cur != NULL) {
        old.push_back(cur);
      }

      cur = (uint8_t *) malloc(buf_size);
      if (cur == NULL) {
        throw std::bad_alloc();
      }

      usage += buf_size;
      off = 0;
    }

    uint8_t *b = cur + off;
    off += l;

    return b;
  }
};

#endif

