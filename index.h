#ifndef INDEX_H
#define INDEX_H

#include <stdint.h>
#include <optional>
#include <chrono>
#include <assert.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <nlohmann/json.hpp>
#include "spdlog/spdlog.h"
 
#include "util.h"
#include "site.h"

#include "hash.h"
#include "vbyte.h"

namespace search {

#define key_max_len 250

enum index_type{words, pairs, trines};

index_type from_str(const std::string &s);
std::string to_str(index_type type);

uint32_t part_split(const std::string &s, size_t parts);

struct backing_piece {
  uint8_t *buf;
  size_t offset;
  size_t size;
};

struct backing_block {
  size_t base_offset;
  size_t block_size;
  size_t used{0};

  uint8_t *buf;

  backing_block(size_t o, size_t s)
  : base_offset(o), block_size(s)
  {
    buf = (uint8_t *) malloc(block_size);
    if (buf == nullptr) {
      throw std::bad_alloc();
    }
  }

  ~backing_block() noexcept {
    if (buf != nullptr) {
      free(buf);
    }
  }

  backing_block(const backing_block &b) = delete;
  backing_block(backing_block &b) noexcept
    : base_offset(b.base_offset),
      block_size(b.block_size),
      used(b.used),
      buf(b.buf)
  {
    b.buf = nullptr;
  }

  backing_block(const backing_block &&b) = delete;
  backing_block(backing_block &&b) noexcept
    : base_offset(b.base_offset),
      block_size(b.block_size),
      used(b.used),
      buf(b.buf)
  {
    b.buf = nullptr;
  }

  bool can_alloc(uint32_t size) {
    return block_size - used > size;
  }

  backing_piece alloc(uint32_t size);

  uint8_t *get_data(uint32_t offset) {
    assert(offset < block_size);

    return &buf[offset];
  }
};

struct write_backing {
  std::string name;

  size_t block_size;
  std::vector<backing_block> blocks;

  write_backing(const std::string &n, size_t block_size)
    : name(n), block_size(block_size)
  {}

  backing_piece alloc(uint32_t size);
  backing_piece realloc(uint32_t offset, uint32_t size);

  uint8_t *get_data(uint32_t offset) {
    assert(!blocks.empty());

    uint32_t n = offset / block_size;

    assert(n < blocks.size());

    return blocks[n].get_data(offset - n * block_size);
  }

  size_t usage() {
    return blocks.size() * block_size;
  }

  void clear() {
    blocks.clear();
  }
};

struct read_backing {
  uint8_t *buf{nullptr};
  size_t size{0};

  read_backing() {}

  read_backing(uint8_t *buf, size_t size)
    : buf(buf), size(size) {}

  void setup(uint8_t *b) {
    buf = b;
  }

  uint8_t *get_data(uint32_t offset) {
    //assert(offset < size);

    //spdlog::info("read {}", offset);

    return &buf[offset];
  }
};

struct post {
  uint32_t id;
  uint8_t count;

  post(uint32_t i, uint8_t c)
    : id(i), count(c) {}
};

struct posting_reader {
  uint32_t len;
  uint32_t offset;

  std::vector<post> decompress(read_backing &p);
};

struct posting_writer {
  uint32_t last_id{0};
  uint32_t len{0}, max_len{0};
  uint32_t offset{0};

  posting_writer() {}

  uint8_t * ensure_size(write_backing &p, uint32_t need);
  void append(write_backing &p, uint32_t id, uint8_t count = 1);
  // Can only read from readers
  void merge(write_backing &p, posting_reader &other, read_backing &op, uint32_t id_offset);
};

struct key_entry {
  std::string key;
  uint32_t posting_id;

  key_entry(uint8_t *d, uint32_t l, uint32_t id)
    : key((const char *) d, l), posting_id(id)
  {}
};


struct key_block_reader {
  uint32_t items{0};
  uint32_t offset{0};

  key_block_reader() {}

  uint8_t *get_lens(uint8_t *b) {
    return b;
  }

  uint32_t *get_offsets(uint8_t *b) {
    return (uint32_t *) (b + items * (sizeof(uint8_t)));
  }

  uint32_t *get_ids(uint8_t *b) {
    return (uint32_t *) (b + items * (sizeof(uint8_t) + sizeof(uint32_t)));
  }

  std::vector<key_entry> load(read_backing &m, read_backing &d);
  std::optional<uint32_t> find(read_backing &m, read_backing &d, const std::string &s);
};

struct key_block_writer {
  uint16_t max_items{0};
  uint16_t items{0};
  uint32_t offset{0};

  key_block_writer() {}

  void reset() {
    max_items = 0;
    items = 0;
    offset = 0;
  }

  uint8_t *get_lens(uint8_t *b) {
    return b;
  }

  uint32_t *get_offsets(uint8_t *b) {
    return (uint32_t *) (b + max_items * (sizeof(uint8_t)));
  }

  uint32_t *get_ids(uint8_t *b) {
    return (uint32_t *) (b + max_items * (sizeof(uint8_t) + sizeof(uint32_t)));
  }

  std::vector<key_entry> load(write_backing &m, write_backing &d);
  std::optional<uint32_t> find(write_backing &m, write_backing &d, const std::string &s);
  void add(write_backing &m, write_backing &d, const std::string &s, uint32_t posting_id);
};

struct index_meta {
  uint32_t htcap;
  uint32_t htable_base;
  uint32_t key_meta_base;
  uint32_t key_data_base;
  uint32_t posting_count;
  uint32_t posting_meta_base;
  uint32_t posting_data_base;
};

struct index_reader {
  std::string path;
  uint8_t *buf{nullptr};
  size_t buf_len;
  size_t part_size;

  index_meta meta;

  // doc id 0 is invalid

  // hash
  key_block_reader *keys{nullptr};
  size_t htcap;

  // array
  posting_reader *postings{nullptr};
  size_t posting_count;

  // backings
  read_backing key_meta_backing;
  read_backing key_data_backing;
  read_backing posting_backing;

  index_reader(const std::string &path, size_t buf_len)
    : path(path), buf_len(buf_len)
  {
    buf = (uint8_t *) malloc(buf_len);
    if (buf == nullptr) {
      throw std::bad_alloc();
    }
  }

  ~index_reader() {
    if (buf) {
      free(buf);
    }
  }

  void load();
  std::vector<post> find(const std::string &s);
};

struct index_writer {
  size_t htcap;

  // hash
  //std::vector<key_block_writer> keys;
  key_block_writer *keys{nullptr};

  // array
  std::vector<posting_writer> postings;

  write_backing key_meta_backing;
  write_backing key_data_backing;
  write_backing posting_backing;

  index_writer(size_t htcap, size_t key_m_b, size_t key_d_b, size_t post_b)
    : htcap(htcap),
      key_meta_backing("key_meta", key_m_b),
      key_data_backing("key_data", key_d_b),
      posting_backing("postings", post_b)
  {
    keys = (key_block_writer *) malloc(sizeof(key_block_writer) * htcap);
    if (keys == nullptr) {
      throw std::bad_alloc();
    }

    memset((void *) keys, 0, sizeof(key_block_writer) * htcap);
  }

  index_writer(const index_writer &o) = delete;
  index_writer(const index_writer &&o) = delete;
  index_writer(index_writer &o) = delete;

  index_writer(index_writer &&o)
    : htcap(o.htcap), keys(o.keys),
      postings(std::move(o.postings)),
      key_meta_backing(std::move(o.key_meta_backing)),
      key_data_backing(std::move(o.key_data_backing)),
      posting_backing(std::move(o.posting_backing))
  {
    o.keys = nullptr;
  }

  ~index_writer() {
    if (keys) {
      free(keys);
    }
  }

  size_t usage() {
    return key_meta_backing.usage() +
           key_data_backing.usage() +
           posting_backing.usage() +
           postings.size() * sizeof(posting_writer);
           //keys.size() * sizeof(key_block_writer) +
  }

  void clear() {
    memset((void *) keys, 0, sizeof(key_block_writer) * htcap);

    postings.clear();

    key_meta_backing.clear();
    key_data_backing.clear();
    posting_backing.clear();
  }

  void write_buf(const std::string &path, uint8_t *buf, size_t len);
  void save(const std::string &path, uint8_t *buf, size_t max_len);
  void merge(index_reader &other, uint32_t page_id_offset);
  void insert(const std::string &s, uint32_t page_id);
};

struct index_info {
  std::string path;

  size_t htcap;
  size_t parts;

  uint32_t average_page_length;
  std::vector<std::pair<std::string, uint32_t>> pages;

  std::map<uint32_t, std::string> word_parts;
  std::map<uint32_t, std::string> pair_parts;
  std::map<uint32_t, std::string> trine_parts;

  index_info(std::string p) : path(p) {}

  void load();
  void save();
};

struct indexer {
  size_t splits, htcap;

  std::vector<std::pair<std::string, uint32_t>> pages;

  std::vector<index_writer> word_t, pair_t, trine_t;

  uint8_t *file_buf{nullptr};
  size_t file_buf_size{0};

  indexer(
      size_t splits,
      size_t htcap,
      size_t max_p)
    : splits(splits), htcap(htcap), file_buf_size(max_p)
  {
    spdlog::info("setting up");

    file_buf = (uint8_t *) malloc(file_buf_size);
    if (file_buf == nullptr) {
      throw std::bad_alloc();
    }

    for (size_t i = 0; i < splits; i++) {
      spdlog::info("setting up split {}", i);

      word_t.emplace_back(htcap,
          1024 * 512,
          1024 * 128,
          1024 * 256);

      pair_t.emplace_back(htcap,
          1024 * 512,
          1024 * 128,
          1024 * 256);

      trine_t.emplace_back(htcap,
          1024 * 512,
          1024 * 128,
          1024 * 256);
    }
  }

  ~indexer() {
    if (file_buf) {
      free(file_buf);
    }
  }

  void clear() {
    pages.clear();
    for (auto &p: word_t) p.clear();
    for (auto &p: pair_t) p.clear();
    for (auto &p: trine_t) p.clear();
  }

  std::string save(const std::string &path);

  std::map<uint32_t, std::string> save_parts(
    std::vector<index_writer> &t,
    const std::string &base_path,
    uint8_t *buf, size_t buf_len);

  std::string flush(const std::string &base_path) {
    spdlog::info("flushing {}", base_path);

    auto meta_path = save(base_path);

    clear();

    return meta_path;
  }

  size_t usage() {
    size_t u = 0;

    for (auto &p: word_t) u += p.usage();
    for (auto &p: pair_t) u += p.usage();
    for (auto &p: trine_t) u += p.usage();

    u += pages.size() * 64;

    return u;
  }

  void index_site(site_map &site);

  void insert(std::vector<index_writer> &t,
      const std::string &s, uint32_t page_id);

  void insert(index_type t, const std::string &s, uint32_t page_id);

  uint32_t add_page(const std::string &page) {
    pages.emplace_back(page, 0);

    return pages.size() - 1;
  }

  void set_page_size(const std::string &page, uint32_t size) {
    auto &p = pages.back();
    assert (p.first == page);
    p.second = size;
  }
};

struct searcher {
  index_info info;
  size_t max_part_size;

  searcher(std::string p, size_t max_part_size)
      : info(p), max_part_size(max_part_size) {}

  void load() {
    info.load();
  }

  void find_part_matches(index_reader &p,
    const std::string &term,
    std::vector<std::vector<std::pair<std::string, double>>> &postings);

  void find_matches(
    std::map<uint32_t, std::string> &parts,
    std::list<std::string> &terms,
    std::vector<std::vector<std::pair<std::string, double>>> &postings);

  std::vector<std::vector<std::pair<std::string, double>>>
    find_matches(char *line);
};

std::list<std::pair<std::string, double>>
intersect_postings(std::vector<std::vector<std::pair<std::string, double>>> &postings);

}

#endif

