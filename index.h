#ifndef INDEX_H
#define INDEX_H

#include <stdint.h>
#include <optional>
#include <chrono>

#include <nlohmann/json.hpp>

using namespace std::chrono_literals;

#include "crawl.h"
#include "posting.h"
#include "hash.h"
#include "key.h"
#include "buf_list.h"
#include "fixed_memory_pool.h"

namespace search {

std::vector<std::string> get_split_at(size_t parts = 200);

enum index_type{words, pairs, trines};

const size_t max_index_part_size = 1024 * 1024 * 200;

std::list<std::string> load_parts(std::string path);
void save_parts(std::string path, std::list<std::string>);

void write_buf(std::string path, uint8_t *buf, size_t len);

std::pair<size_t, size_t> save_postings_to_buf(
    forward_list<std::pair<key, posting>, fixed_memory_pool>::iterator start,
    forward_list<std::pair<key, posting>, fixed_memory_pool>::iterator end,
    uint8_t *buffer, size_t buffer_len);


std::pair<size_t, size_t> save_pages_to_buf(
    std::vector<uint64_t> &pages,
    uint8_t *buffer, size_t buffer_len);

std::pair<size_t, size_t> save_pages_to_buf(
    std::list<std::pair<uint64_t, uint32_t>> &pages,
    uint8_t *buffer, size_t buffer_len);

struct index_part {

  fixed_memory_pool pool_store;
  fixed_memory_pool pool_index;

  index_type type;
  std::string path;

  std::string start;
  std::optional<std::string> end;

  std::vector<std::string> store_split;

  uint8_t *backing{NULL};

  buf_list key_backing;
  buf_list post_backing;

  std::vector<
    forward_list<std::pair<key, posting>, fixed_memory_pool>
    > stores;

  std::vector<forward_list<
      std::pair<uint8_t, forward_list<std::pair<key, posting>, fixed_memory_pool>::iterator>,
      fixed_memory_pool
    >> index;

  std::vector<uint64_t> page_ids;

  std::chrono::nanoseconds index_total{0ms};
  std::chrono::nanoseconds merge_total{0ms};
  std::chrono::nanoseconds find_total{0ms};

  // For indexer
  index_part(std::vector<std::string> s_split)
    : pool_store(forward_list_node_size<std::pair<key, posting>>::value,
            1024 * 128),

      pool_index(forward_list_node_size<
          std::pair<
            uint8_t,
            forward_list<std::pair<key, posting>, fixed_memory_pool>::iterator
          >>::value,
          1024 * 128),

      post_backing(1024 * 1024),
      key_backing(1024 * 1024),
      store_split(s_split)
  {
    index.reserve(HTCAP);
    for (size_t i = 0; i < HTCAP; i++) {
      index.emplace_back(pool_index);
    }

    stores.reserve(s_split.size());
    for (auto &s: s_split) {
      stores.emplace_back(pool_store);
    }
  }

  // For merger
  index_part(index_type t, std::string p,
      std::string s, std::optional<std::string> e)
    : pool_store(forward_list_node_size<std::pair<key, posting>>::value,
          1024 * 128),

      pool_index(forward_list_node_size<
          std::pair<
            uint8_t,
            forward_list<std::pair<key, posting>, fixed_memory_pool>::iterator
          >>::value,
          1024 * 128),

      post_backing(1024 * 1024),
      key_backing(1024 * 1024),
      type(t), path(p),
      start(s), end(e)
  {
    index.reserve(HTCAP);
    for (size_t i = 0; i < HTCAP; i++) {
      index.emplace_back(pool_index);
    }

    stores.emplace_back(pool_store);
  }

  index_part(index_part &&p)
    : type(p.type), path(p.path),
      start(p.start), end(p.end),
      pool_store(std::move(p.pool_store)),
      pool_index(std::move(p.pool_index)),
      key_backing(std::move(p.key_backing)),
      post_backing(std::move(p.post_backing)),
      stores(std::move(p.stores)),
      index(std::move(p.index)),
      page_ids(std::move(p.page_ids))
  {
    backing = p.backing;
    p.backing = NULL;
  }

  ~index_part()
  {
    if (backing) {
      free(backing);
    }
  }

  void clear() {
    for (auto &i: index) {
      i.clear();
    }

    for (auto &s: stores) {
      s.clear();
    }

    key_backing.clear();
    post_backing.clear();

    pool_index.clear();
    pool_store.clear();

    page_ids.clear();

    if (backing) {
      free(backing);
      backing = NULL;
    }
  }

  void print_usage(std::string n)
  {
    spdlog::info("usage {} : key {} kb post {} kb pool store {} kb pool index {} kb page {} kb -> {} mb",
          n,
          key_backing.usage / 1024,
          post_backing.usage / 1024,
          pool_store.usage / 1024,
          pool_index.usage / 1024,
          page_ids.size() * sizeof(uint64_t) / 1024,
          usage() / 1024 / 1024);
  }

  size_t usage() {
    return key_backing.usage
         + post_backing.usage
         + pool_store.usage
         + pool_index.usage
         + page_ids.size() * sizeof(uint64_t);
  }

  bool load_backing();
  void load();

  void unload() {
    clear();
  }

  void save();
  size_t save_to_buf(uint8_t *buffer, size_t len);

  void merge(index_part &other);
  void insert(std::string key, uint32_t val);

  forward_list<std::pair<key, posting>, fixed_memory_pool> * get_store(key s)
  {
    auto store_it = stores.begin();
    auto split_it = store_split.begin();

    while (split_it != store_split.end()) {
      if (split_it + 1 == store_split.end() || s < *(split_it + 1)) {
        return &(*store_it);
      }

      store_it++;
      split_it++;
    }

    throw std::invalid_argument("key does not fit into split store");
  }

  void update_index(forward_list<std::pair<key, posting>, fixed_memory_pool>::iterator);
  forward_list<std::pair<key, posting>, fixed_memory_pool>::iterator find(std::string);

  std::tuple<
    bool,

    forward_list<
      std::pair<
        uint8_t,
        forward_list<std::pair<key, posting>, fixed_memory_pool>::iterator
      >,
      fixed_memory_pool
    > *,

    forward_list<
      std::pair<
        uint8_t,
        forward_list<std::pair<key, posting>, fixed_memory_pool>::iterator
      >,
      fixed_memory_pool
    >::iterator>
      find(key);
};

struct index_part_info {
  std::string path;

  std::string start;
  std::optional<std::string> end;

  index_part_info() {}

  index_part_info(std::string p) : path(p) {}

  index_part_info(std::string p, std::string s,
      std::optional<std::string> e)
    : path(p), start(s), end(e) {}
};

void to_json(nlohmann::json &j, const index_part_info &i);
void from_json(const nlohmann::json &j, index_part_info &i);

struct index_info {
  std::string path;

  uint32_t average_page_length;
  std::map<uint64_t, uint32_t> page_lengths;

  std::vector<index_part_info> word_parts;
  std::vector<index_part_info> pair_parts;
  std::vector<index_part_info> trine_parts;

  index_info(std::string p) : path(p) {}

  void load();
  void save();
};

struct indexer {
  std::list<std::pair<uint64_t, uint32_t>> pages;
  index_part word_t, pair_t, trine_t;

  uint8_t *file_buf{nullptr};
  size_t file_buf_size{0};

  std::string base_path;
  size_t flush_count{0};
  std::list<std::string> paths;

  std::vector<index_part_info> save_part(
    index_part &t, std::string base_path);

  std::string save();

  void clear() {
    pages.clear();
    word_t.clear();
    pair_t.clear();
    trine_t.clear();
  }

  void flush() {
    spdlog::info("flushing {}", base_path);

    word_t.print_usage(fmt::format("{}-words", base_path));
    pair_t.print_usage(fmt::format("{}-pair", base_path));
    trine_t.print_usage(fmt::format("{}-trine", base_path));

    paths.emplace_back(save());

    clear();

    flush_count++;
  }

  size_t usage() {
    return word_t.usage()
         + pair_t.usage()
         + trine_t.usage()
         + pages.size() * (16);
  }

  void index_site(crawl::site &site, char *file_buf, size_t file_buf_len);

  void insert(index_type t, std::string s, uint32_t index_id) {
    if (t == words) {
      word_t.insert(s, index_id);
    } else if (t == pairs) {
      pair_t.insert(s, index_id);
    } else if (t == trines) {
      trine_t.insert(s, index_id);
    }
  }

  uint32_t next_id() {
    return pages.size();
  }

  void add_page(uint64_t page_id, uint32_t size) {
    pages.emplace_back(page_id, size);
  }

  indexer(std::string p, std::vector<std::string> split_at)
    : base_path(p),
      word_t(split_at),
      pair_t(split_at),
      trine_t(split_at)
  {
    file_buf_size = max_index_part_size;
    file_buf = (uint8_t *) malloc(max_index_part_size);
    if (file_buf == nullptr) {
      throw std::bad_alloc();
    }
  }

  ~indexer() {
    if (file_buf) {
      free(file_buf);
    }
  }
};

struct index {
  uint32_t average_page_length{0};

  std::map<uint64_t, uint32_t> page_lengths;

  std::vector<index_part> word_parts;
  std::vector<index_part> pair_parts;
  std::vector<index_part> trine_parts;

  std::string path;

  index(std::string p) : path(p) {}

  void load();
  void save();

  void find_part_matches(index_part &p,
    std::list<std::string> &terms,
    std::vector<std::vector<std::pair<uint64_t, double>>> &postings);

  std::vector<std::vector<std::pair<uint64_t, double>>>
    find_matches(char *line);
};

}
#endif

