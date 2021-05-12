#ifndef INDEX_H
#define INDEX_H

#include <stdint.h>
#include <optional>
#include <chrono>

#include <nlohmann/json.hpp>

using namespace std::chrono_literals;

#include "util.h"
#include "site.h"

#include "posting.h"
#include "hash.h"
#include "key.h"
#include "buf_list.h"
#include "fixed_memory_pool.h"

namespace search {

std::vector<std::string> get_split_at(size_t parts);

enum index_type{words, pairs, trines};

std::list<std::string> load_parts(std::string path);
void save_parts(std::string path, std::list<std::string>);

void write_buf(std::string path, uint8_t *buf, size_t len);

std::pair<size_t, size_t> save_postings_to_buf(
    forward_list<std::pair<key, posting>, fixed_memory_pool>::iterator start,
    forward_list<std::pair<key, posting>, fixed_memory_pool>::iterator end,
    uint8_t *buffer, size_t buffer_len);


std::pair<size_t, size_t> save_pages_to_buf(
    std::vector<std::string> &pages,
    uint8_t *buffer, size_t buffer_len);

std::pair<size_t, size_t> save_pages_to_buf(
    std::list<std::pair<uint64_t, uint32_t>> &pages,
    uint8_t *buffer, size_t buffer_len);

struct index_part {

  size_t htcap;

  fixed_memory_pool pool_store;
  fixed_memory_pool pool_index;

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

  std::vector<
      forward_list<std::pair<key, posting> *, fixed_memory_pool>
    > index;

  std::vector<std::string> pages;

  std::chrono::nanoseconds index_total{0ms};
  std::chrono::nanoseconds merge_total{0ms};
  std::chrono::nanoseconds find_total{0ms};

  // For indexer
  index_part(std::vector<std::string> s_split, size_t cap)
    : htcap(cap),

      pool_store(forward_list_node_size<std::pair<key, posting>>::value,
            1024 * 128),

      pool_index(
          forward_list_node_size<
            std::pair<key, posting> *
          >::value,
          1024 * 128),

      post_backing(1024 * 1024),
      key_backing(1024 * 1024),
      store_split(s_split)
  {
    index.reserve(htcap);
    for (size_t i = 0; i < htcap; i++) {
      index.emplace_back(pool_index);
    }

    stores.reserve(s_split.size());
    for (auto &s: s_split) {
      stores.emplace_back(pool_store);
    }
  }

  // For merger
  index_part(std::string p, size_t cap,
      std::string s, std::optional<std::string> e)
    : htcap(cap),

      pool_store(forward_list_node_size<std::pair<key, posting>>::value,
          1024 * 128),

      pool_index(
          forward_list_node_size<
            std::pair<key, posting> *
          >::value,
          1024 * 128),

      post_backing(1024 * 1024),
      key_backing(1024 * 1024),
      path(p),
      start(s), end(e)
  {
    index.reserve(htcap);
    for (size_t i = 0; i < htcap; i++) {
      index.emplace_back(pool_index);
    }

    stores.emplace_back(pool_store);
  }

  index_part(index_part &&p)
    : path(p.path),
      htcap(p.htcap),
      start(p.start), end(p.end),
      pool_store(std::move(p.pool_store)),
      pool_index(std::move(p.pool_index)),
      key_backing(std::move(p.key_backing)),
      post_backing(std::move(p.post_backing)),
      stores(std::move(p.stores)),
      index(std::move(p.index)),
      pages(std::move(p.pages))
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

    pages.clear();

    if (backing) {
      free(backing);
      backing = NULL;
    }
  }

  void print_usage(std::string n)
  {
    spdlog::info("usage {} : key {} kb post {} kb pool store {} kb pool index {} kb -> {} mb",
          n,
          key_backing.usage / 1024,
          post_backing.usage / 1024,
          pool_store.usage / 1024,
          pool_index.usage / 1024,
          usage() / 1024 / 1024);
  }

  size_t usage() {
    return key_backing.usage
         + post_backing.usage
         + pool_store.usage
         + pool_index.usage;
  }

  bool load_backing();
  void load();

  void unload() {
    clear();
  }

  void save(uint8_t *buffer, size_t len);

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

  void update_index(std::pair<key, posting> *);
  std::pair<key, posting> * find(std::string);

  std::tuple<
    bool,

    forward_list<
        std::pair<key, posting> *,
      fixed_memory_pool
    > *,

    forward_list<
        std::pair<key, posting> *,
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
  std::map<std::string, uint32_t> page_lengths;

  std::vector<index_part_info> word_parts;
  std::vector<index_part_info> pair_parts;
  std::vector<index_part_info> trine_parts;

  index_info(std::string p) : path(p) {}

  void load();
  void save();
};

struct indexer {
  std::list<std::pair<std::string, uint32_t>> pages;
  index_part word_t, pair_t, trine_t;

  uint8_t *file_buf{nullptr};
  size_t file_buf_size{0};

  size_t max_usage;

  std::string base_path{"/tmp/bad"};

  std::vector<index_part_info> save_part(
    index_part &t, std::string base_path);

  std::string save();

  size_t flush_count{0};
  std::function<void(const std::string &)> on_flush;

  void clear() {
    pages.clear();
    word_t.clear();
    pair_t.clear();
    trine_t.clear();
  }

  void reset() {
    clear();

    flush_count = 0;
  }

  void flush() {
    spdlog::info("flushing {}", base_path);

    word_t.print_usage(fmt::format("{}-words", base_path));
    pair_t.print_usage(fmt::format("{}-pair", base_path));
    trine_t.print_usage(fmt::format("{}-trine", base_path));

    auto path = save();

    clear();

    flush_count++;

    if (on_flush) {
      on_flush(path);
    }
  }

  size_t usage() {
    return word_t.usage()
         + pair_t.usage()
         + trine_t.usage()
         + pages.size() * (16);
  }

  void check_usage() {
    if (usage() > max_usage) {
      spdlog::info("indexer using {}", usage());

      flush();
    }
  }

  void index_site(site_map &site, char *file_buf, size_t file_buf_len);

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

  void add_page(std::string url, uint32_t size) {
    pages.emplace_back(url, size);
  }

  indexer(
      std::vector<std::string> split_at,
      size_t htcap,
      size_t max_u,
      size_t max_p)
    : max_usage(max_u),
      file_buf_size(max_p),
      word_t(split_at, htcap),
      pair_t(split_at, htcap),
      trine_t(split_at, htcap)
  {
    file_buf = (uint8_t *) malloc(file_buf_size);
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
  index_info info;
  size_t htcap;

  index(std::string p, size_t cap) : info(p), htcap(cap) {}

  void load() {
    info.load();
  }

  void find_part_matches(index_part &p,
    std::string &term,
    std::vector<std::vector<std::pair<std::string, double>>> &postings);

  void find_matches(
    std::vector<index_part_info> &part_info,
    std::list<std::string> &terms,
    std::vector<std::vector<std::pair<std::string, double>>> &postings);

  std::vector<std::vector<std::pair<std::string, double>>>
    find_matches(char *line);
};

}

#endif

