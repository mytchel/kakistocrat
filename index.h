#ifndef INDEX_H
#define INDEX_H

#include <nlohmann/json.hpp>

#include <stdint.h>

#include <list>
#include <string>
#include <vector>
#include <optional>

#include <chrono>
using namespace std::chrono_literals;

#include "posting.h"
#include "hash_table.h"
#include "key.h"
#include "buf_list.h"

namespace search {

std::vector<std::string> get_split_at();

enum index_type{words, pairs, trines};

const size_t max_index_part_size = 1024 * 1024 * 10;

std::list<std::string> load_parts(std::string path);
void save_parts(std::string path, std::list<std::string>);

struct indexer {
  std::vector<std::pair<uint64_t, uint32_t>> pages;
  hash_table word_t, pair_t, trine_t;
  size_t usage{0};
  
  std::string base_path;
  size_t flush_count{0};
  std::list<std::string> paths;

  std::string save();

  void clear() {
    pages.clear();
    word_t.clear();
    pair_t.clear();
    trine_t.clear();
    usage = 0;
  }

  void flush() {
    paths.emplace_back(save());

    clear();

    flush_count++;
  }

  void insert(index_type t, std::string s, uint32_t index_id) {
    if (t == words) {
      usage += word_t.insert(s, index_id);
    } else if (t == pairs) {
      usage += pair_t.insert(s, index_id);
    } else if (t == trines) {
      usage += trine_t.insert(s, index_id);
    }
  }

  uint32_t next_id() {
    return pages.size();
  }

  void add_page(uint64_t page_id, size_t size) {
    pages.emplace_back(page_id, size);
  }

  indexer(std::string p) : base_path(p) {}
};

struct index_part {
  index_type type;
  std::string path;

  std::optional<std::string> start;
  std::optional<std::string> end;

  uint8_t *backing{NULL};

  buf_list extra_backing;

  std::list<std::pair<key, posting>> store;

  std::vector<std::vector<
      std::pair<uint8_t, std::list<std::pair<key, posting>>::iterator>
    >> index;

  std::vector<uint64_t> page_ids;

  std::chrono::nanoseconds index_total{0ms};
  std::chrono::nanoseconds merge_total{0ms};
  std::chrono::nanoseconds find_total{0ms};

  index_part(index_type t, std::string p,
      std::optional<std::string> s,
      std::optional<std::string> e)
    : index(HTCAP),
      type(t), path(p), start(s), end(e) {}

  index_part(index_part &&p)
    : type(p.type), path(p.path),
      start(p.start), end(p.end),
      extra_backing(std::move(p.extra_backing)),
      store(std::move(p.store)),
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

  bool load_backing();
  void load();
  void save();
  size_t save_to_buf(uint8_t *buffer, size_t len);

  void merge(index_part &other);

  void update_index(std::list<std::pair<key, posting>>::iterator);
  std::list<std::pair<key, posting>>::iterator find(std::string);
  std::list<std::pair<key, posting>>::iterator find(key);
};

struct index_part_info {
  std::string path;

  std::optional<std::string> start;
  std::optional<std::string> end;

  index_part_info() {}

  index_part_info(std::string p) : path(p) {}

  index_part_info(std::string p,
      std::optional<std::string> s,
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
    std::vector<std::string> &terms,
    std::vector<std::vector<std::pair<uint64_t, double>>> &postings);

  std::vector<std::vector<std::pair<uint64_t, double>>>
    find_matches(char *line);
};

}
#endif

