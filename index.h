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

namespace search {

std::vector<std::string> get_split_at();

const size_t max_index_part_size = 1024 * 1024 * 300;

std::list<std::string> load_parts(std::string path);
void save_parts(std::string path, std::list<std::string>);

struct indexer {
  std::vector<std::pair<uint64_t, uint32_t>> pages;
  hash_table words, pairs, trines;

  std::string save(std::string base_path);

  void clear() {
    pages.clear();
    words.clear();
    pairs.clear();
    trines.clear();
  }

  indexer() {}
};

struct key {
  const char *c;
  uint8_t len;

  key(const char *cc) :
    c(cc), len(strlen(cc)) {}

  key(const char *cc, uint8_t l) :
    c(cc), len(l) {}

  key(std::string const &s) {
    len = s.size();
    c = s.data();
  }

  key(key const &o) {
    len = o.len;
    c = o.c;
  }

  uint8_t size() {
    return len;
  }

  const char *data() {
    return c;
  }

  std::string str() {
    return std::string(c, c + len);
  }

  bool operator==(std::string &s) const {
    if (len != s.size()) return false;
    return memcmp(c, s.data(), len) == 0;
  }

  bool operator==(const key &o) const {
    if (len != o.len) return false;
    return memcmp(c, o.c, len) == 0;
  }

  bool operator<=(const std::string &s) const {
    uint8_t l = 0;;
    uint8_t ol = s.size();;
    const char *od = s.data();;

    while (l < len && l < ol) {
      if (c[l] < od[l]) {
        return true;
      } else if (c[l] > od[l]) {
        return false;
      } else {
        l++;
      }
    }

    return true;
  }

  bool operator>=(const std::string &s) const {
    uint8_t l = 0;;
    uint8_t ol = s.size();;
    const char *od = s.data();;

    while (l < len && l < ol) {
      if (c[l] > od[l]) {
        return true;
      } else if (c[l] < od[l]) {
        return false;
      } else {
        l++;
      }
    }

    return true;
  }

  bool operator<(const key &o) const {
    uint8_t l = 0;;
    while (l < len && l < o.len) {
      if (c[l] < o.c[l]) {
        return true;
      } else if (c[l] > o.c[l]) {
        return false;
      } else {
        l++;
      }
    }

    return l == len;
  }
};

enum index_type{words, pairs, trines};

struct index_part {
  index_type type;
  std::string path;

  std::optional<std::string> start;
  std::optional<std::string> end;

  uint8_t *backing{NULL};

  char *extra_backing{NULL};
  size_t extra_backing_offset{0};
  std::list<char *> extra_backing_list;

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
      start(p.start), end(p.end)
  {
    backing = p.backing;

    extra_backing = p.extra_backing;
    extra_backing_offset = p.extra_backing_offset;
    extra_backing_list = std::move(p.extra_backing_list);

    store = std::move(p.store);

    index = std::move(p.index);

    page_ids = std::move(p.page_ids);

    p.backing = NULL;
    p.extra_backing = NULL;
  }

  ~index_part()
  {
    if (backing) {
      free(backing);
    }

    if (extra_backing) {
      free(extra_backing);

      for (auto b: extra_backing_list) {
        free(b);
      }
    }
  }

  bool load_backing();
  void load();
  void save();
  size_t save_to_buf(uint8_t *buffer);

  void merge(index_part &other);

  void update_index(std::list<std::pair<key, posting>>::iterator);
  std::list<std::pair<key, posting>>::iterator find(key k);
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

