#ifndef INDEX_H
#define INDEX_H

#include <nlohmann/json.hpp>

#include <stdint.h>

#include <list>
#include <string>
#include <vector>

#include "posting.h"
#include "hash_table.h"

namespace search {

struct indexer {
  std::map<uint64_t, size_t> page_lengths;
  hash_table words, pairs, trines;

  indexer() {}

  void save(std::string base_path);
};

struct key {
  char *c_str_buf{NULL};
  const char *c;
  size_t len;
  bool owner;

  key(const char *cc) :
    c(cc), len(strlen(cc)), owner(false) {}

  key(const char *cc, size_t l) :
    c(cc), len(l), owner(false) {}

  key(std::string const &s) {
    len = s.size();

    char *ccc = (char *) malloc(len);
    memcpy(ccc, s.c_str(), len);

    c = ccc;
    owner = true;
  }

  key(key const &o) {
    len = o.len;

    char *ccc = (char *) malloc(len);
    memcpy(ccc, o.c, len);

    c = ccc;
    owner = true;
  }

/*
  key(key &&o) {
    len = o.len;
    c = o.c;
    owner = o.owner;
    o.owner = false;
  }
*/

  ~key() {
    if (owner) {
      free((void *) c);
    }

    if (c_str_buf) {
      free((void *) c_str_buf);
    }
  }

  size_t size() {
    return len;
  }

  const char *data() {
    return c;
  }

  const char *c_str() {
    if (c_str_buf == NULL) {
      c_str_buf = (char *) malloc(len + 1);
      memcpy(c_str_buf, c, len);
      c_str_buf[len] = 0;
    }

    return c_str_buf;
  }

  bool operator==(std::string &s) const {
    if (len != s.size()) return false;
    return memcmp(c, s.data(), len) == 0;
  }

  bool operator==(const key &o) const {
    if (len != o.len) return false;
    return memcmp(c, o.c, len) == 0;
  }

  bool operator<(const key &o) const {
    size_t l = 0;;
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

  std::string start;
  std::string end;

  uint8_t *backing{NULL};

  std::list<std::pair<key, posting>> store;

  std::vector<
    std::list<std::pair<key, posting>>::iterator
    > *index[HTCAP]{NULL};

  index_part(index_type t, std::string p,
      std::string s, std::string e)
    : type(t), path(p), start(s), end(e) {}

  index_part(index_part &&p)
    : type(p.type), path(p.path),
    start(p.start), end(p.end)
  {
    backing = p.backing;

    store = std::move(p.store);

    for (size_t i = 0; i < HTCAP; i++) {
      index[i] = p.index[i];
      p.index[i] = NULL;
    }

    p.backing = NULL;
  }

  ~index_part()
  {
    if (backing) {
      free(backing);
    }

    for (size_t i = 0; i < HTCAP; i++) {
      if (index[i]) {
        delete index[i];
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

  std::string start;
  std::string end;

  index_part_info() {}

  index_part_info(std::string p) : path(p) {}

  index_part_info(std::string p,
      std::string s, std::string e) :
    path(p), start(s), end(e) {}
};

void to_json(nlohmann::json &j, const index_part_info &i);
void from_json(const nlohmann::json &j, index_part_info &i);

struct index_info {
  std::string path;
  size_t average_page_length;
  std::map<uint64_t, size_t> page_lengths;

  std::vector<index_part_info> word_parts;
  std::vector<index_part_info> pair_parts;
  std::vector<index_part_info> trine_parts;

  index_info(std::string p) : path(p) {}

  void load();
  void save();
};

struct index {
  size_t average_page_length{0};

  std::map<uint64_t, size_t> page_lengths;

  std::vector<index_part> word_parts;
  std::vector<index_part> pair_parts;
  std::vector<index_part> trine_parts;

  std::string base_path;

  index(std::string p) : base_path(p) {}

  void load();
  void save();

  void merge(index &other);

  void find_part_matches(index_part &p,
    std::vector<std::string> &terms,
    std::vector<std::vector<std::pair<uint64_t, double>>> &postings);

  std::vector<std::vector<std::pair<uint64_t, double>>>
    find_matches(char *line);
};

}
#endif

