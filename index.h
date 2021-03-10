#ifndef INDEX_H
#define INDEX_H

#include <nlohmann/json.hpp>

#include <stdint.h>

#include "posting.h"
#include "hash_table.h"

namespace search {

struct indexer {
  std::map<uint64_t, size_t> page_lengths;
  hash_table words, pairs, trines;

  indexer() {}

  void save(std::string base_path);
};

const size_t ITCAP = (1 << 17);

enum index_type{words, pairs, trines};

struct index_part {
  index_type type;
  std::string path;

  std::string start;
  std::string end;

  uint8_t *backing;
  std::vector<std::pair<std::string, posting>> *store[ITCAP];

  index_part(index_type t, std::string p,
      std::string s, std::string e)
    : type(t), path(p), start(s), end(e)
  {
    for (size_t i = 0; i < ITCAP; i++) {
      store[i] = NULL;
    }
  }

  index_part(index_part &&p)
    : type(p.type), path(p.path),
    start(p.start), end(p.end)
  {
    backing = p.backing;

    for (size_t i = 0; i < ITCAP; i++) {
      store[i] = p.store[i];
    }

    p.backing = NULL;
  }

  ~index_part()
  {
    if (!backing) return;

    free(backing);

    for (size_t i = 0; i < ITCAP; i++) {
      if (store[i]) delete store[i];
    }
  }

  bool load_backing();
  void load();

  posting *find(std::string key);
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
  std::map<uint64_t, size_t> page_lengths;

  std::vector<index_part_info> word_parts;
  std::vector<index_part_info> pair_parts;
  std::vector<index_part_info> trine_parts;

  index_info() {}
};

void to_json(nlohmann::json &j, const index_info &i);
void from_json(const nlohmann::json &j, index_info &i);

struct index {
  size_t average_page_length;

  std::map<uint64_t, size_t> page_lengths;

  std::vector<index_part> word_parts;
  std::vector<index_part> pair_parts;
  std::vector<index_part> trine_parts;

  std::string path;

  index(std::string p) : path(p) {}

  void load();

  void find_part_matches(index_part &p,
    std::vector<std::string> &terms,
    std::vector<std::vector<std::pair<uint64_t, double>>> &postings);

  std::vector<std::vector<std::pair<uint64_t, double>>>
    find_matches(char *line);
};

}
#endif

