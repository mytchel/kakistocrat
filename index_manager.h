#include <stdio.h>
#include <unistd.h>

#include <cstdlib>
#include <cstring>

#include <vector>
#include <list>
#include <set>
#include <unordered_set>
#include <map>
#include <string>
#include <algorithm>
#include <thread>
#include <future>
#include <optional>
#include <iostream>
#include <fstream>
#include <sstream>
#include <cstdint>

#include <nlohmann/json.hpp>

#include "index.h"

struct index_part {
  std::string path;
  std::vector<std::string> sites;
  bool merged{false};

  index_part() = default;

  index_part(const std::string &path,
             const std::vector<std::string> &sites)
    : path(path), sites(sites)
  {}
};

void to_json(nlohmann::json &j, const index_part &p);
void from_json(const nlohmann::json &j, index_part &p);

struct merge_part {
  const std::vector<std::string> *index_parts;

  search::index_type type;
  uint32_t part_index;

  merge_part() = default;

  merge_part(const std::vector<std::string> &parts,
             search::index_type t, uint32_t part_index)
    : index_parts(&parts), type(t), part_index(part_index) {}

  inline bool operator==(const merge_part &a)
  {
    return std::tie(a.index_parts, a.type, a.part_index)
      == std::tie(index_parts, type, part_index);
  }
};

void to_json(nlohmann::json &j, const merge_part &p);
void from_json(const nlohmann::json &j, merge_part &p);

class index_manager {
  std::string path;
  size_t sites_per_part;
  size_t index_splits;

  size_t next_part_id{0};

  std::list<index_part> index_parts;

  std::unordered_set<std::string> sites_pending_index;
  std::unordered_set<std::string> sites_indexing;

  std::vector<std::string> index_parts_merging;
  std::list<merge_part> merge_parts_pending;
  std::list<merge_part> merge_parts_merging;

  std::map<uint32_t, std::string>
    merge_out_w, merge_out_p, merge_out_t;

  std::string index_info;

  bool have_changes{false};

public:
  index_manager(const std::string &path, size_t m, size_t s, const std::string &i)
    : path(path), sites_per_part(m), index_splits(s), index_info(i) {}

  void load();
  void save();

  bool have_unmerged() {
    for (auto &p: index_parts) {
      if (!p.merged) {
        return true;
      }
    }

    return false;
  }

  bool need_index() {
    return !sites_pending_index.empty();
  }

  bool index_active() {
    return !sites_indexing.empty();
  }

  size_t get_next_part_id() {
    return next_part_id++;
  }

  bool need_merge_part() {
    return !merge_parts_pending.empty();
  }

  bool merge_part_active() {
    return !merge_parts_merging.empty();
  }

  bool need_start_merge() {
    return
      !index_active() &&
      !need_index() &&
      !need_merge_part() &&
      !merge_part_active() &&
      have_unmerged();
  }

  // indexing

  void mark_indexable(const std::string &site_path);
  void index_failed(const std::vector<std::string> &sites);

  std::vector<std::string> get_sites_for_index(bool flush);

  void add_part(const std::string &path, const std::vector<std::string> &sites);

  // merging

  void start_merge();

  merge_part& get_merge_part();

  // not really const, will delete m.
  void merge_part_done(merge_part &m, const std::string &output, bool ok);

private:
  void finish_merge();

  index_part * find_part(const std::string &path);

  std::vector<std::string> pop_parts(const std::string &site_path);
};


