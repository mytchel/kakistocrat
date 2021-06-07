#include <stdio.h>
#include <unistd.h>

#include <cstdlib>
#include <cstring>

#include <vector>
#include <list>
#include <set>
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

class index_manager {
  std::string path;
  size_t min_pages;

  size_t next_part_id{0};

  std::list<index_part> index_parts;

  std::set<std::string> sites_pending_index;
  std::set<std::string> sites_indexing;

  bool have_changes{false};

public:
  index_manager(const std::string &path, size_t m) 
    : path(path), min_pages(m) {}

  void load();
  void save();
  
  bool has_changes() {
    return have_changes;
  }

  bool need_merge() {
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

  std::vector<std::string> get_parts_for_merge();
  
  void mark_merged(const std::vector<std::string> &parts);
  
  void mark_indexable(const std::string &site_path);
  
  std::vector<std::string> get_sites_for_index(bool flush);
  
  void add_part(const std::string &path, const std::vector<std::string> &sites);

private:
  void add_indexable(const std::string &path);
  
  index_part * find_part(const std::string &path);
  
  std::vector<std::string> pop_parts(const std::string &site_path);
};

