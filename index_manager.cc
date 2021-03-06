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
#include "spdlog/spdlog.h"

#include "index_manager.h"

using nlohmann::json;

void to_json(nlohmann::json &j, const index_part &p)
{
  j["path"] = p.path;
  j["sites"] = p.sites;
  j["merged"] = p.merged;
}

void from_json(const nlohmann::json &j, index_part &p)
{
  p.path = j.at("path");
  j.at("sites").get_to(p.sites);
  p.merged = j.at("merged");
}

void to_json(nlohmann::json &j, const merge_part &p)
{
  j["type"] = search::to_str(p.type);
  j["index"] = p.part_index;
}

void from_json(const nlohmann::json &j, merge_part &p)
{
  p.type = search::from_str(j.at("type"));
  p.part_index = j.at("start");
}

void index_manager::load() {
  spdlog::debug("load {}", path);

  std::ifstream file;

  file.open(path, std::ios::in);

  if (!file.is_open()) {
    spdlog::warn("error opening file {}", path);
    have_changes = true;
    return;
  }

  try {
    json j = json::parse(file);

    j.at("parts").get_to(index_parts);
    j.at("next_part_id").get_to(next_part_id);

    j.at("sites_pending_index").get_to(sites_pending_index);

    try {
      j.at("index_parts_merging").get_to(index_parts_merging);
      j.at("merge_parts_pending").get_to(merge_parts_pending);

      j.at("merge_out_w").get_to(merge_out_w);
      j.at("merge_out_p").get_to(merge_out_p);
      j.at("merge_out_t").get_to(merge_out_t);

    } catch (const std::exception &e) {
      spdlog::warn("failed to parse new stuff {}", path);

      for (auto &p: index_parts) {
        for (auto &s: p.sites) {
          sites_pending_index.emplace(s);
        }
      }

      index_parts.clear();
    }

    for (auto &m: merge_parts_pending) {
      m.index_parts = &index_parts_merging;
    }

  } catch (const std::exception &e) {
    spdlog::warn("failed to load {}", path);
  }

  file.close();
}

void index_manager::save() {
  if (!have_changes) {
    return;
  }

  spdlog::debug("save {}", path);

  std::vector<std::string> sites;

  sites.reserve(sites_pending_index.size() + sites_indexing.size());

  sites.insert(sites.end(),
        sites_pending_index.begin(),
        sites_pending_index.end());

  sites.insert(sites.end(),
        sites_indexing.begin(),
        sites_indexing.end());

  std::vector<merge_part> merge_parts;

  merge_parts.insert(merge_parts.end(), merge_parts_pending.begin(), merge_parts_pending.end());
  merge_parts.insert(merge_parts.end(), merge_parts_merging.begin(), merge_parts_merging.end());

  json j = {
    { "parts", index_parts },
    { "next_part_id", next_part_id },
    { "sites_pending_index", sites },
    { "merge_parts_pending", merge_parts },
    { "index_parts_merging", index_parts_merging },
    { "merge_out_w", merge_out_w },
    { "merge_out_p", merge_out_p },
    { "merge_out_t", merge_out_t },
  };

  std::ofstream file;

  file.open(path, std::ios::out | std::ios::trunc);

  if (!file.is_open()) {
    spdlog::warn("error opening file {}", path);
    throw std::runtime_error(fmt::format("error writing to file {}", path));
  }

  file << j;

  file.close();

  have_changes = false;
}

void index_manager::mark_indexable(const std::string &site_path) {
  auto removed = pop_parts(site_path);

  for (auto ss: removed) {
    sites_pending_index.emplace(ss);
  }

  sites_pending_index.emplace(site_path);

  have_changes = true;
}

void index_manager::index_failed(const std::vector<std::string> &sites) {
  for (auto &site: sites) {
    auto it = sites_indexing.find(site);
    if (it != sites_indexing.end()) {
      sites_indexing.erase(it);
    }

    sites_pending_index.emplace(site);
  }

  have_changes = true;
}

std::vector<std::string> index_manager::get_sites_for_index(bool flush) {
  spdlog::info("get sites for index");

  if (!flush && sites_pending_index.size() < sites_per_part) {
    spdlog::debug("waiting on more sites, have {} sites", sites_pending_index.size());
    return {};
  }

  std::vector<std::string> sites;

  auto it = sites_pending_index.begin();
  while (it != sites_pending_index.end()) {
    sites.emplace_back(*it);
    it = sites_pending_index.erase(it);

    if (sites.size() > sites_per_part * 3) {
      break;
    }
  }

  sites_indexing.insert(sites.begin(), sites.end());

  have_changes = true;

  return sites;
}

void index_manager::add_part(const std::string &path, const std::vector<std::string> &sites) {
  for (auto &site_path: sites) {
    auto it = sites_indexing.find(site_path);
    if (it != sites_indexing.end()) {
      sites_indexing.erase(it);
    } else {
      // Could happen due to a site spanning parts.
      spdlog::warn("add part with site that is not in sites indexing? {}", site_path);
    }

    if (sites_pending_index.find(site_path) != sites_pending_index.end()) {
      spdlog::error("add parts with site that is in pending index? {}", site_path);
    }
  }

  index_parts.emplace_back(path, sites);

  have_changes = true;
}

void index_manager::start_merge() {
  assert(index_parts_merging.empty());

  index_parts_merging.clear();
  for (auto &part: index_parts) {
    index_parts_merging.emplace_back(part.path);
  }

  for (size_t i = 0; i < index_splits; i++) {
    merge_parts_pending.emplace_back(index_parts_merging,
        search::index_type::words, i);
    merge_parts_pending.emplace_back(index_parts_merging,
        search::index_type::pairs, i);
    merge_parts_pending.emplace_back(index_parts_merging,
        search::index_type::trines, i);
  }

  have_changes = true;
}

merge_part& index_manager::get_merge_part() {
  assert(!merge_parts_pending.empty());

  auto m = merge_parts_pending.front();
  merge_parts_pending.pop_front();

  have_changes = true;

  return merge_parts_merging.emplace_back(m);
}

void index_manager::merge_part_done(merge_part &m, const std::string &output, bool ok) {
  assert(!merge_parts_merging.empty());

  if (ok) {
    switch (m.type) {
      case search::index_type::words:
        merge_out_w.emplace(m.part_index, output);
        break;
      case search::index_type::pairs:
        merge_out_p.emplace(m.part_index, output);
        break;
      case search::index_type::trines:
        merge_out_t.emplace(m.part_index, output);
        break;
    }

  } else {
    merge_parts_pending.emplace_back(m);
  }

  merge_parts_merging.remove_if(
    [&m] (auto &n) {
      return &n == &m;
    });

  if (merge_parts_pending.empty() && merge_parts_merging.empty()) {
    finish_merge();
  }

  have_changes = true;
}

void index_manager::finish_merge() {
  search::index_info info(index_info);

  info.average_page_length = 0;

  for (auto &path: index_parts_merging) {
    search::index_info index(path);
    index.load();

    if (index.parts != index_splits) {
      spdlog::warn("part {} has bad split parts {} != {}",
          path, index.parts, index_splits);
    }

    for (auto &p: index.pages) {
      info.average_page_length += p.second;
      info.pages.emplace_back(p.first, p.second);
    }

    spdlog::info("{} added {} / {} pages", path, index.pages.size(), info.pages.size());
  }

  if (info.pages.size() > 0) {
    info.average_page_length /= info.pages.size();
  } else {
    info.average_page_length = 0;
  }

  info.word_parts = merge_out_w;
  info.pair_parts = merge_out_p;
  info.trine_parts = merge_out_t;

  info.parts = index_splits;

  info.save();

  merge_out_w.clear();
  merge_out_p.clear();
  merge_out_t.clear();

  for (auto &path: index_parts_merging) {
    auto i = find_part(path);
    if (i != nullptr) {
      i->merged = true;
    }
  }

  index_parts_merging.clear();
}

index_part * index_manager::find_part(const std::string &path) {
  for (auto &p: index_parts) {
    if (p.path == path) {
      return &p;
    }
  }

  return nullptr;
}

std::vector<std::string> index_manager::pop_parts(const std::string &site_path) {
  std::vector<std::string> removed_sites;

  auto part = index_parts.begin();

  while (part != index_parts.end()) {
    bool site_in_part = false;

    for (auto s: part->sites) {
      if (s == site_path) {
        site_in_part = true;
        break;
      }
    }

    if (site_in_part) {
      removed_sites.insert(removed_sites.end(), part->sites.begin(), part->sites.end());

      part = index_parts.erase(part);
    } else {
      part++;
    }
  }

  return removed_sites;
}

