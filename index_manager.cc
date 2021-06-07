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

    std::vector<std::string> sites;

    sites.reserve(sites_pending_index.size() + sites_indexing.size());

    sites.insert(sites.end(),
          sites_pending_index.begin(), 
          sites_pending_index.end());

    sites.insert(sites.end(),
          sites_indexing.begin(), 
          sites_indexing.end());

    j.at("sites_pending_index").get_to(sites);

  } catch (const std::exception &e) {
    spdlog::warn("failed ot load {}", path);
  }

  file.close();
}

void index_manager::save() {
  spdlog::debug("save {}", path);

  json j = {
    { "parts", index_parts },
    { "next_part_id", next_part_id },
    { "sites_pending_index", sites_pending_index },
  };

  std::ofstream file;

  file.open(path, std::ios::out | std::ios::trunc);

  if (!file.is_open()) {
    spdlog::warn("error opening file {}", path);
    return;
  }

  file << j;

  file.close();

  have_changes = false;
}

std::vector<std::string> index_manager::get_parts_for_merge() {
  std::vector<std::string> parts;

  parts.reserve(index_parts.size());

  for (auto &p: index_parts) {
    parts.emplace_back(p.path);
  }

  return parts;
}

void index_manager::mark_merged(const std::vector<std::string> &parts) {
  for (auto &p: parts) {
    auto pp = find_part(p);
    if (pp == nullptr) {
      spdlog::error("merged unknown part {}", p);
      continue;
    }

    pp->merged = true;
  }
  
  have_changes = true;
}

void index_manager::mark_indexable(const std::string &site_path) {
  auto removed = pop_parts(site_path);

  for (auto ss: removed) {
    add_indexable(ss);
  }

  add_indexable(site_path);
  
  have_changes = true;
}

std::vector<std::string> index_manager::get_sites_for_index(bool flush) {
  if (!flush && sites_pending_index.size() * 100 < min_pages) {
    spdlog::debug("waiting on more sites, have {} sites", sites_pending_index.size());
    return {};
  }
  
  std::vector<std::string> sites(sites_pending_index.begin(), sites_pending_index.end());

  sites_indexing.insert(sites_pending_index.begin(), sites_pending_index.end());

  sites_pending_index.clear();

  have_changes = true;

  return sites;
}

void index_manager::add_part(const std::string &path, const std::vector<std::string> &sites) {
  for (auto &site_path: sites) {
    auto it = sites_indexing.find(site_path);
    if (it != sites_indexing.end()) {
      sites_indexing.erase(it);
    } else {
      spdlog::warn("add part with site that is not in sites indexing? {}", site_path);
    }

    if (sites_pending_index.find(site_path) != sites_pending_index.end()) {
      spdlog::error("add parts with site that is in pending index? {}", site_path);
    }

    for (auto &p: index_parts) {
      for (auto &ss: p.sites) {
        if (ss == site_path) {
          spdlog::error("add parts with site that is in index part {} : {}", p.path, site_path);
        }
      }
    }
  }

  index_parts.emplace_back(path, sites);

  have_changes = true;
}

void index_manager::add_indexable(const std::string &path) {
  for (auto s: sites_pending_index) {
    if (s == path) {
      return;
    }
  }

  sites_pending_index.emplace(path);
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

