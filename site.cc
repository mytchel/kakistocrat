#include <stdio.h>
#include <unistd.h>

#include <cstdlib>
#include <cstring>

#include <vector>
#include <list>
#include <set>
#include <string>
#include <algorithm>
#include <iostream>
#include <fstream>
#include <sstream>
#include <cstdint>
#include <ctime>

#include "spdlog/spdlog.h"

#include "util.h"
#include "site.h"

using nlohmann::json;

void to_json(json &j, const page &p) {
  j = json{
      {"u", p.url},
      {"p", p.path},
      {"t", p.title},
      {"aliases", p.aliases},
      {"links", p.links},
      {"s", p.last_scanned}};
}

void from_json(const json &j, page &p) {
  j.at("u").get_to(p.url);
  j.at("p").get_to(p.path);
  j.at("t").get_to(p.title);

  j.at("aliases").get_to(p.aliases);
  j.at("links").get_to(p.links);

  j.at("s").get_to(p.last_scanned);
}

void site_map::load() {
  if (loaded) return;
  loaded = true;

  std::ifstream file;

  spdlog::debug("load {}", path);

  file.open(path, std::ios::in);

  if (!file.is_open()) {
    //spdlog::warn("error opening file {}", path);
    return;
  }

  try {
    json j = json::parse(file);

    j.at("host").get_to(host);
    //j.at("last_scanned").get_to(last_scanned);
    j.at("pages").get_to(pages);

  } catch (const std::exception& e) {
    spdlog::warn("failed to load {}", path);
  }

  file.close();

  spdlog::debug("load {} finished {} with {} pages", path, host, pages.size());
}

void site_map::save() {
  spdlog::debug("save {}", path);

  std::ofstream file;

      //{"last_scanned", last_scanned},
  json j = {
      {"host", host},
      {"pages", pages}};

  file.open(path, std::ios::out | std::ios::trunc);

  if (!file.is_open()) {
    spdlog::warn("error opening file {}", path);
    return;
  }

  file << j;

  file.close();

  spdlog::debug("save {} finished", path);
}

page* site_map::find_page(const std::string &url)
{
  for (auto &p: pages) {
    if (p.url == url) {
      return &p;
    }

    for (auto &a: p.aliases) {
      if (a == url) {
        return &p;
      }
    }
  }

  return NULL;
}

page* site_map::find_page_by_path(const std::string &path)
{
  for (auto &p: pages) {
    if (p.path == path) {
      return &p;
    }
  }

  return NULL;
}

page* site_map::add_page(const std::string &url, const std::string &path)
{
  changed = true;
  return &pages.emplace_back(url, path);
}

