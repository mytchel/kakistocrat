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
#include "crawl.h"
#include "config.h"

using nlohmann::json;

namespace crawl {

void to_json(json &j, const page_id &id) {
  j = json{{"s", id.site}, {"p", id.page}};
}

void from_json(const json &j, page_id &id) {
  j.at("s").get_to(id.site);
  j.at("p").get_to(id.page);
}

void to_json(json &j, const page &p) {
  j = json{
      {"i", p.id},
      {"u", p.url},
      {"p", p.path},
      {"t", p.title},
      {"links", p.links},
      {"s", p.last_scanned}};
}

void from_json(const json &j, page &p) {
  j.at("i").get_to(p.id);
  j.at("u").get_to(p.url);
  j.at("p").get_to(p.path);
  j.at("t").get_to(p.title);

  j.at("links").get_to(p.links);

  j.at("s").get_to(p.last_scanned);
}

void site::load() {
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

    j.at("id").get_to(id);
    j.at("host").get_to(host);
    j.at("last_scanned").get_to(last_scanned);
    j.at("next_id").get_to(next_id);
    j.at("pages").get_to(pages);

  } catch (const std::exception& e) {
    spdlog::warn("failed to load {}", path);
  }

  file.close();

  spdlog::debug("load {} finished", path);
}

void site::save() {
  spdlog::debug("save {}", path);

  std::ofstream file;

  json j = {
      {"id", id},
      {"host", host},
      {"last_scanned", last_scanned},
      {"next_id", next_id},
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

void crawler::save()
{
  spdlog::debug("save {}", sites_path);

  std::ofstream file;

  std::vector<json> j_sites;

  for (auto &s: sites) {
    json j = {
      {"path", s.path},
      {"id", s.id},
      {"host", s.host},
      {"level", s.level},
      {"max_pages", s.max_pages},
      {"last_scanned", s.last_scanned}
    };

    j_sites.push_back(j);
  }

  json j = {
    {"next_id", next_id},
    {"sites", j_sites}
  };

  file.open(sites_path, std::ios::out | std::ios::trunc);

  if (!file.is_open()) {
    spdlog::warn("error opening file {}", sites_path);
    return;
  }

  file << j;

  file.close();

  spdlog::debug("save {} finished", sites_path);
}

void crawler::load()
{
  spdlog::debug("load {}", sites_path);

  std::ifstream file;

  file.open(sites_path, std::ios::in);

  if (!file.is_open()) {
    spdlog::warn("error opening file {}", sites_path);
    return;
  }

  sites.clear();

  try {
    json j = json::parse(file);

    j.at("next_id").get_to(next_id);

    for (auto &s_j: j.at("sites")) {
      sites.emplace_back(
            s_j.at("path").get<std::string>(),
            s_j.at("id").get<std::uint32_t>(),
            s_j.at("host").get<std::string>(),
            s_j.at("level").get<size_t>(),
            s_j.at("max_pages").get<size_t>(),
            s_j.at("last_scanned").get<time_t>());
    }

  } catch (const std::exception& e) {
    spdlog::warn("failed to load {}", sites_path);
  }

  file.close();

  spdlog::debug("load {} finished", sites_path);
}

site * crawler::find_site(std::string host)
{
  for (auto &i: sites) {
    if (i.host == host) {
      return &i;
    }
  }

  return NULL;
}

site * crawler::find_site(uint32_t id)
{
  for (auto &i: sites) {
    if (i.id == id) {
      return &i;
    }
  }

  return NULL;
}

page* crawler::find_page(page_id id)
{
  auto s = find_site(id.site);
  if (s != NULL)
    return s->find_page(id.page);
  else
    return NULL;
}

page* crawler::find_page(uint64_t id)
{
  return find_page(page_id(id));
}

page* site::find_page(std::string url)
{
  for (auto &p: pages) {
    if (p.url == url) {
      return &p;
    }
  }

  return NULL;
}

page* site::find_page_by_path(std::string path)
{
  for (auto &p: pages) {
    if (p.path == path) {
      return &p;
    }
  }

  return NULL;
}

page* site::find_page(uint32_t id)
{
  for (auto &p: pages) {
    if (p.id == id) {
      return &p;
    }
  }

  return NULL;
}

}
