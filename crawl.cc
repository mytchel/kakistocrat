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

#include <nlohmann/json.hpp>

#include "util.h"
#include "crawl.h"

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
      {"l", p.level},
      {"u", p.url},
      {"p", p.path},
      {"t", p.title},
      {"links", p.links},
      {"s", p.last_scanned},
      {"v", p.valid}};
}

void from_json(const json &j, page &p) {
  j.at("i").get_to(p.id);
  j.at("l").get_to(p.level);
  j.at("u").get_to(p.url);
  j.at("p").get_to(p.path);
  j.at("t").get_to(p.title);

  j.at("links").get_to(p.links);

  j.at("s").get_to(p.last_scanned);
  j.at("v").get_to(p.valid);
}

void site::load() {
  if (loaded) return;

  std::string path = host + ".index.json";
  std::ifstream file;

  file.open(path, std::ios::in);

  if (!file.is_open()) {
    fprintf(stderr, "error opening file %s\n", path.c_str());

    // So the file gets created
    loaded = true;
    return;
  }

  json j = json::parse(file);

  file.close();

  j.at("id").get_to(id);
  j.at("host").get_to(host);
  j.at("level").get_to(level);
  j.at("enabled").get_to(enabled);
  j.at("last_scanned").get_to(last_scanned);
  j.at("next_id").get_to(next_id);
  j.at("pages").get_to(pages);

  scraped = last_scanned > 0;
  loaded = true;
  scraping = false;
}

void site::save() {
  if (!loaded) return;

  std::string path = host + ".index.json";
  std::ofstream file;

  json j = {
      {"id", id},
      {"level", level},
      {"host", host},
      {"enabled", enabled},
      {"last_scanned", last_scanned},
      {"next_id", next_id},
      {"pages", pages}};

  file.open(path, std::ios::out | std::ios::trunc);

  if (!file.is_open()) {
    fprintf(stderr, "error opening file %s\n", path.c_str());
    return;
  }

  file << j;

  file.close();
}

void site::unload() {
  if (!loaded || scraping) return;

  save();

  loaded = false;
  pages.clear();
}

void index::save()
{
  std::string path = "full_index.json";
  std::ofstream file;

  std::vector<json> j_sites;

  for (auto &s: sites) {
    s.save();

    json j = {
      {"id", s.id},
      {"host", s.host},
      {"level", s.level},
      {"enabled", s.enabled},
      {"last_scanned", s.last_scanned}
    };

    j_sites.push_back(j);
  }

  json j = {
    {"next_id", next_id},
    {"sites", j_sites}
  };

  file.open(path, std::ios::out | std::ios::trunc);

  if (!file.is_open()) {
    fprintf(stderr, "error opening file %s\n", path.c_str());
    return;
  }

  file << j;

  file.close();
}

void index::load()
{
  std::string path = "full_index.json";
  std::ifstream file;

  file.open(path, std::ios::in);

  if (!file.is_open()) {
    fprintf(stderr, "error opening file %s\n", path.c_str());
    return;
  }

  json j = json::parse(file);

  file.close();

  j.at("next_id").get_to(next_id);

  sites.clear();

  for (auto &s_j: j.at("sites")) {
    sites.emplace_back(
          s_j.at("id").get<std::uint32_t>(),
          s_j.at("level").get<size_t>(),
          s_j.at("host").get<std::string>(),
          s_j.at("last_scanned").get<time_t>(),
          s_j.at("enabled").get<bool>());
  }
}

site * index::find_site(std::string host)
{
  for (auto &i: sites) {
    if (i.host == host) {
      return &i;
    }
  }

  return NULL;
}

site * index::find_site(uint32_t id)
{
  for (auto &i: sites) {
    if (i.id == id) {
      return &i;
    }
  }

  return NULL;
}

page* index::find_page(page_id id)
{
  auto s = find_site(id.site);
  if (s != NULL)
    return s->find_page(id.page);
  else
    return NULL;
}

page* index::find_page(uint64_t id)
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
