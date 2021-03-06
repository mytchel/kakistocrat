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
      {"u", p.url},
      {"p", p.path},
      {"t", p.title},
      {"links", p.links},
      {"l", p.last_scanned},
      {"v", p.valid},
      {"s", p.scraped}};
}

void from_json(const json &j, page &p) {
  j.at("i").get_to(p.id);
  j.at("u").get_to(p.url);
  j.at("p").get_to(p.path);
  j.at("t").get_to(p.title);

  j.at("links").get_to(p.links);

  j.at("l").get_to(p.last_scanned);
  j.at("v").get_to(p.valid);
  j.at("s").get_to(p.scraped);
}

void to_json(json &j, const site &s) {
  j = json{
      {"id", s.id},
      {"host", s.host},
      {"level", s.level},
      {"last_scanned", s.last_scanned},
      {"next_id", s.next_id},
      {"pages", s.pages}};
}

void from_json(const json &j, site &s) {
  j.at("id").get_to(s.id);
  j.at("host").get_to(s.host);
  j.at("level").get_to(s.level);
  j.at("last_scanned").get_to(s.last_scanned);
  j.at("next_id").get_to(s.next_id);
  j.at("pages").get_to(s.pages);
}

void index::save(std::string path)
{
  std::ofstream file;

  printf("save index %lu -> %s\n", sites.size(), path.c_str());

  file.open(path, std::ios::out | std::ios::trunc);

  if (!file.is_open()) {
    fprintf(stderr, "error opening file %s\n", path.c_str());
    return;
  }

  json j = {
    {"next_id", next_id},
    {"sites", sites}};

  file << j;

  printf("saved\n");
  file.close();
}

void index::load(std::string path)
{
  std::ifstream file;

  printf("load %s\n", path.c_str());

  file.open(path, std::ios::in);

  if (!file.is_open()) {
    fprintf(stderr, "error opening file %s\n", path.c_str());
    return;
  }

  json j = json::parse(file);

  j.at("next_id").get_to(next_id);
  j.at("sites").get_to(sites);

  file.close();
}

site * index::find_host(std::string host)
{
  for (auto &i: sites) {
    if (i.host == host) {
      return &i;
    }
  }

  return NULL;
}

page* index::find_page(page_id id)
{
  for (auto &s: sites) {
    if (s.id != id.site) continue;
    return s.find_page(id.page);
  }

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
