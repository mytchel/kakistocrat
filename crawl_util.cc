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

#include "util.h"
#include "crawl_util.h"

namespace crawl {

void index::save(std::string path)
{
  std::ofstream file;

  printf("save index %lu -> %s\n", sites.size(), path.c_str());

  file.open(path, std::ios::out | std::ios::trunc);

  if (!file.is_open()) {
    fprintf(stderr, "error opening file %s\n", path.c_str());
    return;
  }

  for (auto &site: sites) {
    bool has_pages = false;
    for (auto &p: site.pages) {
      if (!p.scraped) continue;
      has_pages = true;
      break;
    }

    if (!has_pages) continue;

    file << site.id << "\t";
    file << site.host << "\t";
    file << site.level << "\n";

    for (auto &p: site.pages) {
      if (!p.scraped) continue;

      file << "\t";
      file << p.id << "\t";
      file << p.url << "\t";
      file << p.path;

      for (auto &l: p.links) {
        file << "\t" << l.site << " " << l.page;
      }

      file << "\n";
    }
  }
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

  std::string line;
  while (getline(file, line)) {
    bool have_site = line[0] != '\t';

    std::istringstream ss(line);

    if (have_site) {
      uint32_t id;
      std::string host;
      size_t level;

      ss >> id;
      ss >> host;
      ss >> level;

      site s = {id, host, level};
      sites.push_back(s);

    } else {
      uint32_t id;
      std::string url;
      std::string path;

      std::string id_s, tmp;
      std::getline(ss, tmp, '\t');
      std::getline(ss, id_s, '\t');
      std::getline(ss, url, '\t');
      std::getline(ss, path, '\t');

      id = std::stoi(id_s);

      page p = {true, id, url, path};

      uint32_t ls, lp;
      while (ss >> ls && ss >> lp) {
        page_id id(ls, lp);
        p.links.push_back(id);
      }

      auto &site = sites.back();
      site.pages.push_back(std::move(p));
    }
  }

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
