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

#include "util.h"
#include "crawl.h"

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

  char t_buffer[32];

  for (auto &site: sites) {
    file << site.id << "\t";
    file << site.host << "\t";
    file << site.level << "\t";

    std::tm * stm = std::gmtime(&site.last_scanned);
    std::strftime(t_buffer, 32, "%Y-%m-%d %H:%M:%S", stm);

    file << t_buffer;

    file << "\n";

    for (auto &p: site.pages) {

      std::tm * ptm = std::gmtime(&p.last_scanned);
      std::strftime(t_buffer, 32, "%Y-%m-%d %H:%M:%S", ptm);

      file << "\t";
      file << p.id << "\t";
      file << p.url << "\t";
      file << p.path << "\t";
      file << t_buffer << "\t";
      file << p.valid << "\t";
      file << p.scraped;

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
      std::string scraped_s;

      ss >> id;
      ss >> host;
      ss >> level;
      ss >> scraped_s;

      tm tm;
      strptime(scraped_s.c_str(), "%Y-%m-%d %H:%M:%S", &tm);
      time_t time = mktime(&tm);

      sites.emplace_back(id, host, level, time);

    } else {
      std::string tmp;
      std::string id_s;
      std::string url;
      std::string path;
      std::string time_s;
      std::string valid_s;
      std::string scraped_s;

      std::getline(ss, tmp, '\t');
      std::getline(ss, id_s, '\t');
      std::getline(ss, url, '\t');
      std::getline(ss, path, '\t');
      std::getline(ss, time_s, '\t');
      std::getline(ss, valid_s, '\t');
      std::getline(ss, scraped_s, '\t');

      uint32_t id = std::stoi(id_s);

      tm tm;
      strptime(time_s.c_str(), "%Y-%m-%d %H:%M:%S", &tm);
      time_t time = mktime(&tm);

      bool valid = valid_s == "1";
      bool scraped = scraped_s == "1";

      std::vector<page_id> links;

      uint32_t ls, lp;
      while (ss >> ls && ss >> lp) {
        links.emplace_back(ls, lp);
      }

      auto &site = sites.back();
      site.pages.emplace_back(id, url, path, time, valid, scraped, links);
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
