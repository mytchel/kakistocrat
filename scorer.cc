#include <stdio.h>
#include <unistd.h>

#include <cstdlib>
#include <cstring>

#include <vector>
#include <list>
#include <map>
#include <string>
#include <algorithm>
#include <iostream>
#include <fstream>
#include <sstream>
#include <cstdint>

#include "util.h"
#include "crawl_util.h"
#include "scorer.h"

namespace scorer {

void scores::iteration()
{
  std::map<uint64_t, double> new_scores;

  for (auto &i: pages) {
    auto &p = i.second;

    crawl::page_id page_id(p.id);

    double link_score = p.score / (double) p.links.size();

    p.score = 0;

    for (auto &l: p.links) {
      crawl::page_id id(l);
      double score = link_score;
      if (page_id.site == id.site) {
        score *= 0.9;
      }

      auto i = new_scores.find(l);
      if (i == new_scores.end()) {
        new_scores.emplace(l, score);
      } else {
        i->second += score;
      }
    }
  }

  double sum = 0;

  for (auto &i: pages) {
    auto &p = i.second;
    auto score = new_scores.find(p.id);
    if (score != new_scores.end()) {
      p.score += score->second;
    }

    p.score /= (1 + p.level);

    sum += p.score;
  }

  printf("total score = %f\n", sum);

  for (auto &i: pages) {
    auto &p = i.second;
    p.score /= sum;
  }
}

void scores::init(crawl::index &index)
{
  pages.clear();
  n_pages = 0;

  for (auto &s: index.sites) {
    for (auto &p: s.pages) {
      if (p.scraped) {
        n_pages++;
      }
    }
  }

  printf("init from scrape with %lu pages\n", n_pages);

  /*
   * TODO:
   *    fancy level biasing.
   *
  double r = 2.0/3.0;
  if (level == level_counts.size() - 1) {
    r = 1.0;
  }

  double c = 1.0;
  for (size_t l = 0; l < level; l++) {
    c *= 1.0/3.0;
  }

  double base = c * r / n;


  for (size_t i = 0; i < max_level; i++) {
    level_scores.push_back(0);
  }

  auto &l = level_counts.at(s.level);
*/

  double score = 1.0 / (double) n_pages;

  for (auto &s: index.sites) {
    for (auto &p: s.pages) {
      if (!p.scraped) continue;

      crawl::page_id id(s.id, p.id);

      std::vector<uint64_t> links;

      for (auto &l: p.links) {
        links.push_back(l.to_value());
      }

      page n(id.to_value(), s.level, score, p.url, p.path, links);

      pages.emplace(id.to_value(), n);
    }
  }

  for (auto &i: pages) {
    auto &p = i.second;

    crawl::page_id page_id(p.id);

    std::vector<uint64_t> fixed_links;

    for (auto &l: p.links) {
      auto p = find_page(l);
      if (p != NULL) {
        fixed_links.push_back(l);
      }
    }

    p.links = fixed_links;
  }

  double sum = 0;

  for (auto &i: pages) {
    auto &p = i.second;
    sum += p.score;
  }

  printf("total score = %f\n", sum);
}

void scores::save(std::string path)
{
  std::ofstream file;

  printf("save scores %lu -> %s\n", pages.size(), path.c_str());

  file.open(path, std::ios::out | std::ios::trunc);

  if (!file.is_open()) {
    fprintf(stderr, "error opening file %s\n", path.c_str());
    return;
  }

  for (auto &i: pages) {
    auto &p = i.second;

    file << p.id << "\t";
    file << p.level << "\t";
    file << p.score << "\t";
    file << p.url << "\t";
    file << p.path;

    for (auto &l: p.links) {
      file << "\t" << l;
    }

    file << "\n";
  }

  file.close();
}

void scores::load(std::string path)
{
  std::ifstream file;

  printf("load %s\n", path.c_str());

  file.open(path, std::ios::in);

  if (!file.is_open()) {
    fprintf(stderr, "error opening file %s\n", path.c_str());
    return;
  }

  pages.clear();

  std::string line;
  while (getline(file, line)) {
    std::istringstream ss(line);

    uint64_t id, level;
    double score;
    std::string url;
    std::string path;

    std::string id_s, level_s, score_s;
    std::getline(ss, id_s, '\t');
    std::getline(ss, level_s, '\t');
    std::getline(ss, score_s, '\t');
    std::getline(ss, url, '\t');
    std::getline(ss, path, '\t');

    id = std::stoull(id_s);
    level = std::stoul(level_s);
    score = std::stof(score_s);

    std::vector<uint64_t> links;

    uint64_t l;
    while (ss >> l) {
      links.push_back(l);
    }

    page n(id, level, score, url, path, links);
    pages.emplace(id, n);
  }

  file.close();
}

page* scores::find_page(uint64_t id)
{
  auto i = pages.find(id);
  if (i != pages.end()) {
    return &i->second;
  } else {
    return NULL;
  }
}

}

