#include <stdio.h>
#include <unistd.h>

#include <cstdlib>
#include <cstring>

#include <vector>
#include <list>
#include <map>
#include <set>
#include <string>
#include <algorithm>
#include <iostream>
#include <fstream>
#include <sstream>
#include <cstdint>

#include <nlohmann/json.hpp>
#include "spdlog/spdlog.h"

#include "util.h"
#include "scrape.h"
#include "crawl.h"
#include "scorer.h"

using nlohmann::json;

namespace scorer {

void to_json(json &j, const page &p) {
  j = json{
      {"i", p.id},
      {"l", p.level},
      {"s", p.score},
      {"u", p.url},
      {"p", p.path},
      {"t", p.title}};
}

void from_json(const json &j, page &p) {
  j.at("i").get_to(p.id);
  j.at("l").get_to(p.level);
  j.at("s").get_to(p.score);
  j.at("u").get_to(p.url);
  j.at("p").get_to(p.path);
  j.at("t").get_to(p.title);
}

void scores::iteration()
{
  std::map<uint64_t, double> new_scores;

  for (auto &i: pages) {
    auto &p = i.second;

    crawl::page_id page_id(p.id);

    size_t link_count = 0;
    for (auto &l: p.links) {
      link_count += l.second;
    }

    double link_score = p.score / (double) link_count;
    p.score = 0;

    for (auto &l: p.links) {
      crawl::page_id id(l.first);
      double score = link_score * l.second;
      if (page_id.site == id.site) {
        score *= 0.9;
      }

      auto i = new_scores.try_emplace(l.first, score);
      if (!i.second) {
        i.first->second += score;
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

  spdlog::info("total score = {}", sum);

  for (auto &i: pages) {
    auto &p = i.second;
    p.score /= sum;
  }
}

scores::scores(std::string p, std::list<crawl::site> &sites)
  : path(p)
{
  pages.clear();
  size_t n_pages = 0;

  for (auto &s: sites) {
    if (s.last_scanned == 0) {
      continue;
    }

    s.load();

    for (auto &p: s.pages) {
      if (p.last_scanned > 0) {
        n_pages++;
      }
    }
  }

  spdlog::info("init from scrape with {} pages", n_pages);

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

  for (auto &s: sites) {
    for (auto &p: s.pages) {
      if (p.last_scanned == 0) continue;

      crawl::page_id id(s.id, p.id);

      auto it = pages.emplace(std::piecewise_construct,
                  std::forward_as_tuple(id.to_value()),
                  std::forward_as_tuple(id.to_value(),
                      s.level, score, p.url, p.path, p.title));

      for (auto &l: p.links) {
        it.first->second.links.emplace_back(l.first.to_value(), l.second);
      }
    }

    s.flush();
  }

  for (auto &i: pages) {
    auto &p = i.second;

    crawl::page_id page_id(p.id);

    std::vector<std::pair<uint64_t, size_t>> fixed_links;

    for (auto &l: p.links) {
      auto p = find_page(l.first);
      if (p != NULL) {
        fixed_links.emplace_back(l);
      }
    }

    p.links = fixed_links;
  }

  double sum = 0;

  for (auto &i: pages) {
    auto &p = i.second;
    sum += p.score;
  }

  spdlog::debug("total score = {}", sum);
}

void scores::save()
{
  std::ofstream file;

  spdlog::info("save scores {} -> {}", pages.size(), path);

  file.open(path, std::ios::out | std::ios::trunc);

  if (!file.is_open()) {
    spdlog::warn("error opening file {}", path);
    return;
  }

  json j = {{"pages", pages}};

  file << j;

  file.close();
}

void scores::load()
{
  std::ifstream file;

  spdlog::info("load {}", path);

  file.open(path, std::ios::in);

  if (!file.is_open()) {
    spdlog::warn("error opening file {}", path);
    return;
  }

  pages.clear();

  json j = json::parse(file);

  j.at("pages").get_to(pages);

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

page* scores::find_page(const std::string &url)
{
  for (auto &p: pages) {
    if (p.second.url == url) {
      return &p.second;
    }
  }

  return NULL;
}

}

