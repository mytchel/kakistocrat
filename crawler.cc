#include <stdio.h>
#include <unistd.h>

#include <cstdlib>
#include <cstring>

#include <vector>
#include <set>
#include <map>
#include <string>
#include <algorithm>
#include <future>
#include <optional>
#include <iostream>
#include <fstream>
#include <sstream>
#include <cstdint>
#include <optional>
#include <chrono>

#include <nlohmann/json.hpp>

#include "spdlog/spdlog.h"

#include "util.h"
#include "crawler.h"

using namespace std::chrono_literals;
using nlohmann::json;

namespace crawl {

void to_json(nlohmann::json &j, const site &s)
{
  j["path"] = s.m_site.path;
  j["host"] = s.m_site.host;
  j["level"] = s.level;
  j["max_pages"] = s.max_pages;
  j["last_scanned"] = s.last_scanned;
  j["indexed"] = s.indexed;
  j["merged"] = s.merged;
}

void from_json(const nlohmann::json &j, site &s)
{
  j.at("path").get_to(s.m_site.path);
  j.at("host").get_to(s.m_site.host);
  j.at("level").get_to(s.level);
  j.at("max_pages").get_to(s.max_pages);
  j.at("last_scanned").get_to(s.last_scanned);
  j.at("indexed").get_to(s.indexed);
  j.at("merged").get_to(s.merged);

  s.scraped = s.last_scanned > 0;
}

void crawler::save()
{
  spdlog::debug("save {}", sites_path);

  std::ofstream file;

  json j = {
    {"sites", sites}
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

  try {
    json j = json::parse(file);

    j.at("sites").get_to(sites);

  } catch (const std::exception& e) {
    spdlog::warn("failed to load {}", sites_path);
  }

  file.close();

  spdlog::debug("load {} finished", sites_path);
}

site * crawler::find_site(const std::string &host)
{
  for (auto &i: sites) {
    if (i.m_site.host == host) {
      return &i;
    }
  }

  return NULL;
}

static std::string host_hash(const std::string &host) {
	uint32_t result = 0;

  for (auto &c: host)
		result = (c + 31 * result);

	result = result & ((1<<15) - 1);

  return std::to_string(result);
}

static std::string site_path(std::string base_dir, std::string host)
{
  std::string dir_path = fmt::format("{}/{}",
      base_dir, host_hash(host));

  util::make_path(dir_path);

  return fmt::format("{}/{}", dir_path, host);
}

std::string crawler::get_meta_path(const std::string &host) {
  return fmt::format("{}.json", site_path(site_meta_path, host));
}

std::string crawler::get_data_path(const std::string &host) {
  auto path = site_path(site_data_path, host);

  util::make_path(path);

  return path;
}

/*
scrape::site crawler::make_scrape_site(site *s,
    size_t site_max_con, size_t max_site_part_size, size_t max_page_size)
{
  std::list<scrape::page> pages;

  for (auto &p: s->pages) {
    pages.emplace_back(p.url, p.path, p.last_scanned);
  }

  scrape::site out(
              s->host, pages,
              get_data_path(s->host),
              levels[s->level].max_pages,
              site_max_con,
              max_site_part_size,
              max_page_size);

  return std::move(out);
}
*/

bool crawler::check_blacklist(const std::string &host)
{
  for (auto &b: blacklist) {
    if (host.find(b) != std::string::npos) {
      return true;
    }
  }

  return false;
}

page* site::find_add_page(std::string url, size_t level, std::string path)
{
  load();

  if (level > level) level = level;

  auto p = m_site.find_page(url);
  if (p != NULL) {
    return p;
  }

  p = m_site.find_page_by_path(path);
  if (p != NULL) {
    return p;
  }

  page_count++;
  return m_site.add_page(url, path);
}

void crawler::enable_references(
    site *isite,
    size_t max_add_sites,
    size_t next_max_pages)
{
  std::map<std::string, size_t> sites_link_count;

  isite->load();

  for (auto &page: isite->m_site.pages) {
    for (auto &l: page.links) {
      auto host = util::get_host(l.first);
      if (host == isite->m_site.host) continue;

      auto it = sites_link_count.try_emplace(host, l.second);
      if (!it.second) {
        it.first->second += l.second;
      }
    }
  }

  std::list<std::string> linked_sites;

  for (auto &s: sites_link_count) {
    linked_sites.push_back(s.first);
  }

  linked_sites.sort(
      [&sites_link_count](std::string a, std::string b) {
        auto aa = sites_link_count.find(a);
        auto bb = sites_link_count.find(b);
        return aa->second > bb->second;
      });

  size_t add_sites = 0;
  for (auto &host: linked_sites) {
    auto site = find_site(host);
    if (site != NULL) {
      site->max_pages += next_max_pages;
      add_sites++;

      spdlog::debug("site {} is adding available pages {} to {}",
          isite->m_site.host, next_max_pages, site->m_site.host);

      if (add_sites >= max_add_sites) {
        break;
      }
    }
  }
}

/*
static void add_link(page *p, page_id id, size_t count)
{
  for (auto &l: p->links) {
    if (l.first == id) {
      l.second += count;
      return;
    }
  }

  p->links.emplace_back(id, count);
}
*/

void crawler::expand_links(site *isite)
{
  spdlog::info("update {} info", isite->m_site.host);

  for (auto &p: isite->m_site.pages) {
    for (auto &l: p.links) {
      auto host = util::get_host(l.first);
      if (host == "") continue;
      if (host == isite->m_site.host) continue;

      if (check_blacklist(host)) {
        continue;
      }

      site *o_site = find_site(host);
      if (o_site == NULL) {
        sites.emplace_back(get_meta_path(host), host, isite->level + 1);

        o_site = &sites.back();
      }

      o_site->find_add_page(l.first, isite->level + 1);
    }
  }
}

void crawler::load_seed(std::vector<std::string> urls)
{
  for (auto &o: urls) {
    auto host = util::get_host(o);

    spdlog::info("load seed site {}", o);

    if (check_blacklist(host)) {
      continue;
    }

    auto site = find_site(host);
    if (site == NULL) {
      spdlog::info("seed site {} is new", o);
      sites.emplace_back(get_meta_path(host), host, 0);
      site = &sites.back();
    }

    site->find_add_page(o, 0);

    site->max_pages = levels[0].max_pages;
  }
}

bool crawler::have_next_site()
{
  for (auto &site: sites) {
    if (site.scraping) continue;
    if (site.scraped) continue;
    if (site.max_pages == 0) continue;
    if (site.level >= levels.size()) continue;

    return true;
  }

  return false;
}

site* crawler::get_next_site()
{
  site *s = NULL;

  auto start = std::chrono::steady_clock::now();

  for (auto &site: sites) {
    if (site.scraping) continue;
    if (site.scraped) continue;
    if (site.max_pages == 0) continue;
    if (site.level >= levels.size()) continue;

    if (s == NULL || site.level < s->level) {
      s = &site;
    }
  }

  auto end = std::chrono::steady_clock::now();
  std::chrono::duration<double, std::milli> elapsed = end - start;
  if (elapsed.count() > 100) {
    spdlog::warn("get next site took {}ms", elapsed.count());
  }

  return s;
}

}

