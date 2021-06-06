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
  j["path"] = s.path;
  j["host"] = s.host;
  j["level"] = s.level;
  j["max_pages"] = s.max_pages;
  j["last_scanned"] = s.last_scanned;
  j["indexed"] = s.indexed;
  j["merged"] = s.merged;
}

void from_json(const nlohmann::json &j, site &s)
{
  j.at("path").get_to(s.path);
  j.at("host").get_to(s.host);
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

  for (auto &site: sites) {
    sites_map.emplace(site.host, &site);
  }

  spdlog::debug("load {} finished", sites_path);
}

site* crawler::find_site(const std::string &host)
{
  auto it = sites_map.find(host);
  if (it == sites_map.end()) {
    return nullptr;
  }

  return it->second;
}

site* crawler::add_site(const std::string &host, size_t level)
{
  auto &site = sites.emplace_back(get_meta_path(host), host, level);

  sites_map.emplace(host, &site);

  return &site;
}

static std::string host_hash(const std::string &host) {
	uint32_t result = 0;

  for (auto &c: host)
		result = (c + 31 * result);

  // TODO: make this way smaller. 1<<10 or so
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
  return fmt::format("{}.capnp", site_path(site_meta_path, host));
}

std::string crawler::get_data_path(const std::string &host) {
  auto path = site_path(site_data_path, host);

  util::make_path(path);

  return path;
}

bool crawler::check_blacklist(const std::string &host)
{
  for (auto &b: blacklist) {
    if (host.find(b) != std::string::npos) {
      return true;
    }
  }

  return false;
}

page* site::find_add_page(const std::string &url, size_t n_level, const std::string &path)
{
  load();

  if (level > n_level) level = n_level;

  auto p = find_page(url);
  if (p != NULL) {
    return p;
  }

  if (path != "") {
    p = find_page_by_path(path);
    if (p != NULL) {
      return p;
    }
  }

  return add_page(url, path);
}

void crawler::expand(
    site *isite,
    size_t max_add_sites,
    size_t next_max_pages)
{
  std::unordered_map<std::string, std::vector<uint32_t>> links_map;
  
  spdlog::info("expand {} starting", isite->host);

  isite->load();

  for (auto &page: isite->pages) {
    for (auto &l: page.links) {
      auto &link_url = isite->urls[l.first];
      auto host = util::get_host(link_url);

      if (host == "") continue;
      if (host == isite->host) continue;

      if (check_blacklist(host)) {
        continue;
      }

      auto it = links_map.try_emplace(host);
      it.first->second.push_back(l.first);
    }
  }

  spdlog::info("expand {} got {} links", isite->host, links_map.size());

  std::list<std::pair<std::string, std::vector<uint32_t>>> links(links_map.begin(), links_map.end());

/*
  for (auto &s: links_map) {
    links.emplace_back(s.first, s.second);
  }
*/

  links.sort([](auto &a, auto &b) {
      return a.second.size() > b.second.size();
    });
  
  spdlog::info("expand {} sorted {} linkes", isite->host, links.size());

  size_t add_sites = 0;
  for (auto &l: links) {
    auto &host = l.first;
    auto &host_links = l.second;

    auto o_site = find_site(host);
    if (o_site == NULL) {
      o_site = add_site(host, isite->level + 1);
    }

    size_t with_dups = host_links.size();

    auto it = std::unique(host_links.begin(), host_links.end());
    host_links.resize(std::distance(host_links.begin(), it));

    spdlog::debug("expand {} adding {} ({} unique) links to {}",
      isite->host, with_dups, host_links.size(), o_site->host);

    for (auto l: host_links) {
      auto &link_url = isite->urls[l]; 
      o_site->find_add_page(link_url, isite->level + 1);
    }

    o_site->max_pages += next_max_pages;
    add_sites++;

    if (o_site->pages.size() < 5) {
      o_site->flush();
    }

    if (add_sites >= max_add_sites) {
      break;
    }
  }
  
  spdlog::info("expand {} done", isite->host);
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
      site = add_site(host, 0);
    }

    auto p = site->find_add_page(o, 0);

    site->max_pages = levels[0].max_pages;

    site->flush();
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

