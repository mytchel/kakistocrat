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
#include "crawl.h"
#include "crawler.h"

using namespace std::chrono_literals;
using nlohmann::json;

namespace crawl {

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
      {"last_scanned", s.last_scanned},
      {"indexed_part", s.indexed_part},
      {"indexed", s.indexed}
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
      bool i_p = false, i_i = false;
      try {
        s_j.at("indexed_part").get_to(i_p);
        s_j.at("indexed").get_to(i_i);
      } catch (const std::exception& e) {
        spdlog::debug("default to not indexed");
      }

      sites.emplace_back(
            s_j.at("path").get<std::string>(),
            s_j.at("id").get<std::uint32_t>(),
            s_j.at("host").get<std::string>(),
            s_j.at("level").get<size_t>(),
            s_j.at("max_pages").get<size_t>(),
            s_j.at("last_scanned").get<time_t>(),
            i_p, i_i);
    }

  } catch (const std::exception& e) {
    spdlog::warn("failed to load {}", sites_path);
  }

  file.close();

  spdlog::debug("load {} finished", sites_path);
}

site * crawler::find_site(const std::string &host)
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

  return fmt::format("{}/{}.json", dir_path, host);
}

std::string crawler::get_data_path(const std::string &host) {
  return site_path(site_data_path, host);
}

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
              site_max_con,
              levels[s->level].max_pages,
              max_site_part_size,
              max_page_size);

  return std::move(out);
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

page* site_find_add_page(site *site, std::string url, size_t level,
    std::string path = "")
{
  site->load();

  if (site->level > level) site->level = level;

  auto p = site->find_page(url);
  if (p != NULL) {
    return p;
  }

  p = site->find_page_by_path(path);
  if (p != NULL) {
    return p;
  }

  auto id = site->next_id++;

  site->changed = true;
  return &site->pages.emplace_back(id, url, path);
}

void crawler::enable_references(
    site *isite,
    size_t max_add_sites,
    size_t next_max_pages)
{
  std::map<uint32_t, size_t> sites_link_count;

  isite->load();

  for (auto &page: isite->pages) {
    for (auto &l: page.links) {
      if (l.first.site == isite->id) continue;

      auto it = sites_link_count.try_emplace(l.first.site, l.second);
      if (!it.second) {
        it.first->second += l.second;
      }
    }
  }

  std::list<uint32_t> linked_sites;

  for (auto &s: sites_link_count) {
    linked_sites.push_back(s.first);
  }

  linked_sites.sort(
      [&sites_link_count](uint32_t a, uint32_t b) {
        auto aa = sites_link_count.find(a);
        auto bb = sites_link_count.find(b);
        return aa->second > bb->second;
      });

  size_t add_sites = 0;
  for (auto &sid: linked_sites) {
    auto site = find_site(sid);
    if (site != NULL) {
      site->max_pages += next_max_pages;
      add_sites++;

      spdlog::debug("site {} is adding available pages {} to {}",
          isite->host, next_max_pages, site->host);

      if (add_sites >= max_add_sites) {
        break;
      }
    }
  }
}

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

void crawler::update_site(
    site *isite,
    std::list<scrape::page *> &page_list)
{
  spdlog::info("update {} info", isite->host);

  for (auto &u: page_list) {
    auto p = site_find_add_page(isite, u->url, isite->level, u->path);

    p->links.clear();

    p->title = u->title;

    p->path = u->path;
    p->last_scanned = u->last_scanned;
  }

  for (auto &u: page_list) {
    auto p = isite->find_page(u->url);
    if (p == NULL) continue;

    for (auto &l: u->links) {
      auto host = util::get_host(l.first);
      if (host == "") continue;

      if (host == isite->host) {
        auto n_p = isite->find_page(l.first);
        // Incase all the links didn't come though then
        // don't fuck with the page reference.
        if (n_p != NULL) {
          add_link(p, page_id(isite->id, n_p->id), l.second);
        }

      } else {
        if (check_blacklist(host)) {
          continue;
        }

        site *o_site = find_site(host);
        if (o_site == NULL) {
          sites.emplace_back(
                site_path(site_meta_path, host),
                next_id++, host, isite->level + 1);

          o_site = &sites.back();
        }

        auto o_p = site_find_add_page(o_site, l.first, isite->level + 1);

        add_link(p, page_id(o_site->id, o_p->id), l.second);
      }
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
      sites.emplace_back(
          site_path(site_meta_path, host),
          next_id++, host, 0);
      site = &sites.back();
    }

    site_find_add_page(site, o, 0);

    site->changed = true;
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

