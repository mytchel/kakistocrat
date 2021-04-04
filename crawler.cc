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

using namespace std::chrono_literals;

#include <curl/curl.h>

#include <nlohmann/json.hpp>

#include "spdlog/spdlog.h"

#include "util.h"
#include "crawl.h"

namespace crawl {

bool crawler::check_blacklist(
      std::string host)
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
    std::list<scrape::page> &page_list)
{
  spdlog::info("update {} info", isite->host);

  for (auto &u: page_list) {
    auto p = site_find_add_page(isite, u.url, isite->level, u.path);

    p->links.clear();

    p->title = u.title;

    p->path = u.path;
    p->last_scanned = u.last_scanned;
  }

  for (auto &u: page_list) {
    auto p = isite->find_page(u.url);
    if (p == NULL) continue;

    for (auto &l: u.links) {
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

void crawler::load_seed(std::vector<std::string> url)
{
  for (auto &o: url) {
    auto host = util::get_host(o);

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
    spdlog::info("get next site took {}ms", elapsed.count());
  }

  return s;
}

void crawler::crawl()
{
  spdlog::info("starting {} threads", n_threads);

  Channel<scrape::site*> in_channels[n_threads];
  Channel<scrape::site*> out_channels[n_threads];
  Channel<bool> stat_channels[n_threads];
  bool thread_stats[n_threads];

  std::vector<std::thread> threads;
  for (size_t i = 0; i < n_threads; i++) {
    auto th = std::thread(scrape::scraper, i,
        std::ref(in_channels[i]),
        std::ref(out_channels[i]),
        std::ref(stat_channels[i]),
        thread_max_sites, thread_max_con);

    threads.emplace_back(std::move(th));
  }

  std::list<scrape::site> scrapping_sites;

  auto last_save = std::chrono::system_clock::now() - 100s;
  bool have_changes = true;

  while (true) {
    if (have_changes && last_save + 10s < std::chrono::system_clock::now()) {
      spdlog::info("periodic save");

      for (auto &s: sites) {
        s.flush();
      }

      // Save the current index so exiting early doesn't loose
      // all the work that has been done
      save();

      have_changes = false;
      last_save = std::chrono::system_clock::now();
    }

    for (size_t i = 0; i < n_threads; i++) {
      if (!stat_channels[i].empty()) {
        thread_stats[i] << stat_channels[i];
      }

      if (thread_stats[i]) {
        auto site = get_next_site();
        if (site != NULL) {
          thread_stats[i] = false;

          spdlog::info("send site {} to be scraped ({} active)",
              site->host, scrapping_sites.size());

          site->load();

          site->scraping = true;

          std::list<scrape::page> pages;

          for (auto &p: site->pages) {
            pages.emplace_back(p.url, p.path, p.last_scanned);
          }

          scrapping_sites.emplace_back(
              site->host, pages,
              fmt::format("{}/{}/{}",
                  site_data_path,
                  util::host_hash(site->host),
                  site->host),
              levels[site->level].max_pages,
              max_site_part_size,
              max_page_size);

          auto &s = scrapping_sites.back();

          &s >> in_channels[i];
        }
      }

      if (!out_channels[i].empty()) {
        scrape::site *s;
        s << out_channels[i];

        spdlog::info("site finished {}", s->host);

        auto site = find_site(s->host);
        if (site == NULL) {
          spdlog::info("site '{}' not found in map.", s->host);
          exit(1);
        }

        site->load();

        site->max_pages = 0;
        site->scraped = true;
        site->scraping = false;

        site->last_scanned = time(NULL);
        site->changed = true;

        update_site(site, s->url_scanned);

        if (site->level + 1 < levels.size()) {
          auto level = levels[site->level];
          auto next_level = levels[site->level + 1];

          enable_references(site, level.max_add_sites, next_level.max_pages);
        }

        spdlog::info("site finished for level {} with {:3} (+ {:3} unchanged) pages : {}",
            site->level, s->url_scanned.size(), s->url_unchanged.size(),
            site->host);

        scrapping_sites.remove_if([s](const scrape::site &ss) {
            return &ss == s;
            });

        have_changes = true;
      }
    }

    if (scrapping_sites.empty() && !have_next_site()) {
      time_t now = time(NULL);

      bool have_something = false;
      for (auto &s: sites) {
        size_t min = (4 * (1 + s.level)) * 60 * 60;
        if (s.scraped && s.last_scanned + min < now) {
          int r = rand() % ((24 * (1 + s.level) - 4) * 60 * 60);
          bool should_scrape = s.last_scanned + min + r < now;
          if (should_scrape) {
            s.scraped = false;
            spdlog::info("transition {} from scraped to ready", s.host);

            if (s.level == 0) {
              spdlog::info("give site {} at level 0 more pages", s.host);
              s.max_pages = levels[0].max_pages;
            }
          }
        }
      }
    }

    std::this_thread::sleep_for(100ms);
  }
}

}

