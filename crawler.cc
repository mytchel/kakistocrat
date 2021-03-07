#include <stdio.h>
#include <unistd.h>

#include <cstdlib>
#include <cstring>

#include <vector>
#include <list>
#include <set>
#include <map>
#include <string>
#include <algorithm>
#include <thread>
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

#include "channel.h"

#include "util.h"
#include "scrape.h"
#include "scraper.h"
#include "crawl.h"
#include "crawler.h"

namespace crawl {

bool index::check_blacklist(
      std::string host)
{
  for (auto &b: blacklist) {
    if (host.find(b) != std::string::npos) {
      return true;
    }
  }

  return false;
}

page* site_find_add_page(site *site, std::string url)
{
  auto p = site->find_page(url);
  if (p != NULL) {
    return p;
  }

  auto path = util::make_path(url);

  p = site->find_page_by_path(path);
  if (p != NULL) {
    return p;
  }

  auto id = site->next_id++;

  return &site->pages.emplace_back(id, url, path);
}

size_t insert_site_index(
    index &index,
    site *isite,
    size_t max_add_sites,
    std::list<scrape::index_url> &page_list)
{
  for (auto &u: page_list) {
    auto p = isite->find_page(u.url);
    if (p == NULL) {
      p = isite->find_page_by_path(u.path);
      if (p == NULL) {
        isite->pages.emplace_back(isite->next_id++, u.url, u.path);
        p = &isite->pages.back();
      }
    }

    p->title = u.title;
    p->links.clear();

    p->last_scanned = u.last_scanned;
    p->valid = u.ok;
  }

  std::list<site> new_sites;
  std::map<std::string, size_t> new_sites_link_count;

  for (auto &u: page_list) {
    auto p = isite->find_page(u.url);
    if (p == NULL) continue;

    for (auto &l: u.links) {
      auto host = util::get_host(l);

      if (host == isite->host) {
        auto n_p = isite->find_page(l);
        if (n_p == NULL) continue;

        p->links.emplace_back(isite->id, n_p->id);

      } else {
        if (index.check_blacklist(host)) {
          continue;
        }

        site *o_site = NULL;
        bool is_new_site = false;

        for (auto &s: new_sites) {
          if (s.host == host) {
            o_site = &s;
            is_new_site = true;
            break;
          }
        }

        if (o_site == NULL) {
          o_site = index.find_site(host);
        }

        if (o_site == NULL) {
          site n_site(index.next_id++, host, isite->level + 1);

          auto n_p = site_find_add_page(&n_site, l);

          p->links.emplace_back(n_site.id, n_p->id);

          new_sites.push_back(n_site);
          new_sites_link_count.emplace(host, 1);

        } else {
          auto o_p = site_find_add_page(o_site, l);

          p->links.emplace_back(o_site->id, o_p->id);

          if (is_new_site) {
            auto it = new_sites_link_count.find(host);
            it->second++;
          }
        }
      }
    }
  }

  new_sites.sort(
      [&new_sites_link_count](site &a, site &b) {
        auto aa = new_sites_link_count.find(a.host);
        auto bb = new_sites_link_count.find(b.host);
        return aa->second > bb->second;
      });

  size_t add_sites = 0;
  for (auto &s: new_sites) {
    if (add_sites >= max_add_sites) {
      break;
    }

    add_sites++;
    index.sites.push_back(s);
  }

  return add_sites;
}

void index::load_seed(std::vector<std::string> url)
{
  for (auto &o: url) {
    auto host = util::get_host(o);

    if (check_blacklist(host)) {
      continue;
    }

    auto o_site = find_site(host);
    if (o_site == NULL) {
      site n_site(next_id++, host, 0);

      site_find_add_page(&n_site, o);

      sites.push_back(n_site);

    } else {
      site_find_add_page(o_site, o);
    }
  }
}

bool have_next_site(index &index)
{
  for (auto &site: index.sites) {
    if (site.scraping) continue;
    if (site.scraped) continue;

    return true;
  }

  return false;
}

site* get_next_site(index &index)
{
  site *s = NULL;

  auto start = std::chrono::steady_clock::now();

  for (auto &site: index.sites) {
    if (site.scraping) continue;
    if (site.scraped) continue;

    if (s == NULL || site.level < s->level) {
      s = &site;
    }
  }

  auto end = std::chrono::steady_clock::now();
  std::chrono::duration<double, std::milli> elapsed = end - start;
  if (elapsed.count() > 100) {
    printf("get next site took %fms\n", elapsed.count());
  }

  if (s != NULL) {
    s->load();
  }

  return s;
}

void crawl(std::vector<level> levels, index &index)
{
  auto n_threads = 1;//std::thread::hardware_concurrency();
  // TODO: get from file limit
  size_t max_con_per_thread = 100;//1000 / (2 * n_threads);

  printf("starting %i threads\n", n_threads);

  Channel<scrape::site*> in_channels[n_threads];
  Channel<scrape::site*> out_channels[n_threads];
  Channel<bool> stat_channels[n_threads];
  bool thread_stats[n_threads];

  std::vector<std::thread> threads;
  for (size_t i = 0; i < n_threads; i++) {
    auto th = std::thread(
        [](
            Channel<scrape::site*> &in,
            Channel<scrape::site*> &out,
            Channel<bool> &stat,
            int i, size_t m) {
          scrape::scraper(in, out, stat, i, m);
        },
        std::ref(in_channels[i]),
        std::ref(out_channels[i]),
        std::ref(stat_channels[i]),
        i, max_con_per_thread);

    threads.emplace_back(std::move(th));
  }

  std::list<scrape::site> scrapping_sites;

  index.save();
  auto last_save = std::chrono::system_clock::now();
  bool have_changes = false;

  while (true) {
    if (have_changes && last_save + 10s < std::chrono::system_clock::now()) {
      // Save the current index so exiting early doesn't loose
      // all the work that has been done
      index.save();
      last_save = std::chrono::system_clock::now();
    }

    bool all_blocked = true;

    for (size_t i = 0; i < n_threads; i++) {
      if (!stat_channels[i].empty()) {
        thread_stats[i] << stat_channels[i];
        all_blocked &= !thread_stats[i];
      }

      if (thread_stats[i]) {
        auto site = get_next_site(index);
        if (site != NULL) {
          site->scraping = true;

          std::list<scrape::index_url> urls;

          for (auto &p: site->pages) {
            urls.emplace_back(p.url, p.path, p.last_scanned, p.valid);
          }

          scrapping_sites.emplace_back(site->host, levels[site->level].max_pages, urls);
          auto &s = scrapping_sites.back();

          &s >> in_channels[i];
        }
      }

      if (!out_channels[i].empty()) {
        scrape::site *s;
        s << out_channels[i];

        auto site = index.find_site(s->host);
        if (site == NULL) {
          printf("site '%s' not found in index.\n", s->host.c_str());
          exit(1);
        }

        site->scraped = true;
        site->scraping = false;
        site->last_scanned = time(NULL);

        size_t added = insert_site_index(index, site,
            levels[site->level].max_add_sites,
            s->url_scanned);

        printf("site finished for level %zu with %3zu (+ %3zu unchanged) pages, %2zu external: %s\n",
            site->level, s->url_scanned.size(), s->url_unchanged.size(), added,
            site->host.c_str());

        scrapping_sites.remove_if([s](const scrape::site &ss) {
            return &ss == s;
            });

        site->unload();
        have_changes = true;
      }
    }

    auto delay = std::chrono::milliseconds(1);

    if (scrapping_sites.empty() && !have_next_site(index)) {
      time_t now = time(NULL);

      bool have_something = false;
      for (auto &s: index.sites) {
        size_t min = (4 * (1 + s.level)) * 60 * 60;
        if (s.last_scanned + min < now) {
          int r = rand() % ((24 * (1 + s.level) - 4) * 60 * 60);
          s.scraped = s.last_scanned + min + r < now;
          if (!s.scraped) {
            printf("transition %s from scraped to ready as %i + %i + %i < %i\n",
                s.host.c_str(), s.last_scanned, min, r, now);

            s.load();
            have_something = true;
          }
        } else {
          s.unload();
        }
      }

      if (!have_something) {
        delay = std::chrono::minutes(1);
        printf("have nothing\n");
        index.save();
      }

    } else if (all_blocked) {
      delay = std::chrono::milliseconds(100);
    }

    std::this_thread::sleep_for(delay);
  }
}

}

