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

#include <curl/curl.h>

#include "channel.h"

#include "util.h"
#include "scrape.h"
#include "scraper.h"
#include "crawl.h"
#include "crawler.h"

namespace crawl {

bool check_blacklist(
      std::vector<std::string> &blacklist,
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
    std::list<scrape::index_url> &page_list,
    std::vector<std::string> &blacklist)
{
  for (auto &u: page_list) {
    auto p = isite->find_page(u.url);
    if (p == NULL) {
      isite->pages.emplace_back(isite->next_id++,
          u.url, u.path, u.last_scanned, u.ok, true);

    } else {
      p->last_scanned = u.last_scanned;
      p->valid = u.ok;
      p->scraped = true;
    }
  }

  std::list<site> new_sites;

  struct site_link_data {
    std::string host;
    size_t count;
  };

  std::list<site_link_data> linked_sites;

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
        if (check_blacklist(blacklist, host)) {
          continue;
        }

        site *o_site = NULL;

        for (auto &s: new_sites) {
          if (s.host == host) {
            o_site = &s;
            break;
          }
        }

        if (o_site == NULL) {
          o_site = index.find_host(host);
        }

        if (o_site == NULL) {
          site n_site(index.next_id++, host, isite->level + 1);

          auto n_p = site_find_add_page(&n_site, l);

          p->links.emplace_back(n_site.id, n_p->id);

          new_sites.push_back(n_site);

          site_link_data data = {host, 1};
          linked_sites.push_back(data);

        } else {
          auto o_p = site_find_add_page(o_site, l);

          p->links.emplace_back(o_site->id, o_p->id);

          bool found = false;
          for (auto &l: linked_sites) {
            if (l.host == host) {
              l.count++;
              found = true;
              break;
            }
          }

          if (!found) {
            site_link_data data = {host, 1};
            linked_sites.push_back(data);
          }
        }
      }
    }
  }

  linked_sites.sort([](site_link_data &a, site_link_data &b) {
        return a.count > b.count;
      });

  size_t add_sites = 0;
  for (auto &l: linked_sites) {
    if (add_sites >= max_add_sites) {
      break;
    }

    add_sites++;

    for (auto &s: new_sites) {
      if (s.host == l.host) {
        index.sites.push_back(s);
        break;
      }
    }
  }

  return add_sites;
}

void insert_site_index_seed(
    index &index,
    std::vector<std::string> url,
    std::vector<std::string> &blacklist)
{
  for (auto &o: url) {
    auto host = util::get_host(o);

    if (check_blacklist(blacklist, host)) {
      continue;
    }

    auto o_site = index.find_host(host);
    if (o_site == NULL) {
      site n_site(index.next_id++, host, 0, false);

      site_find_add_page(&n_site, o);

      index.sites.push_back(n_site);

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
    printf("get next site took %ims\n", elapsed.count());
  }

  return s;
}

void crawl(std::vector<level> levels, index &index,
    std::vector<std::string> &blacklist)
{
  size_t scrapped_sites = 0;

  time_t now = time(NULL);

  for (auto &s: index.sites) {
    s.scraped = s.last_scanned + 60 * 60 * 24 > now;
    printf("load %i : %s\n", s.scraped, s.host.c_str());
  }

  auto n_threads = std::thread::hardware_concurrency();

  printf("starting %i threads\n", n_threads);

  Channel<scrape::site*> in_channels[n_threads];
  Channel<scrape::site*> out_channels[n_threads];
  Channel<bool> stat_channels[n_threads];
  bool thread_stats[n_threads];

  std::vector<std::thread> threads;
  for (size_t i = 0; i < n_threads; i++) {
    auto th = std::thread([](
          Channel<scrape::site*> &in,
          Channel<scrape::site*> &out,
          Channel<bool> &stat, int i) {
        scrape::scraper(in, out, stat, i);
    },
        std::ref(in_channels[i]),
        std::ref(out_channels[i]),
        std::ref(stat_channels[i]), i);

    threads.emplace_back(std::move(th));
  }

  std::list<scrape::site> scrapping_sites;

  int iteration = 0;
  while (!scrapping_sites.empty() || have_next_site(index)) {
    if (++iteration % 50 == 0) {
      printf("main crawled %i / %i sites\n",
          scrapped_sites, index.sites.size());

      // Save the current index so exiting early doesn't loose
      // all the work that has been done
      index.save("index.scrape");
    }

    bool delay = false;
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

      if (out_channels[i].empty()) continue;

      scrape::site *s;
      s << out_channels[i];

      auto site = index.find_host(s->host);
      if (site == NULL) {
        printf("site '%s' not found in index.\n", s->host.c_str());
        exit(1);
      }

      site->scraped = true;
      site->scraping = false;
      site->last_scanned = time(NULL);

      // TODO: changes for unchanged?
      size_t added = insert_site_index(index, site, levels[site->level].max_add_sites,
          s->url_scanned, blacklist);

      printf("site finished for level %i with %3i (+ %3i unchanged) pages, %2i external: %s\n",
          site->level, s->url_scanned.size(), s->url_unchanged.size(), added,
          site->host.c_str());

      scrapping_sites.remove_if([s](const scrape::site &ss) {
          return &ss == s;
          });

      scrapped_sites++;
    }

    if (all_blocked || !have_next_site(index)) {
      std::this_thread::sleep_for(std::chrono::milliseconds(250));
    } else {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
  }

  printf("main finished\n");

  index.save("index.scrape");

  printf("main cleanup threads\n");
  // Wait for all threads to finish
  for (size_t i = 0; i < n_threads; i++) {
    scrape::site *s = NULL;
    s >> in_channels[i];
    threads.at(i).join();
  }

  printf("main end\n");
}

}

