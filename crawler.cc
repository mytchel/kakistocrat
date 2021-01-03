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

#include <curl/curl.h>

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

page* site_find_add_page(site *site,
    std::string url)
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

  return &site->pages.emplace_back(id, url, path, false);
}

void insert_site_index(
    index &index,
    site *isite,
    size_t max_add_sites,
    std::list<scrape::index_url> &site_index,
    std::vector<std::string> &blacklist)
{
  for (auto &u: site_index) {
    auto p = isite->find_page(u.url);
    if (p == NULL) {
      isite->pages.emplace_back(isite->next_id++, u.url, u.path, true);

    } else {
      p->scraped = true;
      p->path = u.path;
    }
  }

  std::list<site> new_sites;

  struct site_link_data {
    std::string host;
    size_t count;
  };

  std::list<site_link_data> linked_sites;

  for (auto &u: site_index) {
    auto p = isite->find_page(u.url);
    if (p == NULL) continue;

    for (auto &l: u.links) {
      auto host = util::get_host(l);

      if (host == isite->host) {
        auto n_p = isite->find_page(u.url);
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
    if (add_sites++ > max_add_sites) {
      break;
    }

    for (auto &s: new_sites) {
      if (s.host == l.host) {
        index.sites.push_back(s);
        break;
      }
    }
  }
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

size_t site_ref_count(site &site) {
  size_t sum = 0;
  for (auto &p: site.pages) {
    sum += p.links.size();
  }
  return sum;
}

struct thread_data {
  scrape::site t_site;
  std::future<void> future;

  thread_data(size_t max_pages, site *s);
  thread_data() : t_site() {}

  bool finished() {
    return future.wait_for(std::chrono::seconds(0)) == std::future_status::ready;
  }

  void begin() {
    future = std::async(std::launch::async,
        [this]() {
          scrape::scrape(&t_site);
        });
  }
};

thread_data::thread_data(size_t max_pages, site *s) :
  t_site(s->host, max_pages)
{
  for (auto &p: s->pages) {
    t_site.url_scanning.emplace_back(p.url, p.path);
  }
}

std::string* get_next_host(index &index)
{
  std::string *host = NULL;
  size_t level = 1000;

  for (auto &site: index.sites) {
    if (site.scraping) continue;
    if (site.scraped) continue;

    if (site.level < level) {
      level = site.level;
      host = &site.host;
    }
  }

  if (host != NULL) {
    return host;
  } else {
    return NULL;
  }
}

thread_data pop_finished_thread(std::list<thread_data> &threads)
{
  while (threads.size() > 0) {
    std::this_thread::sleep_for(std::chrono::seconds(1));

    auto t = threads.begin();
    while (t != threads.end()) {
      if (t->finished()) {
        auto tt = std::move(*t);
        threads.erase(t++);
        return tt;
      } else {
        t++;
      }
    }
  }

  thread_data dummy;
  return dummy;
}

bool maybe_start_thread(
    std::vector<level> &levels,
    std::list<thread_data> &threads,
    index &index,
    std::vector<std::string> &blacklist)
{
  auto host = get_next_host(index);

  if (host == NULL) {
    return false;
  }

  auto site = index.find_host(*host);
  if (site == NULL) {
    printf("site %s not found in index.\n", host->c_str());
    exit(1);
  }

  site->scraping = true;

  printf("start thread for '%s' level %i (max_pages = %i)\n",
      host->c_str(), site->level,
      levels[site->level].max_pages);

  threads.emplace_back(levels[site->level].max_pages, site);
  threads.back().begin();

  return true;
}

void crawl(std::vector<level> levels,
    size_t max_threads,
    index &index,
    std::vector<std::string> &blacklist)
{
  std::list<thread_data> threads;

  for (auto &s: index.sites) {
    if (!s.scraped) continue;

    size_t fails = 0;
    for (auto &p: s.pages) {
      if (!p.scraped) fails++;
    }

    if (fails > s.pages.size() / 4) {
      s.scraped = false;
    }
  }

  while (threads.size() < max_threads) {
    if (!maybe_start_thread(levels, threads, index, blacklist)) {
      break;
    }
  }

  while (threads.size() > 0) {
    printf("waiting on %i threads\n", threads.size());

    auto t = pop_finished_thread(threads);

    auto site = index.find_host(t.t_site.host);
    if (site == NULL) {
      printf("site %s not found in index.\n", t.t_site.host.c_str());
      exit(1);
    }

    printf("thread finished for %s level %i\n", site->host.c_str(), site->level);

    site->scraped = true;
    site->scraping = false;

    insert_site_index(index, site, levels[site->level].max_add_sites,
        t.t_site.url_scanned, blacklist);

    // Save the current index so exiting early doesn't loose
    // all the work that has been done
    index.save("index.scrape");

    while (threads.size() < max_threads) {
      if (!maybe_start_thread(levels, threads, index, blacklist)) {
        break;
      }
    }
  }

  index.save("index.scrape");

  printf("all done\n");
}

}

