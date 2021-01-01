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
#include "crawl_util.h"
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

  page page = {false, id, url, path};
  site->pages.push_back(page);

  return &site->pages.back();
}

void insert_site_index(
    index &index,
    site *site,
    size_t level,
    std::list<scrape::index_url> &site_index,
    std::vector<std::string> &blacklist)
{
  for (auto &u: site_index) {
    auto p = site->find_page(u.url);
    if (p == NULL) {
     auto id = site->next_id++;

      page page = {true, id, u.url, u.path};
      site->pages.push_back(page);

    } else {
      p->scraped = true;
      p->path = u.path;
    }
  }

  for (auto &u: site_index) {
    auto p = site->find_page(u.url);
    if (p == NULL) continue;

    for (auto &l: u.links) {
      auto host = util::get_host(l);

      if (host == site->host) {
        auto n_p = site->find_page(u.url);
        if (n_p == NULL) continue;

        page_id i(site->id, n_p->id);
        p->links.push_back(i);

      } else {
        if (check_blacklist(blacklist, host)) {
          continue;
        }

        auto o_site = index.find_host(host);
        if (o_site == NULL) {
          struct site n_site = {index.next_id++, host, level, false};

          auto n_p = site_find_add_page(&n_site, l);

          page_id i(n_site.id, n_p->id);
          p->links.push_back(i);

          index.sites.push_back(n_site);

        } else {
          auto o_p = site_find_add_page(o_site, l);

          page_id i(o_site->id, o_p->id);
          p->links.push_back(i);
        }
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
      struct site n_site = {index.next_id++, host, 1, false};

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
  std::string host;

  uint32_t next_id;

  std::vector<std::string> urls;

  std::list<scrape::index_url> url_index;

  std::future<void> future;
  bool done{false};
};

template<typename T>
bool future_is_ready(std::future<T>& t){
    return t.wait_for(std::chrono::seconds(0)) == std::future_status::ready;
}

std::list<std::string> get_batch_hosts(
    size_t max_sites,
    index &index)
{
  struct host_data {
    std::string host;
    size_t score;
  };

  std::vector<host_data> hosts;

  hosts.reserve(index.sites.size());

  for (auto &site: index.sites) {
    if (site.scraped) continue;
    host_data h = {site.host, site_ref_count(site)};
    hosts.push_back(std::move(h));
  }

  std::sort(hosts.begin(), hosts.end(),
      [&index](host_data &a, host_data &b) {
        return a.score > b.score;
      });

  std::list<std::string> ret;

  for (auto &h: hosts) {
    if (max_sites > 0 && ret.size() >= max_sites) {
      break;
    }

    ret.push_back(h.host);
  }

  return ret;
}

void start_thread(
    std::list<thread_data> &threads,
    size_t max_pages,
    std::string host,
    index &index)
{
  auto site = index.find_host(host);
  if (site == NULL) {
    printf("site %s not found in index.\n", host.c_str());
    exit(1);
  }

  thread_data t;

  t.host = host;

  t.urls.reserve(site->pages.size());

  for (auto &p: site->pages) {
    scrape::index_url u = {p.url, p.path};

    t.url_index.push_back(u);
  }

  threads.push_back(std::move(t));

  auto &tt = threads.back();
  tt.future = std::async(std::launch::async,
      [max_pages, &tt]() {
        scrape::scrape(max_pages, tt.host, tt.url_index);
      });
}

thread_data pop_finished_thread(std::list<thread_data> &threads)
{
  while (threads.size() > 0) {
    std::this_thread::sleep_for(std::chrono::seconds(2));

    auto t = threads.begin();
    while (t != threads.end()) {
      if (future_is_ready(t->future)) {
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

void run_round(size_t level, size_t max_level,
    size_t max_sites, size_t max_pages,
    size_t max_threads,
    index &index,
    std::vector<std::string> &blacklist)
{
  printf("run round %i\n", level);

  auto hosts = get_batch_hosts(max_sites, index);

  std::list<thread_data> threads;

  while (hosts.size() > 0 && threads.size() < max_threads) {
    auto host = hosts.front();
    hosts.pop_front();

    start_thread(threads, max_pages, host, index);
  }

  while (threads.size() > 0) {
    printf("waiting on %i threads\n", threads.size());

    auto t = pop_finished_thread(threads);

    printf("thread finished for %s\n", t.host.c_str());

    auto site = index.find_host(t.host);
    if (site == NULL) {
      printf("site %s not found in index.\n", t.host.c_str());
      exit(1);
    }

    site->scraped = true;

    insert_site_index(index, site, level + 1, t.url_index, blacklist);

    t.url_index.clear();

    // Save the current index so exiting early doesn't loose
    // all the work that has been done
    index.save("index.scrape");

    if (hosts.size() > 0) {
      auto host = hosts.front();
      hosts.pop_front();

      start_thread(threads, max_pages, host, index);
    }
  }

  index.save("index.scrape");

  printf("all done\n");
}

}

