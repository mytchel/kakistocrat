#include <stdio.h>
#include <unistd.h>

#include <cstdlib>
#include <cstring>

#include <vector>
#include <list>
#include <string>
#include <algorithm>
#include <thread>
#include <future>
#include <optional>
#include <iostream>
#include <fstream>
#include <cstdint>

#include <curl/curl.h>

#include "util.h"
#include "scrape.h"
#include "crawl.h"

namespace crawl {

void save_index(index &index, std::string path)
{
  std::ofstream file;
  
  printf("save index %lu -> %s\n", index.sites.size(), path.c_str());

  file.open(path, std::ios::out | std::ios::trunc);

  if (!file.is_open()) {
    fprintf(stderr, "error opening file %s\n", path.c_str());
    return;
  }

  for (auto &site: index.sites) {
    bool has_pages = false;
    for (auto &p: site.pages) {
      if (p.path.empty()) continue;

      has_pages = true;
      break;
    }

    if (!has_pages) continue;

    file << site.id << "\t";
    file << site.host << "\t";
    file << site.level << "\n";

    for (auto &p: site.pages) {
      if (p.path.empty()) continue;

      file << "\t";
      file << p.id << "\t";
      file << p.url << "\t";
      file << p.path;

      for (auto &l: p.linked_by) {
        file << "\t" << l.site << ":" << l.page;
      }

      file << "\n";
    }
  }
  file.close();
}

index load_index(std::string path)
{
  std::ifstream file;
  index index;

  printf("load %s\n", path.c_str());

  file.open(path, std::ios::in);

  if (!file.is_open()) {
    fprintf(stderr, "error opening file %s\n", path.c_str());
    return index;
  }

  std::string line;
  while (getline(file, line)) {
  }

  file.close();

  return index;
}

site * index_find_host(
        index &index,
        std::string host)
{
  for (auto &i: index.sites) {
    if (i.host == host) {
      return &i;
    }
  }

  return NULL;
}

page& index_find_add_page(site *site, std::uint32_t id, 
    std::string url, std::string path) 
{
  for (auto &p: site->pages) {
    if (p.id == id) {
      return p;
    }
  }

  page page = {id, url, path};
  site->pages.push_back(page);

  return site->pages.back();
}

void insert_site_index(
    site *site,
    std::list<scrape::index_url> &site_index) 
{
  for (auto &u: site_index) {
    auto &p = index_find_add_page(site, u.id, u.url, u.path);
    for (auto &l: u.linked_by) {
      page_id id = {site->id, l};
      p.linked_by.push_back(id);
    }
  }
}

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

void insert_site_other(
    index &index,
    std::uint32_t site_id,
    size_t level,
    std::list<scrape::other_url> &site_other,
    std::vector<std::string> &blacklist)
{
  for (auto &u: site_other) {
    auto host = util::get_host(u.url);
 
    if (check_blacklist(blacklist, host)) {
      continue;
    }
 
    auto index_site = index_find_host(index, host);
    if (index_site == NULL) {
      site site = {index.next_id++, host, level, false};

      page page = {u.id, u.url, ""};
      site.pages.push_back(page);

      index.sites.push_back(site);

    } else {
      auto &p = index_find_add_page(index_site, u.id, u.url, "");
      for (auto &l: u.linked_by) {
        page_id id = {site_id, l};
        p.linked_by.push_back(id);
      }
    }
  }
}

size_t count_site_refs(site &site) {
  size_t sum = 0;
  for (auto &p: site.pages) {
    sum += p.linked_by.size();
  }
  return sum;
}

struct thread_data {
  std::string host;

  std::vector<std::string> urls;
  
  std::list<scrape::index_url> url_index;
  std::list<scrape::other_url> url_other;

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

  float min_score = 1.0;

  for (auto &site: index.sites) {
    if (site.scraped) continue;
    host_data h = {site.host, count_site_refs(site)};
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
  printf("start new thread for %s\n", host.c_str());

  auto site = index_find_host(index, host);
  if (site == NULL) {
    printf("site %s not found in index.\n", host.c_str());
    exit(1);
  }

  thread_data t;

  t.host = host;
  
  t.urls.reserve(site->pages.size());

  for (auto &p: site->pages) {
    t.urls.push_back(p.url);
  }

  threads.push_back(std::move(t));

  auto &tt = threads.back();
  tt.future = std::async(std::launch::async,
      [max_pages, &tt]() {
        scrape::scrape(max_pages, tt.host, tt.urls, tt.url_index, tt.url_other);
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

const size_t max_threads = 100;

void run_round(size_t level, size_t max_sites, size_t max_pages,
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

    auto site = index_find_host(index, t.host);
    if (site == NULL) {
      printf("site %s not found in index.\n", t.host.c_str());
      exit(1);
    }

    site->scraped = true;

    insert_site_index(site, t.url_index);
    
    insert_site_other(index, site->id, level + 1, t.url_other, blacklist);
 
    // Save the current index so exiting early doesn't loose
    // all the work that has been done
    save_index(index, "full_index");

    t.url_index.clear();
    t.url_other.clear();

    if (hosts.size() > 0) {
      auto host = hosts.front();
      hosts.pop_front();

      start_thread(threads, max_pages, host, index);
    }
  }

  printf("all done\n");
}

}

int main(int argc, char *argv[]) {
  std::vector<std::string> blacklist = util::load_list("../mine/blacklist");
  std::vector<std::string> initial_seed = util::load_list("../mine/seed");

  std::list<scrape::other_url> seed_other;
  for (auto &u: initial_seed) {
    scrape::other_url i = {1, u};
    seed_other.push_back(i);
  }

  crawl::index index;

  struct level {
    size_t max_sites;
    size_t max_pages;
  };

  curl_global_init(CURL_GLOBAL_DEFAULT);

  std::vector<struct level> levels = {{0, 2000}, {1000, 50}, {1000, 1}};
  //std::vector<struct level> levels = {{0, 500}, {5, 5}, {5, 1}};
  //std::vector<struct level> levels = {{0, 2}, {50, 2}, {50, 1}};
  size_t level_count = 1;

  crawl::insert_site_other(index, 0, level_count, seed_other, blacklist);

  crawl::save_index(index, "full_index");

  for (auto level: levels) {
    crawl::run_round(level_count++, level.max_sites, level.max_pages, 
        index, blacklist);
  }

  curl_global_cleanup();

  return 0;
}

