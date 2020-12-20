#include <stdio.h>
#include <unistd.h>

#include <cstdlib>
#include <cstring>

#include <vector>
#include <list>
#include <set>
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

const size_t max_threads = 100;

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

    //if (!has_pages) continue;

    file << site.id << "\t";
    file << site.host << "\t";
    file << site.level << "\n";

    for (auto &p: site.pages) {
     // if (p.path.empty()) continue;

      file << "\t";
      file << p.id << "\t";
      file << p.url << "\t";
      file << p.path;

      for (auto &l: p.links) {
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

page& index_find_add_page(site *site, std::uint32_t id, 
    std::string url, std::string path) 
{
  for (auto &p: site->pages) {
    if (id > 0) {
      if (p.id == id) {
        p.path = path;
        return p;
      }

    } else if (p.url == url) {
      p.path = path;
      return p;
    }
  }

  if (id == 0) {
    id = site->next_id++;
  }

  page page = {id, url, path};
  site->pages.push_back(page);

  return site->pages.back();
}

void insert_site_index(
    index &index,
    site *site,
    size_t level,
    std::list<scrape::index_url> &site_index,
    std::vector<std::string> &blacklist)
{
  for (auto &u: site_index) {
    auto &p = index_find_add_page(site, u.id, u.url, u.path);

    if (u.id > site->next_id) 
      site->next_id = u.id + 1;

    for (auto &l: u.links) {
      page_id i = {site->id, l};
      p.links.push_back(i);

      if (l > site->next_id) 
        site->next_id = l + 1;
    }

    for (auto &o: u.ext_links) {
      auto host = util::get_host(o);
   
      if (check_blacklist(blacklist, host)) {
        continue;
      }
   
      auto o_site = index_find_host(index, host);
      if (o_site == NULL) {
        struct site n_site = {index.next_id++, host, level, false};

        auto &o_p = index_find_add_page(&n_site, 0, o, "");

        page_id i = {n_site.id, o_p.id};
        p.links.push_back(i);

        index.sites.push_back(n_site);

      } else if (!o_site->scraping) {
        auto &o_p = index_find_add_page(o_site, 0, o, "");

        page_id i = {o_site->id, o_p.id};
        p.links.push_back(i);

      } else {
        // Drop so that id's don't get fucked with
      }
    }
  }
  
  site->scraping = false;
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
 
    auto o_site = index_find_host(index, host);
    if (o_site == NULL) {
      struct site n_site = {index.next_id++, host, 1, false};

      index_find_add_page(&n_site, 0, o, "");

      index.sites.push_back(n_site);

    } else {
      index_find_add_page(o_site, 0, o, "");
    }
  }
}

size_t count_site_refs(site &site) {
  size_t sum = 0;
  for (auto &p: site.pages) {
    sum += 0;//p.linked_by.size();
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

  site->scraping = true;

  thread_data t;

  t.host = host;
  
  t.urls.reserve(site->pages.size());

  for (auto &p: site->pages) {
    auto path = p.path;
    if (path == "") {
      path = util::make_path(p.url);
    }

    scrape::index_url u = {p.id, p.url, path};

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

    insert_site_index(index, site, level + 1, t.url_index, blacklist);
    
    t.url_index.clear();
 
    // Save the current index so exiting early doesn't loose
    // all the work that has been done
    save_index(index, "full_index");

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

  crawl::index index;

  insert_site_index_seed(index, initial_seed, blacklist);

  struct level {
    size_t max_sites;
    size_t max_pages;
  };

  curl_global_init(CURL_GLOBAL_DEFAULT);

  std::vector<struct level> levels = {{0, 2000}, {1000, 50}, {1000, 1}};
  //std::vector<struct level> levels = {{0, 500}, {5, 5}, {5, 1}};
  //std::vector<struct level> levels = {{0, 2}, {50, 2}, {50, 1}};
  size_t level_count = 1;

  crawl::save_index(index, "full_index");

  for (auto level: levels) {
    crawl::run_round(level_count++, level.max_sites, level.max_pages, 
        index, blacklist);
  }

  curl_global_cleanup();

  return 0;
}

