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

#include <curl/curl.h>

#include "util.h"
#include "scrape.h"

util::page * find_page(util::site *site, std::string url) {
  for (auto &p: site->pages) {
    if (p.url == url) {
      return &p;
    }
  }

  return NULL;
}

void insert_site_index(
    util::site *index_site,
    std::string url, std::string path,
    int count) 
{
  auto p = find_page(index_site, url);
    
  if (p == NULL) {
    util::page page = {url, path, count};
    index_site->pages.push_back(page);

  } else {
    p->path = path;
    p->refs += count;
  }
}

void insert_site_index(
    util::site *index_site,
    std::list<scrape::index_url> &site_index) 
{
  for (auto &u: site_index) {
    insert_site_index(index_site, u.url, u.path, u.count);
  }
}

bool check_blacklist(
      std::vector<std::string> &blacklist, 
      std::string url)
{
  for (auto &b: blacklist) {
    if (url.find(b) != std::string::npos) {
      return true;
    }
  }

  return false;
}

void insert_site_other(
    std::list<util::site> &index,
    size_t level,
    std::list<scrape::other_url> &site_other,
    std::vector<std::string> &blacklist)
{
  for (auto &u: site_other) {

    if (check_blacklist(blacklist, u.url)) {
      continue;
    }

    std::string url(u.url);
    auto host = util::get_host(url);
  
    auto index_site = util::index_find_host(index, host);
    if (index_site == NULL) {
      util::page page = {u.url, "", u.count};

      util::site site = {host, false ,level, 1};
      site.pages.push_back(page);

      index.push_back(site);

    } else {
      insert_site_index(index_site, u.url, "", u.count);
    }
  }
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
    std::list<util::site> &index)
{
  struct host_data {
    std::string host;
    size_t refs;
    size_t level;
  };

  printf("get hosts\n");

  std::vector<host_data> hosts;

  hosts.reserve(index.size());

  for (auto &site: index) {
    if (site.scraped) continue;
  printf("put for sortig %s\n", site.host.c_str());
    host_data h = {site.host, site.refs, site.level};
    hosts.push_back(std::move(h));
  }

  std::sort(hosts.begin(), hosts.end(), 
      [&index](host_data &a, host_data &b) {
        return a.refs > b.refs;
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
    std::list<util::site> &index)
{
  printf("start new thread for %s\n", host.c_str());

  auto site = util::index_find_host(index, host);
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
  std::list<util::site> &index,
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

    auto site = util::index_find_host(index, t.host);
    if (site == NULL) {
      printf("site %s not found in index.\n", t.host.c_str());
      exit(1);
    }

    site->scraped = true;

    insert_site_index(site, t.url_index);
    
    insert_site_other(index, level + 1, t.url_other, blacklist);
 
    // Save the current index so exiting early doesn't loose
    // all the work that has been done
    util::save_index(index, "full_index");

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

int main(int argc, char *argv[]) {
  std::vector<std::string> blacklist = util::load_list("../mine/blacklist");
  std::vector<std::string> initial_seed = util::load_list("../mine/seed");

  std::list<scrape::other_url> seed_other;
  for (auto &u: initial_seed) {
    scrape::other_url i = {1, u};
    seed_other.push_back(i);
  }

  std::list<util::site> index;

  struct level {
    size_t max_sites;
    size_t max_pages;
  };

  curl_global_init(CURL_GLOBAL_DEFAULT);

  std::vector<struct level> levels = {{0, 2000}, {1000, 50}, {1000, 1}};
  //std::vector<struct level> levels = {{0, 500}, {5, 5}, {5, 1}};
  //std::vector<struct level> levels = {{0, 2}, {50, 2}, {50, 1}};
  size_t level_count = 1;

  insert_site_other(index, level_count, seed_other, blacklist);

  save_index(index, "full_index");

  for (auto level: levels) {
      run_round(level_count++, level.max_sites, level.max_pages, 
        index, blacklist);
  }

  index.clear();

  curl_global_cleanup();

  return 0;
}

