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

util::page * find_page(struct util::site *site, std::string url) {
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
    int count) {

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
    struct util::site *index_site,
    std::list<scrape::index_url> &site_index) {

  for (auto &u: site_index) {
    insert_site_index(index_site, u.url, u.path, u.count);
  }
}

bool check_blacklist(
      std::vector<std::string> &blacklist, 
      std::string url
) {
  for (auto &b: blacklist) {
    if (url.find(b) != std::string::npos) {
      return true;
    }
  }

  return false;
}

void insert_site_other(
    std::list<struct util::site> &index,
    int level,
    std::list<scrape::other_url> &site_other,
    std::vector<std::string> &blacklist
) {
  for (auto &u: site_other) {

    if (check_blacklist(blacklist, u.url)) {
      continue;
    }

    std::string url(u.url);
    auto host = util::get_host(url);
  
    auto index_site = util::index_find_host(index, host);
    if (index_site == NULL) {
      util::page page = {u.url, "", u.count};

      util::site site = {host, level, false, 1};
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

void run_round(int level, int max_sites, int max_pages,
  std::list<struct util::site> &index,
  std::vector<std::string> &blacklist)
{
  printf("run round %i\n", level);

  std::vector<std::string> hosts;

  max_sites = 50;

  hosts.reserve(index.size());

  for (auto &site: index) {
    if (site.scraped) continue;
    hosts.push_back(std::string(site.host));
  }

  std::sort(hosts.begin(), hosts.end(), 
      [&index](std::string &host_a, std::string &host_b) {
 
        auto site_a = util::index_find_host(index, host_a);
        if (site_a == NULL) {
          return false;
        }

        auto site_b =  util::index_find_host(index, host_b);
        if (site_b == NULL) {
          return true;
        }

        return site_a->refs > site_b->refs;
      });

  int site_count = 0;
  std::vector<thread_data> threads;

  // TODO: should run this in batches

  threads.reserve(max_sites);

  for (auto &host: hosts) {
    if (max_sites > 0 && ++site_count >= max_sites) {
      break;
    }

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
  }

  for (auto &t: threads) {
    t.future = std::async(std::launch::async,
        [max_pages, &t]() {
          scrape::scrape(max_pages, t.host, t.urls, t.url_index, t.url_other);
        });
  }

  bool waiting = true;
  while (waiting) {
    std::this_thread::sleep_for(std::chrono::seconds(2));

    waiting = false;
    for (auto &t: threads) {
      if (t.done) continue;

      if (future_is_ready(t.future)) {
        printf("finished %s: scrapped %lu pages and found %lu others\n", 
            t.host.c_str(), t.url_index.size(), t.url_other.size());

        auto site = util::index_find_host(index, t.host);
        if (site == NULL) {
          printf("site %s not found in index.\n", t.host.c_str());
          exit(1);
        }

        site->scraped = true;
        /*

        insert_site_index(site, t.url_index);
        
        insert_site_other(index, level + 1, t.url_other, blacklist);
     
        // Save the current index so exiting early doesn't loose
        // all the work that has been done
        util::save_index(index, "full_index");
        */

        t.url_index.clear();
        t.url_other.clear();
        t.url_other.clear();

        t.done = true;
      }

      if (!t.done) waiting = true;
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

  std::list<struct util::site> index;

  struct level {
    int max_sites;
    int max_pages;
  };

  curl_global_init(CURL_GLOBAL_DEFAULT);

  //std::vector<struct level> levels = {{0, 2000}, {1000, 50}, {1000, 1}};
  std::vector<struct level> levels = {{50, 500}, {5, 5}, {5, 1}};
  //std::vector<struct level> levels = {{0, 2}, {50, 2}, {50, 1}};
  int level_count = 1;

  insert_site_other(index, level_count, seed_other, blacklist);

  save_index(index, "full_index");

  for (auto level: levels) {
      printf("wait before running\n");
      std::this_thread::sleep_for(std::chrono::seconds(5));

      run_round(level_count++, level.max_sites, level.max_pages, 
        index, blacklist);

      printf("wait after running\n");
      std::this_thread::sleep_for(std::chrono::seconds(5));
  }

  index.clear();

  curl_global_cleanup();

  return 0;
}

