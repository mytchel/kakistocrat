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
#include "crawl.h"

namespace crawl {

const size_t max_threads = 500;

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
      //if (p.path.empty()) continue;

      file << "\t";
      file << p.id << "\t";
      file << p.score << "\t";
      file << p.url << "\t";
      file << p.path;

      for (auto &l: p.links) {
        file << "\t" << l.site << " " << l.page;
      }

      file << "\n";
    }
  }
  file.close();
}

void load_index(index &index, std::string path)
{
  std::ifstream file;

  printf("load %s\n", path.c_str());

  file.open(path, std::ios::in);

  if (!file.is_open()) {
    fprintf(stderr, "error opening file %s\n", path.c_str());
    return;
  }

  std::string line;
  while (getline(file, line)) {
    bool have_site = line[0] != '\t';

    std::istringstream ss(line);

    if (have_site) {
      uint32_t id;
      std::string host;
      size_t level;

      ss >> id;
      ss >> host;
      ss >> level;

      site s = {id, host, level};
      index.sites.push_back(s);
 
    } else {
      uint32_t id;
      double score;
      std::string url;
      std::string path;

      ss >> id;
      ss >> score;
      ss >> url;
      ss >> path;

      page p = {id, url, path, score};

      uint32_t ls, lp;
      while (ss >> ls && ss >> lp) {
        page_id id = {ls, lp};
        p.links.push_back(id);
      }

      auto &site = index.sites.back();
      site.pages.push_back(std::move(p));
    }
  }

  file.close();
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

page& index_find_add_page(site *site, 
    std::string url) 
{
  for (auto &p: site->pages) {
    if (p.url == url) {
      return p;
    }
  }

  auto id = site->next_id++;

  page page = {id, url, ""};
  site->pages.push_back(page);

  return site->pages.back();
}

page* index_find_page(site *site, 
    std::string url) 
{
  for (auto &p: site->pages) {
    if (p.url == url) {
      return &p;
    }
  }

  return NULL;
}

void insert_site_index(
    index &index,
    site *site,
    size_t level,
    std::list<scrape::index_url> &site_index,
    std::vector<std::string> &blacklist)
{
  for (auto &u: site_index) {
    auto p = index_find_page(site, u.url);
    if (p == NULL) {
     auto id = site->next_id++;

      page page = {id, u.url, u.path};
      site->pages.push_back(page);

    } else {
      p->path = u.path;
    }
  }

  for (auto &u: site_index) {
    auto p = index_find_page(site, u.url);
    if (p == NULL) continue;
      
    for (auto &l: u.links) {
      auto host = util::get_host(l);

      if (host == site->host) {
        auto n_p = index_find_page(site, u.url);
        if (n_p == NULL) continue;
          
        page_id i = {site->id, n_p->id};
        p->links.push_back(i);

      } else {
        if (check_blacklist(blacklist, host)) {
          continue;
        }
     
        auto o_site = index_find_host(index, host);
        if (o_site == NULL) {
          struct site n_site = {index.next_id++, host, level, false};

          auto &n_p = index_find_add_page(&n_site, l);

          page_id i = {n_site.id, n_p.id};
          p->links.push_back(i);

          index.sites.push_back(n_site);

        } else {
          auto &o_p = index_find_add_page(o_site, l);

          page_id i = {o_site->id, o_p.id};
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
 
    auto o_site = index_find_host(index, host);
    if (o_site == NULL) {
      struct site n_site = {index.next_id++, host, 1, false};

      index_find_add_page(&n_site, o);

      index.sites.push_back(n_site);

    } else {
      index_find_add_page(o_site, o);
    }
  }
}

double site_score(site &site) {
  double sum = 0;
  for (auto &p: site.pages) {
    sum += p.score;
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
    double score;
  };

  std::vector<host_data> hosts;

  hosts.reserve(index.sites.size());

  for (auto &site: index.sites) {
    if (site.scraped) continue;
    host_data h = {site.host, site_score(site)};
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
  auto site = index_find_host(index, host);
  if (site == NULL) {
    printf("site %s not found in index.\n", host.c_str());
    exit(1);
  }

  thread_data t;

  t.host = host;
  
  t.urls.reserve(site->pages.size());

  for (auto &p: site->pages) {
    auto path = p.path;
    if (path == "") {
      path = util::make_path(p.url);
    }

    scrape::index_url u = {p.url, path};

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

void score_iteration(index &index)
{
  std::map<page_id, double> new_scores;

  printf("score iteration for %i sites\n", index.sites.size());

  for (auto &s: index.sites) {
    printf("  scoring %s with %i pages\n", s.host.c_str(), s.pages.size());
    
    for (auto &p: s.pages) {
      if (p.links.empty()) continue;

      double link_score = p.score / p.links.size();
      p.score = 0;

      for (auto &l: p.links) {
        auto i = new_scores.find(l);
        if (i == new_scores.end()) {
          new_scores.insert(std::pair<page_id, double>(
                l, link_score));
        } else {
          i->second += link_score;
        }
      }
    }
  }

  printf("apply %i score adjustments\n", new_scores.size());

  for (auto &s: index.sites) {
    for (auto &p: s.pages) {
      page_id id = {s.id, p.id};
      auto score = new_scores.find(id);
      if (score != new_scores.end()) {
        p.score += score->second;
      }
    }
  }
  
  printf("score iteration complete\n");
}

void score_initial(index &index, size_t level, size_t max_level) 
{
  size_t n = 0;

  printf("score initial settings for level %i\n", level);

  for (auto &s: index.sites) {
    if (s.level == level) n++;
  }

  double r = 2.0/3.0;
  if (level == max_level) {
    r = 1.0;
  }

  double c = 1.0;
  for (size_t l = 1; l < level; l++) {
    c *= 1.0/3.0;
  }

  double base = c * r / n;

  printf("score base value of %f = %f * %f / %i\n", 
      base, c, r, n);

  for (auto &s: index.sites) {
    if (s.level != level) continue;

    double page_score = base / s.pages.size();

    for (auto &p: s.pages) {
      p.score = page_score;
    }
  }
}

void run_round(size_t level, size_t max_level,
    size_t max_sites, size_t max_pages,
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

  score_initial(index, level, max_level);
  score_iteration(index);
    
  save_index(index, "full_index");

  printf("all done\n");
}

}

int main(int argc, char *argv[]) {
  std::vector<std::string> blacklist = util::load_list("../mine/blacklist");
  std::vector<std::string> initial_seed = util::load_list("../mine/seed");

  crawl::index index;

  load_index(index, "full_index");

  insert_site_index_seed(index, initial_seed, blacklist);

  struct level {
    size_t max_sites;
    size_t max_pages;
  };

  curl_global_init(CURL_GLOBAL_DEFAULT);

  //std::vector<struct level> levels = {{0, 2000}, {1000, 50}, {1000, 1}};
  std::vector<struct level> levels = {{5, 50}, {20, 5}, {50, 1}};
  //std::vector<struct level> levels = {{0, 2}, {50, 2}, {50, 1}};
  size_t level_count = 1;

  crawl::save_index(index, "full_index");

  for (auto level: levels) {
    crawl::run_round(level_count++, levels.size() + 1,
        level.max_sites, level.max_pages, 
        index, blacklist);
  }

  for (int i = 0; i < 5; i++) {
    score_iteration(index);
    save_index(index, "full_index");
  }

  curl_global_cleanup();

  return 0;
}

