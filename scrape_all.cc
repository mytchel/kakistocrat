#include <stdio.h>
#include <unistd.h>

#include <cstdlib>
#include <cstring>

#include <vector>
#include <string>
#include <algorithm>

#include "util.h"
#include "scrape.h"

struct page {
  std::string path;
  int refs;
};

struct site {
  std::string host;
  int level;
  bool scraped;
  int refs;
  std::map<std::string, struct page> pages;
};

struct site * index_find_host(
        std::vector<struct site> &index,
        std::string host
) {
  for (auto &i: index) {
    if (i.host == host) {
      return &i;
    }
  }

  return NULL;
}

void insert_site_index(
    std::vector<struct site> &index,
    std::string host,
    std::vector<struct index_url> &site_index
) {

  auto index_site = index_find_host(index, host);
  if (index_site == NULL) {
    printf("site %s not found in index.\n", host.c_str());
    exit(1);
  }

  for (auto &u: site_index) {
    auto u_iter = index_site->pages.find(u.url);
    
    if (u_iter == index_site->pages.end()) {
      struct page page = {u.path, u.count};
      index_site->pages.emplace(u.url, page);

    } else {
      u_iter->second.path = u.path;
      u_iter->second.refs += u.count;
    }
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
    std::vector<struct site> &index,
    int level,
    std::vector<struct other_url> &site_other,
    std::vector<std::string> &blacklist
) {
  printf("insert other\n");

  for (auto &u: site_other) {

    if (check_blacklist(blacklist, u.url)) {
      printf("  blacklisted '%s'\n", u.url.c_str());
      continue;
    }

    auto host = util::get_host(u.url);
  
    printf("  insert %s -- %s\n", host.c_str(), u.url.c_str());
    
    auto index_site = index_find_host(index, host);
    if (index_site == NULL) {
      std::map<std::string, struct page> pages;

      struct page page = {"", u.count};

      std::string url(u.url);
     
      pages.emplace(url, page);

      struct site site = {host, level, false, 1, pages};

      index.push_back(site);

    } else {
      auto u_iter = index_site->pages.find(u.url);

      index_site->refs++;
    
      if (u_iter == index_site->pages.end()) {
        struct page page = {"", u.count};
        std::string url(u.url);
        index_site->pages.emplace(url, page);

      } else {
        u_iter->second.refs += u.count;
      }
    }
  }
}

void print_index(std::ofstream &file, std::vector<struct site> &index)
{
  printf("index\n");

  for (auto &site: index) {
    printf("    %s  at level %i with %i refs\n", 
        site.host.c_str(), site.level, site.refs);
    
    bool has_pages = false;
    for (auto &p: site.pages) {
      if (p.second.path.empty()) continue;

      has_pages = true;
      break;
    }

    if (!has_pages) continue;

    file << site.host << "\t";
    file << site.level << "\n";

    for (auto &p: site.pages) {
      if (p.second.path.empty()) continue;

      printf("\t\t%s\t%s\t%i\n", p.first.c_str(), 
            p.second.path.c_str(),
            p.second.refs);

      file << "\t";
      file << p.first << "\t";
      file << p.second.path << "\t";
      file << p.second.refs << "\n";
    }
  }
}

void save_index(std::vector<struct site> &index, std::string path)
{
  std::ofstream file;
  
  printf("save index %lu -> %s\n", index.size(), path.c_str());

  file.open(path, std::ios::out | std::ios::trunc);

  if (!file.is_open()) {
    fprintf(stderr, "error opening file %s\n", path.c_str());
    return;
  }

  print_index(file, index);

  file.close();
}

void run_round(int level, int max_sites, int max_pages,
  std::vector<struct site> &index,
  std::vector<std::string> &blacklist)
{
  printf("run round %i\n", level);

  std::vector<std::string> hosts;

  for (auto &site: index) {
    if (site.scraped) continue;
    hosts.push_back(std::string(site.host));
  }

  std::sort(hosts.begin(), hosts.end(), 
      [&index](std::string &host_a, std::string &host_b) {
 
        auto site_a = index_find_host(index, host_a);
        if (site_a == NULL) {
          return false;
        }

        auto site_b =  index_find_host(index, host_b);
        if (site_b == NULL) {
          return true;
        }

        return site_a->refs > site_b->refs;
      });

  int site_count = 0;
  for (auto &host: hosts) {
    if (max_sites > 0 && ++site_count >= max_sites) {
      printf("reached max sites\n");
      break;
    }

    auto site = index_find_host(index, host);
    if (site == NULL) {
      printf("site %s not found in index.\n", host.c_str());
      exit(1);
    }
   
    printf("scrape host '%s' with %i refs at level %i\n", 
        host.c_str(), site->refs, site->level);

    // TODO: should skipped and revisisted sites get to use their 
    // original level?
 
    if (site->level != level) {
      printf("interestingly, '%s' was previously skipped\n", host.c_str());
    }
    
    std::vector<struct index_url> url_index;
    std::vector<struct other_url> url_other;;

    std::vector<std::string> urls;

    for (auto &p: site->pages) {
      urls.push_back(p.first);
    }

    scrape(max_pages, host, urls, url_index, url_other);

    site->scraped = true;

    insert_site_index(index, host, url_index);
    
    insert_site_other(index, level + 1, url_other, blacklist);
 
    // Save the current index so exiting early doesn't loose
    // all the work that has been done
    save_index(index, "full_index");
  }
}

int main(int argc, char *argv[]) {
  std::vector<std::string> blacklist = util::load_list("../mine/blacklist");
  std::vector<std::string> initial_seed = util::load_list("../mine/seed");

  std::vector<struct other_url> seed_other;
  for (auto &u: initial_seed) {
    struct other_url i = {1, u};
    seed_other.push_back(i);
  }

  std::vector<struct site> index;

  struct level {
    int max_sites;
    int max_pages;
  };

  std::vector<struct level> levels = {{0, 30}, {10, 20}, {10, 10}, {10, 5}, {10, 1}};
  int level_count = 1;

  insert_site_other(index, level_count, seed_other, blacklist);

  save_index(index, "full_index");

  for (auto level: levels) {
    run_round(level_count++, level.max_sites, level.max_pages, 
        index, blacklist);
  }

  return 0;
}
    
