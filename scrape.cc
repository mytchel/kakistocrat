#include <string.h>
#include <math.h>
#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include <sys/stat.h>
#include <sys/types.h>

#include <unistd.h>
#include <fcntl.h>

#include <list>
#include <vector>
#include <set>
#include <map>
#include <string>
#include <iostream>
#include <fstream>
#include <ctime>
#include <optional>

#include <chrono>
#include <thread>

#include "spdlog/spdlog.h"

#include "util.h"
#include "scrape.h"

namespace scrape {

site::site(
    const std::string &path,
    const std::string &n_output,
    size_t n_max_pages,
    size_t n_max_connections,
    size_t n_max_part_size,
    size_t n_max_page_size)
  : site_map(path), output_dir(n_output),
    max_pages(n_max_pages),
    max_links(n_max_pages * 5),
    max_part_size(n_max_part_size),
    max_page_size(n_max_page_size)
{
  load();

  if (pages.size() >= max_pages) {
    spdlog::warn("scrape site given more pages than max pages");
  }

  size_t id = 0;
  for (auto &u: urls) {
    url_id_map.emplace(u, id++);
  }

  for (auto &u: pages) {
    url_pending.push_back(&u);
  }

  url_pending.sort([] (auto a, auto b) {
    return a->url.size() < b->url.size();
  });
  
  // TODO: limit url pending size here

  init_paths();

  buf_max = n_max_page_size;

  for (size_t i = 0; i < n_max_connections; i++) {
    uint8_t *buf = (uint8_t *) malloc(buf_max);
    if (buf == nullptr) {
      throw std::bad_alloc();
    }

    free_bufs.push_back(buf);
  }
}

site::~site() {
  for (auto b: free_bufs) {
    free(b);
  }
}

std::string make_path(const std::string &output_dir, const std::string &url) {
  spdlog::info("make path '{}' '{}'", output_dir, url);

  auto host = util::get_host(url);
  if (host.empty()) {
    return "junk_path";
  }

  auto path = util::get_path(url);
  auto path_parts = util::split_path(path);

  auto file_path = output_dir;

  util::make_path(file_path);

  if (path_parts.empty()) {
    return file_path + "/index";
  }

  for (int i = 0; i < path_parts.size(); i++) {
    auto &part = path_parts[i];

    bool need_dir = i + 1 < path_parts.size();
    bool exists = false;

    auto p = file_path + "/" + part;

    struct stat s;
    if (stat(p.c_str(), &s) != -1) {

      bool is_dir = (s.st_mode & S_IFMT) == S_IFDIR;
      bool is_file = (s.st_mode & S_IFMT) == S_IFDIR;

      if (need_dir && !is_dir) {
        p = file_path + "/" + part + "_dir";
      } else if (!need_dir && is_dir) {
        p = file_path + "/" + part + "/index";
      }

      exists = stat(p.c_str(), &s) != -1;

    } else {
      exists = false;
    }

    file_path = p;

    if (!exists) {
      if (need_dir) {
        mkdir(file_path.c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);

      } else {
        int fd = creat(file_path.c_str(), S_IRUSR | S_IWUSR);
        if (fd > 0) {
          close(fd);
        }
      }
    }
  }

  return file_path;
}

bool want_proto(const std::string &proto) {
  return proto.empty() || proto == "http" || proto == "https";
}

bool bad_suffix(const std::string &path) {
  return
      util::has_suffix(path, "?share=twitter") ||
      util::has_suffix(path, ".txt") ||
      util::has_suffix(path, ".md") ||
      util::has_suffix(path, ".rss") ||
      util::has_suffix(path, ".apk") ||
      util::has_suffix(path, ".page") ||
      util::has_suffix(path, ".JPG") ||
      util::has_suffix(path, ".jpg") ||
      util::has_suffix(path, ".png") ||
      util::has_suffix(path, ".gif") ||
      util::has_suffix(path, ".svg") ||
      util::has_suffix(path, ".mov") ||
      util::has_suffix(path, ".mp3") ||
      util::has_suffix(path, ".mp4") ||
      util::has_suffix(path, ".flac") ||
      util::has_suffix(path, ".ogg") ||
      util::has_suffix(path, ".epub") ||
      util::has_suffix(path, ".tar") ||
      util::has_suffix(path, ".rar") ||
      util::has_suffix(path, ".zip") ||
      util::has_suffix(path, ".gz") ||
      util::has_suffix(path, ".tgz") ||
      util::has_suffix(path, ".xz") ||
      util::has_suffix(path, ".bz2") ||
      util::has_suffix(path, ".exe") ||
      util::has_suffix(path, ".crate") ||
      util::has_suffix(path, ".xml") ||
      util::has_suffix(path, ".csv") ||
      util::has_suffix(path, ".ppt") ||
      util::has_suffix(path, ".sheet") ||
      util::has_suffix(path, ".sh") ||
      util::has_suffix(path, ".py") ||
      util::has_suffix(path, ".js") ||
      util::has_suffix(path, ".asc") ||
      util::has_suffix(path, ".pdf");
}

bool bad_prefix(const std::string &path) {
  return
      util::has_prefix(path, "/api") ||
      util::has_prefix(path, "/signup") ||
      util::has_prefix(path, "/login") ||
      util::has_prefix(path, "/forgot") ||
      util::has_prefix(path, "/register") ||
      util::has_prefix(path, "/admin") ||
      util::has_prefix(path, "/signin") ||
      util::has_prefix(path, "/cart") ||
      util::has_prefix(path, "/checkout") ||
      util::has_prefix(path, "/forum") ||
      util::has_prefix(path, "/account") ||
      util::has_prefix(path, "/uploads") ||
      util::has_prefix(path, "/cgit") ||
      util::has_prefix(path, "/admin") ||
      util::has_prefix(path, "/wp-login");
}

bool index_check(
    std::list<page *> &url_index,
    const std::string &url)
{
  for (auto &i: url_index) {
    if (i->url == url) {
      return true;
    }
  }

  return false;
}

bool index_check_path(
    std::list<page *> &url_index,
    const std::string &path)
{
  for (auto &i: url_index) {
    if (i->path == path) {
      return true;
    }
  }

  return false;
}

void site::add_disallow(const std::string &path) {
  disallow_path.emplace(path);

  auto u = url_pending.begin();
  while (u != url_pending.end()) {
    if (util::has_prefix((*u)->url, path)) {
      u = url_pending.erase(u);
    } else {
      u++;
    }
  }

  /*
  // This will break things wont it?

  auto p = pages.begin();
  while (p != pages.end()) {
    if (util::has_prefix(p->url, path)) {
      p = pages.erase(p);
    } else {
      p++;
    }
  }
  */
}

void site::add_sitemap(const std::string &url) {
  auto count = sitemap_url_pending.size() + sitemap_url_getting.size() + sitemap_url_got.size();

  if (count >  max_pages / 10) {
    return;
  }

  if (sitemap_url_pending.find(url) != sitemap_url_pending.end()) {
    return;
  }

  if (sitemap_url_getting.find(url) != sitemap_url_getting.end()) {
    return;
  }

  if (sitemap_url_got.find(url) != sitemap_url_got.end()) {
    return;
  }

  sitemap_url_pending.insert(url);
}

void site::process_sitemap_entry(
    const std::string &url, std::optional<time_t> lastmod)
{
  if (url.empty()) return;

  // TODO: support compressed?
  if (!util::has_suffix(url, "xml")) {
    return;
  }

  auto u = url_pending.begin();
  while (u != url_pending.end()) {
    auto uu = *u;
    if (uu->url == url) {
      if (lastmod && *lastmod < uu->last_scanned) {
        url_unchanged.push_back(uu);
        url_pending.erase(u);
      }

      return;
    }

    u++;
  }

  if (pages.size() + 1 >= max_pages) {
    return;
  }

  if (find_page(url) != nullptr) {
    spdlog::warn("drop sitemap url {} as in pages (url) already", url);
    return;
  }

  auto p = make_path(output_dir, url);

  if (find_page_by_path(p) != nullptr) {
    spdlog::warn("drop sitemap url {} as in pages (path) already", url);
    return;
  }

  maybe_insert_new_pending(url, p);
}

bool site::add_link(page *p, const std::string &n) {
  auto it = url_id_map.find(n);
  if (it == url_id_map.end()) {
    size_t id = urls.size();

    url_id_map.emplace(n, id);
    urls.emplace_back(n);

    p->links.emplace_back(id, 1);
    return true;
  }
  
  size_t id = it->second;

  for (auto &l: p->links) {
    if (l.first == id) {
      l.second++;
      return false;
    }
  }

  p->links.emplace_back(id, 1);
  return true;
}

bool site::maybe_insert_new_pending(const std::string &u, const std::string &p) {
  if (url_pending.size() >= max_pages) {
    if (url_pending.back()->url.size() > u.size()) {
      url_pending.pop_back();
    } else {
      return false;
    }
  }

  url_pending.push_back(add_page(u, p));
  url_pending.sort([] (auto a, auto b) {
    return a->url.size() < b->url.size();
  });

  return true;
}

// TODO: effective url check, put as alias.

void site::finish(
      page *url,
      std::vector<std::string> &links,
      std::string &title)
{
  spdlog::info("{} finished good {}", host, url->url);

  spdlog::debug("{} bad={} uc={} c={} s={} p={} finish {}",
      host,
      url_bad.size(),
      url_unchanged.size(),
      url_scanned.size(),
      url_scanning.size(),
      url_pending.size(),
      url->url);

  for (auto &u: links) {
    auto u_host = util::get_host(u);
    if (u_host.empty()) continue;

    bool is_new = add_link(url, u);

    if (is_new && u_host == host && pages.size() + 1 < max_pages) {
      if (disallow_url(u)) {
        continue;
      }

      if (find_page(u) != nullptr) {
        continue;
      }

      auto p = make_path(output_dir, u);

      if (find_page_by_path(p) != nullptr) {
        continue;
      }

      maybe_insert_new_pending(u, p);
    }
  }

  url->title.clear();

  for (int i = 0; i < title.size(); i++) {
    char c = title[i];

    if (c == '\t' || c == '\n' || c == '\r' || c == '\v') {
      continue;
    }

    if (c == '&') {
      if (title[i+1] == '#') {
        while (i < title.size()) {
          if (title[i] == ';') {
            break;
          } else {
            i++;
          }
        }

        url->title += ' ';
      } else {
        url->title += '&';
      }

      continue;
    }

    if (('a' <= c && c <= 'z') ||
        ('A' <= c && c <= 'Z') ||
        ('0' <= c && c <= '9') ||
        (c == ' ' || c == '|') ||
        (c == '+' || c == ',') ||
        (c == '.' || c == '/') ||
        (c == '-' || c == '_') ||
        (c == '(' || c == ')')) {
      url->title += c;
    }
  }

  url->last_scanned = time(NULL);

  url_scanning.remove(url);
  url_scanned.push_back(url);
}

void site::finish_unchanged(page *url) {
  spdlog::info("{} finished unchanged {}", host, url->url);

  url_scanning.remove(url);
  url_unchanged.push_back(url);
}

void site::finish_bad(page *url, bool actually_bad) {
  spdlog::info("{} finished bad {}", host, url->url);

  url_scanning.remove(url);
  url_bad.push_back(url);

  if (actually_bad) {
    fail++;
  }
}

bool site::should_finish() {
  if (url_pending.empty()) {
    return true;
  }

  if (url_unchanged.size() + url_scanned.size() >= max_pages) {
    return true;
  }

  if (fail > 10 && fail > url_scanned.size() / 2) {
    return true;
  }

  return false;
}

std::optional<site_op *> site::get_next() {
  if (!got_robots) {
    if (getting_robots) {
      return {};
    } else {
      auto buf = pop_buf();
      if (buf) {
        getting_robots = true;
        spdlog::debug("{} get next robots", host);
        return new site_op_robots(this, buf, buf_max);
      } else {
        spdlog::debug("{} get next robots out of bufs", host);
        return {};
      }
    }
  }

  if (!sitemap_url_pending.empty()) {
    auto buf = pop_buf();
    if (buf) {
      std::string url = *sitemap_url_pending.begin();

      sitemap_url_pending.erase(url);
      sitemap_url_getting.insert(url);

      spdlog::debug("{} get next sitemap {}", host, url);
      return new site_op_sitemap(this, buf, buf_max, url);
    } else {
      spdlog::debug("{} get next sitemap out of bufs", host);
      return {};
    }
  }

  if (!sitemap_url_getting.empty()) {
    return {};
  }

  if (url_unchanged.size() + url_scanned.size() + url_scanning.size() >= max_pages) {
    spdlog::debug("{} over max", host);
    return {};

  } else if (url_pending.empty()) {
    spdlog::debug("{} no pending", host);
    return {};

  } else if (should_finish()) {
    spdlog::debug("{} should finish", host);
    return {};
  }

  auto buf = pop_buf();
  if (buf) {
    auto page = url_pending.front();
    url_pending.pop_front();
    url_scanning.push_back(page);

    spdlog::debug("{} get next page {}", host, page->url);

    return new site_op_page(this, buf, buf_max, page);

  } else {
    spdlog::debug("{} get next page out of bufs", host);
    return {};
  }
}

bool site::finished() {
  if (!got_robots) {
    spdlog::debug("{} not finished, no robots", host);
    return false;
  }

  if (!sitemap_url_getting.empty() || !sitemap_url_pending.empty()) {
    spdlog::debug("{} not finished, pending sitemaps", host);
    return false;
  }

  if (!url_scanning.empty()) {
    spdlog::debug("{} has active", host);
    return false;
  }

  return should_finish();
}

bool site::disallow_url(const std::string &u) {
  auto path = util::get_path(u);

  for (auto disallow: disallow_path) {
    if (util::has_prefix(path, disallow)) {
      return true;
    }
  }

  return false;
}

void site::init_paths() {
  for (auto &i: url_pending) {
    if (i->path == "") {
      i->path = make_path(output_dir, i->url);
    }
  }
}

}

