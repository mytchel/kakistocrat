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

#include "util.h"
#include "scrape.h"

namespace scrape {

std::string make_path(const std::string &url) {
  auto host = util::get_host(url);
  if (host.empty()) {
    return "junk_path";
  }

  auto h = util::host_hash(host);

  auto path = util::get_path(url);
  auto path_parts = util::split_path(path);

  auto file_path = "scrape_output/" + h + "/" + host;

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

bool want_proto(std::string proto) {
  return proto.empty() || proto == "http" || proto == "https";
}

bool bad_suffix(std::string path) {
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

bool bad_prefix(std::string path) {
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
    std::list<index_url> &url_index,
    std::string url)
{
  for (auto &i: url_index) {
    if (i.url == url) {
      return true;
    }
  }

  return false;
}

bool index_check_path(
    std::list<index_url> &url_index,
    std::string path)
{
  for (auto &i: url_index) {
    if (i.path == path) {
      return true;
    }
  }

  return false;
}

void site::add_disallow(std::string path) {
  disallow_path.push_back(path);

  auto u = url_pending.begin();
  while (u != url_pending.end()) {
    if (util::has_prefix(u->url, path)) {
      u = url_pending.erase(u);
    } else {
      u++;
    }
  }
}

void site::process_sitemap_entry(
    std::string url, std::optional<time_t> lastmod)
{
  if (url.empty()) return;

  auto u = url_pending.begin();
  while (u != url_pending.end()) {
    if (u->url == url) {
      if (lastmod && *lastmod < u->last_scanned) {
        url_unchanged.push_back(*u);
        url_pending.erase(u);
      }

      return;
    }

    u++;
  }

  if (url_pending.size() > 4 * max_pages) {
    return;
  }

  auto p = make_path(url);

  if (index_check_path(url_scanned, p)) {
    return;
  }

  if (index_check_path(url_pending, p)) {
    return;
  }

  if (index_check_path(url_scanning, p)) {
    return;
  }

  if (index_check_path(url_unchanged, p)) {
    return;
  }

  url_pending.emplace_back(url, p);
}

void site::finish(
      index_url url,
      std::list<std::string> links,
      std::string title)
{
  for (auto &u: links) {
    auto u_host = util::get_host(u);
    if (u_host.empty()) continue;

    url.links.insert(u);

    if (u_host == host) {
      if (url_pending.size() > 4 * max_pages) {
        continue;
      }

      if (disallow_url(u)) {
        continue;
      }

      if (index_check(url_bad, u)) {
        continue;
      }

      if (index_check(url_scanned, u)) {
        continue;
      }

      if (index_check(url_pending, u)) {
        continue;
      }

      if (index_check(url_scanning, u)) {
        continue;
      }

      if (index_check(url_unchanged, u)) {
        continue;
      }

      auto p = make_path(u);

      if (index_check_path(url_scanned, p)) {
        continue;
      }

      if (index_check_path(url_pending, p)) {
        continue;
      }

      if (index_check_path(url_scanning, p)) {
        continue;
      }

      if (index_check_path(url_unchanged, p)) {
        continue;
      }

      url_pending.emplace_back(u, p);
    }
  }

  url.title.clear();

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

        url.title += ' ';
      } else {
        url.title += '&';
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
      url.title += c;
    }
  }

  url.last_scanned = time(NULL);
  url.ok = true;

  url_scanned.push_back(url);

  url_scanning.remove_if([&url](auto &u) { return u.url == url.url; });
}

void site::finish_unchanged(index_url url) {
  url.ok = true;

  url_unchanged.push_back(url);

  url_scanning.remove_if([url](auto u) {
        return u.url == url.url;
      });
}

void site::finish_bad(index_url url, bool actually_bad) {
  url.last_scanned = time(NULL);
  url.ok = false;

  url_bad.push_back(url);


  url_scanning.remove_if([url](auto u) {
        return u.url == url.url;
      });

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

  if (fail > 10 && fail > url_scanned.size() / 4) {
    return true;
  }

  return false;
}

std::optional<index_url> site::get_next() {
  if (url_unchanged.size() + url_scanned.size() + url_scanning.size() >= max_pages) {
    return {};

  } else if (should_finish()) {
    return {};
  }

  if (url_scanning.size() >= max_active) {
    return {};
  }

  if (url_pending.empty()) {
    return {};
  }

  auto u = url_pending.front();
  url_pending.pop_front();

  url_scanning.push_back(u);

  return u;
}

bool site::finished() {
  if (!url_scanning.empty()) {
    return false;
  }

  if (!sitemap_url_getting.empty() || !sitemap_url_pending.empty()) {
    return false;
  }

  return should_finish();
}

bool site::disallow_url(std::string u) {
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
    if (i.path == "") {
      i.path = make_path(i.url);
    }
  }
}

}

