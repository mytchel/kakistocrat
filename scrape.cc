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
  : m_site(path), output_dir(n_output),
    max_pages(n_max_pages),
    max_links(n_max_pages * 5),
    max_part_size(n_max_part_size),
    max_page_size(n_max_page_size)
{
  m_site.load();

  if (m_site.pages.size() >= max_pages) {
    spdlog::warn("scrape site given more pages than max pages");
  }

  for (auto &u: m_site.pages) {
    if (url_pending.size() == max_pages) {
      break;
    }

    url_pending.push_back(&u);
  }

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

site::site(site &&o)
  : m_site(std::move(o.m_site)),
    output_dir(std::move(o.output_dir)),
    max_pages(o.max_pages),
    max_links(o.max_links),
    max_part_size(o.max_part_size),
    max_page_size(o.max_page_size),
    url_pending(std::move(o.url_pending)),
    url_scanning(std::move(o.url_scanning)),
    url_scanned(std::move(o.url_scanned)),
    url_unchanged(std::move(o.url_unchanged)),
    url_bad(std::move(o.url_bad)),
    disallow_path(std::move(o.disallow_path)),
    getting_robots(o.getting_robots),
    got_robots(o.got_robots),
    sitemap_url_pending(std::move(o.sitemap_url_pending)),
    sitemap_url_getting(std::move(o.sitemap_url_getting)),
    sitemap_url_got(std::move(o.sitemap_url_got)),
    fail(o.fail),
    buf_max(o.buf_max),
    free_bufs(std::move(o.free_bufs)),
    using_bufs(std::move(o.using_bufs))
{
  spdlog::info("scrape::site moved '{}'", m_site.host);
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

  auto p = m_site.pages.begin();
  while (p != m_site.pages.end()) {
    if (util::has_prefix(p->url, path)) {
      p = m_site.pages.erase(p);
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

  if (m_site.pages.size() + 1 >= max_pages) {
    return;
  }

  if (m_site.find_page(url) != nullptr) {
    spdlog::warn("drop sitemap url {} as in pages (url) already", url);
    return;
  }

  auto p = make_path(output_dir, url);

  if (m_site.find_page_by_path(p) != nullptr) {
    spdlog::warn("drop sitemap url {} as in pages (path) already", url);
    return;
  }

  url_pending.push_back(m_site.add_page(url, p));
}

bool add_link(page *p, std::string &n) {
  for (auto &l: p->links) {
    if (l.first == n) {
      l.second++;
      return false;
    }
  }

  p->links.emplace_back(n, 1);
  return true;
}

// TODO: effective url check, put as alias.

void site::finish(
      page *url,
      std::vector<std::string> &links,
      std::string &title)
{
  spdlog::info("{} finished good {}", m_site.host, url->url);

  spdlog::debug("{} bad={} uc={} c={} s={} p={} finish {}",
      m_site.host,
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

    if (is_new && u_host == m_site.host && m_site.pages.size() + 1 < max_pages) {
      if (url_pending.size() > max_pages) {
        continue;
      }

      if (disallow_url(u)) {
        continue;
      }

      if (m_site.find_page(u) != nullptr) {
        continue;
      }

      auto p = make_path(output_dir, u);

      if (m_site.find_page_by_path(p) != nullptr) {
        continue;
      }

      url_pending.push_back(m_site.add_page(u, p));
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

  auto it = url_scanning.begin();
  while (it != url_scanning.end()) {
    auto u = *it;
    if (u->url == url->url) {
      url_scanned.splice(url_scanned.end(), url_scanning, it);
      break;
    } else {
      it++;
    }
  }
}

void site::finish_unchanged(page *url) {
  spdlog::info("{} finished unchanged {}", m_site.host, url->url);

  auto it = url_scanning.begin();
  while (it != url_scanning.end()) {
    auto u = *it;
    if (u->url == url->url) {
      url_unchanged.splice(url_unchanged.end(), url_scanning, it);
      break;
    } else {
      it++;
    }
  }
}

void site::finish_bad(page *url, bool actually_bad) {
  spdlog::info("{} finished bad {}", m_site.host, url->url);

  auto it = url_scanning.begin();
  while (it != url_scanning.end()) {
    auto u = *it;
    if (u->url == url->url) {
      url_bad.splice(url_bad.end(), url_scanning, it);
      break;
    } else {
      it++;
    }
  }

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
        spdlog::debug("{} get next robots", m_site.host);
        return new site_op_robots(this, buf, buf_max);
      } else {
        spdlog::debug("{} get next robots out of bufs", m_site.host);
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

      spdlog::debug("{} get next sitemap {}", m_site.host, url);
      return new site_op_sitemap(this, buf, buf_max, url);
    } else {
      spdlog::debug("{} get next sitemap out of bufs", m_site.host);
      return {};
    }
  }

  if (!sitemap_url_getting.empty()) {
    return {};
  }

  if (url_unchanged.size() + url_scanned.size() + url_scanning.size() >= max_pages) {
    spdlog::debug("{} over max", m_site.host);
    return {};

  } else if (url_pending.empty()) {
    spdlog::debug("{} no pending", m_site.host);
    return {};

  } else if (should_finish()) {
    spdlog::debug("{} should finish", m_site.host);
    return {};
  }

  auto buf = pop_buf();
  if (buf) {
    spdlog::debug("{} get next page {}", m_site.host, url_pending.front()->url);

    url_scanning.splice(url_scanning.end(),
      url_pending, url_pending.begin());

    return new site_op_page(this, buf, buf_max, url_scanning.back());

  } else {
    spdlog::debug("{} get next page out of bufs", m_site.host);
    return {};
  }
}

bool site::finished() {
  if (!got_robots) {
    spdlog::debug("{} not finished, no robots", m_site.host);
    return false;
  }

  if (!sitemap_url_getting.empty() || !sitemap_url_pending.empty()) {
    spdlog::debug("{} not finished, pending sitemaps", m_site.host);
    return false;
  }

  if (!url_scanning.empty()) {
    spdlog::debug("{} has active", m_site.host);
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

