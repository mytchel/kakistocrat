#include <string.h>
#include <math.h>
#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include <sys/stat.h>
#include <sys/types.h>

#include <list>
#include <vector>
#include <set>
#include <map>
#include <string>
#include <iostream>
#include <fstream>

#include <chrono>
#include <thread>

#include "util.h"
#include "scrape.h"

namespace scrape {

void curl_data::save()
{
  std::ofstream file;

  if (buf == NULL) {
    printf("save '%s' but no buffer\n", url.url.c_str());
    return;
  }

  file.open(url.path, std::ios::out | std::ios::binary | std::ios::trunc);

  if (!file.is_open()) {
    fprintf(stderr, "error opening file %s for %s\n", url.path.c_str(), url.url.c_str());
    return;
  }

  file.write(buf, size);

  file.close();
}

size_t curl_cb_buffer_write(void *contents, size_t sz, size_t nmemb, void *ctx)
{
  curl_data *buf = (curl_data *) ctx;
  size_t realsize = sz * nmemb;

  if (buf->max < buf->size + realsize) {
    return 0;
  }

  if (buf->buf == NULL) {
    buf->buf = (char *) malloc(buf->max);
    if (buf->buf == NULL) {
      return 0;
    }
  }

  memcpy(&(buf->buf[buf->size]), contents, realsize);
  buf->size += realsize;

  return realsize;
}

size_t curl_cb_header_write(char *buffer, size_t size, size_t nitems, void *userdata) {
  buffer[nitems*size] = 0;
  if (strstr(buffer, "content-type:")) {
    if (strstr(buffer, "text/html") == NULL) {
      return 0;
    }
  }

  return nitems * size;
}

bool has_suffix(std::string const &s, std::string const &suffix) {
  if (s.length() >= suffix.length()) {
    return (0 == s.compare(s.length() - suffix.length(), suffix.length(), suffix));
  } else {
    return false;
  }
}

bool has_prefix(std::string const &s, std::string const &prefix) {
  if (s.length() >= prefix.length()) {
    return (0 == s.compare(0, prefix.length(), prefix));
  } else {
    return false;
  }
}

bool want_proto(std::string proto) {
  return proto.empty() || proto == "http" || proto == "https";
}

bool bad_suffix(std::string path) {
  return
      has_suffix(path, "?share=twitter") ||
      has_suffix(path, ".txt") ||
      has_suffix(path, ".md") ||
      has_suffix(path, ".rss") ||
      has_suffix(path, ".apk") ||
      has_suffix(path, ".page") ||
      has_suffix(path, ".JPG") ||
      has_suffix(path, ".jpg") ||
      has_suffix(path, ".png") ||
      has_suffix(path, ".gif") ||
      has_suffix(path, ".svg") ||
      has_suffix(path, ".mov") ||
      has_suffix(path, ".mp3") ||
      has_suffix(path, ".mp4") ||
      has_suffix(path, ".flac") ||
      has_suffix(path, ".ogg") ||
      has_suffix(path, ".epub") ||
      has_suffix(path, ".tar") ||
      has_suffix(path, ".rar") ||
      has_suffix(path, ".zip") ||
      has_suffix(path, ".gz") ||
      has_suffix(path, ".tgz") ||
      has_suffix(path, ".xz") ||
      has_suffix(path, ".bz2") ||
      has_suffix(path, ".exe") ||
      has_suffix(path, ".crate") ||
      has_suffix(path, ".xml") ||
      has_suffix(path, ".csv") ||
      has_suffix(path, ".ppt") ||
      has_suffix(path, ".sheet") ||
      has_suffix(path, ".sh") ||
      has_suffix(path, ".py") ||
      has_suffix(path, ".js") ||
      has_suffix(path, ".asc") ||
      has_suffix(path, ".pdf");
}

bool bad_prefix(std::string path) {
  return
      has_prefix(path, "/signup") ||
      has_prefix(path, "/login") ||
      has_prefix(path, "/forgot") ||
      has_prefix(path, "/register") ||
      has_prefix(path, "/admin") ||
      has_prefix(path, "/signin") ||
      has_prefix(path, "/cart") ||
      has_prefix(path, "/checkout") ||
      has_prefix(path, "/forum") ||
      has_prefix(path, "/account") ||
      has_prefix(path, "/uploads") ||
      has_prefix(path, "/cgit") ||
      has_prefix(path, "/admin") ||
      has_prefix(path, "/wp-login");
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

void site::finish(
      index_url url,
      std::list<std::string> links)
{
  for (auto &u: links) {
    auto u_host = util::get_host(u);
    if (u_host.empty()) continue;

    url.links.insert(u);

    if (u_host == host) {
      bool bad = false;
      for (auto &b: url_bad) {
        if (b == u) {
          bad = true;
          break;
        }
      }

      if (bad) continue;

      if (index_check(url_scanned, u)) {
        continue;
      }

      if (index_check(url_scanning, u)) {
        continue;
      }

      auto p = util::make_path(u);

      if (index_check_path(url_scanned, p)) {
        continue;
      }

      if (index_check_path(url_scanning, p)) {
        continue;
      }

      url_scanning.emplace_back(u, p);
    }
  }

  active--;
  url_scanned.push_back(url);
}

void site::finish_bad_http(index_url u, int code) {
  active--;
  url_bad.push_back(u.url);

  switch (code) {
    case 404:
    case 301:
      break;
    default:
      printf("miss %d %s\n", code, u.url.c_str());
      fail_web++;
      break;
  }
}

void site::finish_bad_net(index_url u, bool actually_bad) {
  active--;
  url_bad.push_back(u.url);
  if (actually_bad) {
    fail_net++;
  }
}

index_url site::pop_next() {
  auto best = url_scanning.begin();

  // TODO: keep the list sorted?
  for (auto u = url_scanning.begin(); u != url_scanning.end(); u++) {
    // TODO: base on count too
    if ((*u).url.length() < (*best).url.length()) {
      best = u;
    }
  }

  active++;

  index_url r(*best);
  url_scanning.erase(best);
  return r;
}

bool site::finished() {
  if (active > 0) {
    return false;
  }

  if (url_scanning.empty()) {
    return true;
  }

  if (url_scanned.size() >= max_pages) {
    printf("%s reached max pages\n", host.c_str());
    return true;
  }

  if (fail_net > 10 && fail_net > 1 + url_scanned.size() / 4) {
    printf("%s reached max fail net %i / %lu\n", host.c_str(),
        fail_net, url_scanned.size());
    return true;
  }

  if (fail_web > 10 && fail_web > 1 + url_scanned.size() / 2) {
    printf("%s reached max fail web %i / %lu\n", host.c_str(),
        fail_web, url_scanned.size());
    return true;
  }

  return false;
}

}

