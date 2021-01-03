#include <string.h>
#include <math.h>
#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include <sys/stat.h>
#include <sys/types.h>

#include <curl/curl.h>
#include "lexbor/html/html.h"
#include <lexbor/dom/dom.h>

#include <list>
#include <vector>
#include <set>
#include <map>
#include <string>
#include <iostream>
#include <fstream>

#include <chrono>
#include <thread>

using namespace std::chrono_literals;

#include "util.h"
#include "scrape.h"

namespace scrape {

/* resizable buffer */
typedef struct {
  char *buf;
  size_t max, size;
} memory;

extern "C" {

size_t grow_buffer(void *contents, size_t sz, size_t nmemb, void *ctx)
{
  memory *mem = (memory*) ctx;
  size_t realsize = sz * nmemb;

  if (mem->max < mem->size + realsize) {
    return 0;
  }

  memcpy(&(mem->buf[mem->size]), contents, realsize);
  mem->size += realsize;

  return realsize;
}

size_t header_callback(char *buffer, size_t size, size_t nitems, void *userdata) {
  buffer[nitems*size] = 0;
  if (strstr(buffer, "content-type:")) {
    if (strstr(buffer, "text/html") == NULL) {
      return 0;
    }
  }

  return nitems * size;
}

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

std::list<std::string> find_links_lex(
      lxb_html_parser_t *parser,
      memory *mem,
      std::string page_url)
{
  std::list<std::string> urls;

  lxb_status_t status;
  lxb_dom_element_t *element;
  lxb_html_document_t *document;
  lxb_dom_collection_t *collection;

  document = lxb_html_parse(parser, (const lxb_char_t *) mem->buf, mem->size);
  if (document == NULL) {
    printf("Failed to create Document object\n");
    return urls;
  }

  collection = lxb_dom_collection_make(&document->dom_document, 128);
  if (collection == NULL) {
    printf("Failed to create Collection object");
    exit(1);
  }

  if (document->body == NULL) {
    return urls;
  }

  status = lxb_dom_elements_by_tag_name(lxb_dom_interface_element(document->body),
                                        collection,
                                        (const lxb_char_t *) "a", 1);
  if (status != LXB_STATUS_OK) {
      printf("Failed to get elements by name\n");
      exit(1);
  }

  auto page_proto = util::get_proto(page_url);
  auto page_host = util::get_host(page_url);
  auto page_dir = util::get_dir(util::get_path(page_url));

  char attr_name[] = "href";
  size_t attr_len = 4;
  for (size_t i = 0; i < lxb_dom_collection_length(collection); i++) {
      element = lxb_dom_collection_element(collection, i);

      size_t len;
      char *s = (char *) lxb_dom_element_get_attribute(
            element,
            (const lxb_char_t*) attr_name,
            attr_len,
            &len);

      if (s == NULL) {
        continue;
      }

      // http://
      // https://
      // #same-page skip
      // /from-root
      // from-current-dir
      // javascript: skip
      // //host/page keep protocol

      std::string url(s);

      if (url.empty() || url.front() == '#')
        continue;

      if (!util::bare_minimum_valid_url(url))
        continue;

      auto proto = util::get_proto(url);
      if (proto.empty()) {
        proto = page_proto;

      } else if (!want_proto(proto))  {
        continue;
      }

      auto host = util::get_host(url);
      if (host.empty()) {
        host = page_host;
      }

      auto path = util::get_path(url);

      if (bad_suffix(path))
        continue;

      if (bad_prefix(path))
        continue;

      if (!path.empty() && path.front() != '/') {
        path = page_dir + "/" + path;
      }

      auto fixed = proto + "://" + host + path;
      urls.push_back(fixed);
  }

  lxb_dom_collection_destroy(collection, true);
  lxb_html_document_destroy(document);

  return urls;
}

void save_file(std::string path, std::string url, memory *mem)
{
  std::ofstream file;

  file.open(path, std::ios::out | std::ios::binary | std::ios::trunc);

  if (!file.is_open()) {
    fprintf(stderr, "error opening file %s for %s\n", path.c_str(), url.c_str());
    return;
  }

  file.write(mem->buf, mem->size);

  file.close();
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

void insert_urls(std::string host,
      index_url &url,
      std::list<std::string> urls,
      std::list<struct index_url> &url_index,
      std::list<std::string> &url_bad,
      std::list<struct index_url> &url_scanning)
{
  for (auto &u: urls) {
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

      if (index_check(url_index, u)) {
        continue;
      }

      if (index_check(url_scanning, u)) {
        continue;
      }

      auto p = util::make_path(u);

      if (index_check_path(url_scanning, p)) {
        continue;
      }

      if (index_check_path(url_index, p)) {
        continue;
      }

      struct index_url i = {u, p};

      url_scanning.push_back(i);
    }
  }
}

struct index_url pick_next(std::list<struct index_url> &urls) {
  auto best = urls.begin();

  for (auto u = urls.begin(); u != urls.end(); u++) {
    // TODO: base on count too
    if ((*u).url.length() < (*best).url.length()) {
      best = u;
    }
  }

  struct index_url r(*best);
  urls.erase(best);
  return r;
}

void
scrape(int max_pages,
    const std::string host,
    std::list<struct index_url> &url_index)
{
  std::list<index_url> url_scanning(url_index);
  std::list<std::string> url_bad;

  url_index.clear();

  lxb_status_t status;
  lxb_html_parser_t *parser;
  parser = lxb_html_parser_create();
  status = lxb_html_parser_init(parser);

  if (status != LXB_STATUS_OK) {
    printf("Failed to create HTML parser\n");
    exit(1);
  }

  CURL *curl_handle;
  CURLcode res;

  int fail_net = 0;
  int fail_web = 0;

  size_t max_size = 1024 * 1024 * 10;
  char *c = (char *) malloc(max_size);
  memory mem{c, max_size, 0};

  curl_handle = curl_easy_init();

  curl_easy_setopt(curl_handle, CURLOPT_WRITEFUNCTION, grow_buffer);
  curl_easy_setopt(curl_handle, CURLOPT_WRITEDATA, &mem);
  curl_easy_setopt(curl_handle, CURLOPT_HEADERFUNCTION, header_callback);
  curl_easy_setopt(curl_handle, CURLOPT_USERAGENT, "libcurl-agent/1.0");
  curl_easy_setopt(curl_handle, CURLOPT_FOLLOWLOCATION, 1);
  curl_easy_setopt(curl_handle, CURLOPT_TIMEOUT, 10);

  // Potentially stops issues but doesn't seem to change much.
  curl_easy_setopt(curl_handle, CURLOPT_NOSIGNAL, 1L);


  while (!url_scanning.empty()) {
    if (url_index.size() >= max_pages) {
      printf("%s reached max pages\n", host.c_str());
      break;
    }

    if (fail_net > 10 && fail_net > 1 + url_index.size() / 4) {
      printf("%s reached max fail net %i / %lu\n", host.c_str(),
          fail_net, url_index.size());
      break;
    }

    if (fail_web > 10 && fail_web > 1 + url_index.size() / 2) {
      printf("%s reached max fail web %i / %lu\n", host.c_str(),
          fail_web, url_index.size());
      break;
    }

    auto u = pick_next(url_scanning);

    char s[util::max_url_len];
    strcpy(s, u.url.c_str());
    curl_easy_setopt(curl_handle, CURLOPT_URL, s);

    mem.size = 0;
    res = curl_easy_perform(curl_handle);
    if (res == CURLE_OK) {
      long res_status;
      curl_easy_getinfo(curl_handle, CURLINFO_RESPONSE_CODE, &res_status);

      if (res_status == 200) {
        char *ctype;
        curl_easy_getinfo(curl_handle, CURLINFO_CONTENT_TYPE, &ctype);

        if (mem.size > 10) {
          auto urls = find_links_lex(parser, &mem, u.url);

          save_file(u.path, u.url, &mem);

          if (url_index.size() > 0 && url_index.size() % 50 == 0) {
            printf("%s %lu / %lu (max %lu) with %lu failures\n",
                host.c_str(),
                url_index.size(),
                url_scanning.size(),
                max_pages,
                url_bad.size());
          }

          insert_urls(host, u, urls, url_index, url_bad, url_scanning);

          url_index.push_back(u);

        } else {
          printf("miss '%s' %lu %s\n", ctype, mem.size, u.url.c_str());
          url_bad.push_back(u.url);
        }

      } else {
        switch ((int) res_status) {
          case 404:
          case 301:
            break;
          default:
            printf("miss %d %s\n", (int) res_status, u.url.c_str());
            fail_web++;
            break;
        }

        url_bad.push_back(u.url);
      }

    } else {
      url_bad.push_back(u.url);

      if (res != CURLE_WRITE_ERROR) {
        printf("miss %s %s\n", curl_easy_strerror(res), u.url.c_str());
        fail_net++;
      }
    }
  }

  curl_easy_cleanup(curl_handle);
  free(mem.buf);

  lxb_html_parser_destroy(parser);
}
}


