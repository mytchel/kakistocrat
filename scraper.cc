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

#include "util.h"
#include "scrape.h"
#include "scraper.h"

namespace scrape {

std::list<std::string> find_links_lex(
      lxb_html_parser_t *parser,
      curl_buffer *buf,
      std::string page_url)
{
  std::list<std::string> urls;

  lxb_status_t status;
  lxb_dom_element_t *element;
  lxb_html_document_t *document;
  lxb_dom_collection_t *collection;

  document = lxb_html_parse(parser, (const lxb_char_t *) buf->buf, buf->size);
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

void
scrape(site *site)
{
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

  size_t max_size = 1024 * 1024 * 10;
  char *c = (char *) malloc(max_size);
  curl_buffer buf(c, max_size);

  curl_handle = curl_easy_init();

  curl_easy_setopt(curl_handle, CURLOPT_WRITEFUNCTION, curl_cb_buffer_write);
  curl_easy_setopt(curl_handle, CURLOPT_WRITEDATA, &buf);
  curl_easy_setopt(curl_handle, CURLOPT_HEADERFUNCTION, curl_cb_header_write);
  curl_easy_setopt(curl_handle, CURLOPT_USERAGENT, "libcurl-agent/1.0");
  curl_easy_setopt(curl_handle, CURLOPT_FOLLOWLOCATION, 1);
  curl_easy_setopt(curl_handle, CURLOPT_TIMEOUT, 10);

  // Potentially stops issues but doesn't seem to change much.
  curl_easy_setopt(curl_handle, CURLOPT_NOSIGNAL, 1L);

  while (!site->finished()) {
    auto u = site->pop_next();

    char s[util::max_url_len];
    strcpy(s, u.url.c_str());
    curl_easy_setopt(curl_handle, CURLOPT_URL, s);

    buf.reset();

    res = curl_easy_perform(curl_handle);
    if (res == CURLE_OK) {
      long res_status;
      curl_easy_getinfo(curl_handle, CURLINFO_RESPONSE_CODE, &res_status);

      if (res_status == 200) {
        char *ctype;
        curl_easy_getinfo(curl_handle, CURLINFO_CONTENT_TYPE, &ctype);

        auto urls = find_links_lex(parser, &buf, u.url);

        save_file(u.path, u.url, &buf);

        site->finish(u, urls);

      } else {
        site->finish_bad_http(u, (int) res_status);
      }

    } else {
      bool bad = false;
      if (res != CURLE_WRITE_ERROR) {
        printf("miss %s %s\n", curl_easy_strerror(res), u.url.c_str());
        bad = true;
      }

      site->finish_bad_net(u, bad);
    }
  }

  curl_easy_cleanup(curl_handle);
  free(buf.buf);

  lxb_html_parser_destroy(parser);
}
}


