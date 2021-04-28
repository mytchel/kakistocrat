#include <string.h>
#include <math.h>
#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <signal.h>

#include <sys/stat.h>
#include <sys/types.h>

#include <curl/curl.h>

#include <list>
#include <vector>
#include <set>
#include <map>
#include <string>
#include <iostream>
#include <fstream>
#include <sstream>
#include <algorithm>

#include <chrono>
#include <thread>

#include "spdlog/spdlog.h"

using namespace std::chrono_literals;

#include "util.h"
#include "scrape.h"
#include "tokenizer.h"

namespace scrape {

size_t curl_cb_header_write(char *buffer, size_t size, size_t nitems, void *ctx) {
  site_op_page *op = (site_op_page *) ctx;

  buffer[nitems*size] = 0;

  if (strstr(buffer, "content-type:")) {
    if (strstr(buffer, "text/html") == NULL &&
        strstr(buffer, "text/plain") == NULL) {
      return 0;
    }

  } else if (op->m_page->last_scanned && strstr(buffer, "Last-Modified: ")) {
    char *s = buffer + strlen("Last-Modified: ");

    if (strlen(s) > 25) {
      tm tm;
      strptime(s, "%a, %d %b %Y %H:%M:%S", &tm);
      time_t time = mktime(&tm);

      if (op->m_page->last_scanned > time) {
        op->unchanged = true;
        return 0;
      }
    }
  }

  return nitems * size;
}

size_t curl_cb_buffer_write(void *contents, size_t sz, size_t nmemb, void *ctx)
{
  site_op *op = (site_op *) ctx;
  size_t realsize = sz * nmemb;

  if (op->buf_max < op->size + realsize) {
    return 0;
  }

  memcpy(&(op->buf[op->size]), contents, realsize);
  op->size += realsize;

  return realsize;
}

void setup_handle_page(site_op_page *op, CURL *curl_handle)
{
  spdlog::info("setup page   {}", op->m_page->url);

  curl_easy_setopt(curl_handle, CURLOPT_HEADERFUNCTION, curl_cb_header_write);
  curl_easy_setopt(curl_handle, CURLOPT_HEADERDATA, op);

  curl_easy_setopt(curl_handle, CURLOPT_MAXREDIRS, 5L);
}

void setup_handle_robots(site_op_robots *op, CURL *curl_handle)
{
  spdlog::info("setup robots {}", op->m_site->host);

  curl_easy_setopt(curl_handle, CURLOPT_MAXREDIRS, 2L);
}

void setup_handle_sitemap(site_op_sitemap *op, CURL *curl_handle)
{
  spdlog::info("setup sitemap {}", op->url);

  curl_easy_setopt(curl_handle, CURLOPT_MAXREDIRS, 2L);
}

CURL *make_handle(site_op *op)
{
  CURL *curl_handle = curl_easy_init();

  curl_easy_setopt(curl_handle, CURLOPT_USERAGENT, "crawlycrawler");

  curl_easy_setopt(curl_handle, CURLOPT_PRIVATE, op);
  curl_easy_setopt(curl_handle, CURLOPT_WRITEFUNCTION, curl_cb_buffer_write);
  curl_easy_setopt(curl_handle, CURLOPT_WRITEDATA, op);

  curl_easy_setopt(curl_handle, CURLOPT_TIMEOUT, 60L);
  curl_easy_setopt(curl_handle, CURLOPT_CONNECTTIMEOUT, 30L);

  curl_easy_setopt(curl_handle, CURLOPT_FOLLOWLOCATION, 1);

  curl_easy_setopt(curl_handle, CURLOPT_HTTP_VERSION, CURL_HTTP_VERSION_2TLS);

  char c_url[util::max_url_len];
  strncpy(c_url, op->url.c_str(), sizeof(c_url));

  curl_easy_setopt(curl_handle, CURLOPT_URL, c_url);

  if (auto pop = dynamic_cast<site_op_page *>(op)) {
    setup_handle_page(pop, curl_handle);

  } else if (auto rop = dynamic_cast<site_op_robots *>(op)) {
    setup_handle_robots(rop, curl_handle);

  } else if (auto sop = dynamic_cast<site_op_sitemap *>(op)) {
    setup_handle_sitemap(sop, curl_handle);

  } else {
    throw std::runtime_error("unknown type");
  }

  return curl_handle;
}

void
scraper(int tid,
    Channel<site*> &in,
    Channel<site*> &out,
    Channel<bool> &stat,
    size_t max_sites,
    size_t max_con)
{
  spdlog::info("thread {} started with {} max concurrent connections", tid, max_con);

  size_t max_op = max_con / 2;

  CURLM *multi_handle = curl_multi_init();
  curl_multi_setopt(multi_handle, CURLMOPT_MAX_TOTAL_CONNECTIONS, max_op);
  curl_multi_setopt(multi_handle, CURLMOPT_PIPELINING, CURLPIPE_MULTIPLEX);

  std::list<site*> sites;
  std::list<site_op*> ops;

  auto last_accepting = std::chrono::system_clock::now() - 100s;

  while (true) {
    spdlog::debug("scrape loop");

    bool accepting = ops.size() < max_op && sites.size() < max_sites;

    if (accepting && last_accepting + 100ms < std::chrono::system_clock::now()) {
      spdlog::info("thread {} has {} ops for {} sites",
               tid, ops.size(), sites.size());

      true >> stat;
      last_accepting = std::chrono::system_clock::now();
    }

    if (!in.empty()) {
      site *s;
      s << in;

      if (s == NULL) {
        break;
      }

      spdlog::info("start scraping site {}", s->host);

      s->init_paths();
      sites.push_back(s);
    }

    auto s = sites.begin();
    while (s != sites.end() && ops.size() < max_op) {
      if ((*s)->finished()) {
        spdlog::debug("site finished {}", (*s)->host);

        *s >> out;
        s = sites.erase(s);

        continue;
      }

      while (ops.size() < max_op) {
        auto op = (*s)->get_next();
        if (!op) {
          break;
        }

        ops.emplace_back(*op);

        curl_multi_add_handle(multi_handle, make_handle(ops.back()));
      }

      s++;
    }

    if (ops.empty()) {
      std::this_thread::sleep_for(100ms);
      continue;
    }

    auto start = std::chrono::steady_clock::now();

    int still_running = 1;
    while (still_running && std::chrono::steady_clock::now() < start + 1s) {
      curl_multi_wait(multi_handle, NULL, 0, 1000, NULL);
      curl_multi_perform(multi_handle, &still_running);

      int msgs_left;
      CURLMsg *m = NULL;
      while ((m = curl_multi_info_read(multi_handle, &msgs_left))) {
        if (m->msg == CURLMSG_DONE) {
          CURL *handle = m->easy_handle;
          site_op *op;
          char *url_c;

          curl_easy_getinfo(handle, CURLINFO_PRIVATE, &op);
          curl_easy_getinfo(handle, CURLINFO_EFFECTIVE_URL, &url_c);

          std::string url(url_c);

          CURLcode res = m->data.result;

          if (res == CURLE_OK) {
            long res_status;
            curl_easy_getinfo(handle, CURLINFO_RESPONSE_CODE, &res_status);
            if (res_status == 200) {
              op->finish(url);
            } else {
              spdlog::warn("miss http {} : {}", res_status, url);
              op->finish_bad(true);
            }
          } else {
            if (res == CURLE_WRITE_ERROR) {
              op->finish_bad(false);
            } else {
              spdlog::warn("miss curl {} {} : {}", res,
                  curl_easy_strerror(res), url);

              op->finish_bad(true);
            }
          }

          ops.remove(op);
          delete op;

          curl_multi_remove_handle(multi_handle, handle);
          curl_easy_cleanup(handle);
        }
      }
    }
  }

  curl_multi_cleanup(multi_handle);
}

}

