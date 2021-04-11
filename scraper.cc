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

  op->setup_handle(curl_handle);

  return curl_handle;
}

void
scraper(int tid,
    Channel<site*> &in,
    Channel<site*> &out,
    Channel<bool> &stat,
    size_t max_con)
{
  spdlog::info("thread {} started with {} max concurrent connections", tid, max_con);

  CURLM *multi_handle = curl_multi_init();
  curl_multi_setopt(multi_handle, CURLMOPT_MAX_TOTAL_CONNECTIONS, max_con);
  curl_multi_setopt(multi_handle, CURLMOPT_PIPELINING, CURLPIPE_MULTIPLEX);

  std::list<site*> sites;
  std::list<site_op*> ops;

  size_t max_op = max_con / 2;

  auto last_accepting = std::chrono::system_clock::now() - 100s;

  while (true) {
    bool accepting = ops.size() < max_op;

    if (accepting && last_accepting + 5s < std::chrono::system_clock::now()) {
      spdlog::info("thread {} has {} ops for {} sites",
               tid, ops.size(), sites.size());

      true >> stat;
      last_accepting = std::chrono::system_clock::now();
    }

    bool adding = true;
    while (ops.size() < max_op && adding) {
      adding = false;
      auto s = sites.begin();
      while (s != sites.end()) {
        if ((*s)->finished()) {
          *s >> out;
          s = sites.erase(s);
          continue;

        } else {
          auto op = (*s)->get_next();
          if (op) {
            ops.emplace_back(*op);

            curl_multi_add_handle(multi_handle, make_handle(ops.back()));
            adding = true;
          }
        }

        s++;
      }
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

    if (sites.empty()) {
      std::this_thread::sleep_for(100ms);
    }

    curl_multi_wait(multi_handle, NULL, 0, 1000, NULL);
    int still_running = 0;
    curl_multi_perform(multi_handle, &still_running);

    int msgs_left;
    CURLMsg *m = NULL;
    while ((m = curl_multi_info_read(multi_handle, &msgs_left))) {
      if (m->msg == CURLMSG_DONE) {
        CURL *handle = m->easy_handle;
        site_op *op;
        char *url;

        curl_easy_getinfo(handle, CURLINFO_PRIVATE, &op);
        curl_easy_getinfo(handle, CURLINFO_EFFECTIVE_URL, &url);

        CURLcode res = m->data.result;

        if (res == CURLE_OK) {
          long res_status;
          curl_easy_getinfo(handle, CURLINFO_RESPONSE_CODE, &res_status);
          if (res_status == 200) {
            op->finish(std::string(url));
          } else {
            op->finish_bad(CURLE_OK, (int) res_status);
          }
        } else {
          op->finish_bad(res, 0);
        }

        ops.remove(op);
        delete op;

        curl_multi_remove_handle(multi_handle, handle);
        curl_easy_cleanup(handle);
      }
    }
  }

  curl_multi_cleanup(multi_handle);
}

}

