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

using namespace std::chrono_literals;

#include "util.h"
#include "scrape.h"
#include "tokenizer.h"

namespace scrape {

enum request_type { URL, ROBOTS, SITEMAP };

static int active_malloc = 0;

const size_t max_data = 1024 * 1024 * 10;

struct curl_data {
  bool in_use{false};

  char *buf{NULL};

  site *m_site;
  request_type req_type;
  index_url url;
  std::string s_url;

  size_t size;
  bool unchanged;

  void init_page(site *s, index_url u) {
    in_use = true;

    m_site = s;
    req_type = URL;
    url = u;

    unchanged = false;
    size = 0;
  }

  void init_other(site *s, request_type t, std::string ss) {
    in_use = true;

    m_site = s;
    req_type = t;
    s_url = ss;

    size = 0;
  }

  curl_data() {};

  ~curl_data() {
    if (buf) {
      free(buf);
    }
  }

  void finish(std::string effective_url);
  void finish_bad_http(int code);
  void finish_bad_net(CURLcode res);

  void save();

  std::list<std::string> find_links(std::string page_url, std::string &title);
  void process_robots();
  void process_sitemap();
};

size_t curl_cb_buffer_write(void *contents, size_t sz, size_t nmemb, void *ctx)
{
  curl_data *d = (curl_data *) ctx;
  size_t realsize = sz * nmemb;

  if (max_data < d->size + realsize) {
    return 0;
  }

  if (d->buf == NULL) {
    d->buf = (char *) malloc(max_data);
    if (d->buf == NULL) {
      return 0;
    }
  }

  memcpy(&(d->buf[d->size]), contents, realsize);
  d->size += realsize;

  return realsize;
}

size_t curl_cb_header_write(char *buffer, size_t size, size_t nitems, void *ctx) {
  curl_data *d = (curl_data *) ctx;

  buffer[nitems*size] = 0;

  if (strstr(buffer, "content-type:")) {
    if (strstr(buffer, "text/html") == NULL &&
        strstr(buffer, "text/plain") == NULL) {
      return 0;
    }

  } else if (strstr(buffer, "Last-Modified: ")) {
    char *s = buffer + strlen("Last-Modified: ");

    if (strlen(s) > 25) {
      tm tm;
      strptime(s, "%a, %d %b %Y %H:%M:%S", &tm);
      time_t time = mktime(&tm);

      if (d->url.last_scanned > time) {
        d->unchanged = true;
        return 0;
      }
    }
  }

  return nitems * size;
}

void curl_data::save()
{
  std::ofstream file;

  if (buf == NULL) {
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

std::optional<std::string> process_link(
    std::string page_proto,
    std::string page_host,
    std::string page_dir,
    std::string url)
{
  // http://
  // https://
  // #same-page skip
  // /from-root
  // from-current-dir
  // javascript: skip
  // //host/page keep protocol

  if (url.empty() || url.front() == '#')
    return {};

  if (!util::bare_minimum_valid_url(url))
    return {};

  auto proto = util::get_proto(url);
  if (proto.empty()) {
    proto = page_proto;

  } else if (!want_proto(proto))  {
    return {};
  }

  auto host = util::get_host(url);
  if (host.empty()) {
    host = page_host;
  }

  auto path = util::get_path(url);

  if (bad_suffix(path))
    return {};

  if (bad_prefix(path))
    return {};

  // TODO: Pretty sure this is broken
  if (!path.empty() && path.front() != '/') {
    path = page_dir + "/" + path;
  }

  return proto + "://" + host + path;
}

std::list<std::string> curl_data::find_links(std::string page_url, std::string &title)
{
  std::list<std::string> urls;

  if (buf == NULL) {
    return urls;
  }

  char tok_buffer_store[1024];
	struct str tok_buffer;
	str_init(&tok_buffer, tok_buffer_store, sizeof(tok_buffer_store));

  tokenizer::token_type token;
  tokenizer::tokenizer tok;

  tok.init(buf, size);

  auto page_proto = util::get_proto(page_url);
  auto page_host = util::get_host(page_url);
  auto page_dir = util::get_dir(util::get_path(page_url));

  if (page_proto.empty() || page_host.empty() || page_dir.empty()) {
    printf("BAD PAGE URL '%s' : '%s' -> '%s' '%s' '%s'\n",
        url.url.c_str(), page_url.c_str(),
        page_proto.c_str(), page_host.c_str(), page_dir.c_str());
    return urls;
  }

  bool in_head = false;
  bool got_title = false;
  do {
    token = tok.next(&tok_buffer);

    if (token == tokenizer::TAG) {
      char tag_name[tokenizer::tag_name_max_len];
      tokenizer::get_tag_name(tag_name, str_c(&tok_buffer));

      if (strcmp(tag_name, "a") == 0) {
        char attr[tokenizer::attr_value_max_len];
        if (tokenizer::get_tag_attr(attr, "href", str_c(&tok_buffer))) {
          auto s = process_link(page_proto, page_host, page_dir, std::string(attr));
          if (s.has_value()) {
            urls.push_back(*s);
          }
        }

        // only do this in head.
        // https://whereismyspoon.co/category/main-dish/meat/
        // this page has svg's with titles.
        // also only take first
        // and make sure not in a sub tag?.
      } else if (strcmp(tag_name, "head") == 0) {
        in_head = true;
      } else if (strcmp(tag_name, "/head") == 0) {
        in_head = false;

      } else if (in_head && !got_title && strcmp(tag_name, "title") == 0) {
        tok.consume_until("</title>", &tok_buffer);
        title = std::string(str_c(&tok_buffer));
        got_title = true;
      }
    }

  } while (token != tokenizer::END);

  return urls;
}

void curl_data::process_robots() {
  m_site->disallow_path.clear();

  std::string file = std::string(buf, buf + size);

  std::istringstream fss(file);

  bool matching_useragent = true;

  std::string line;
  while (std::getline(fss, line, '\n')) {
    if (line.empty() || line[0] == '#') continue;

    line.erase(std::remove_if(line.begin(), line.end(),
            [](char c) { return std::isspace(static_cast<unsigned char>(c)); }),
        line.end());

    std::istringstream lss(line);

    std::string key, value;
    std::getline(lss, key, ':');
    std::getline(lss, value);

    if (key.empty() || value.empty()) continue;

    if (key == "User-agent" || key == "User-Agent") {
      if (value == "*") {
        matching_useragent = true;
      } else {
        matching_useragent = false;
      }

    } else if (key == "Disallow") {
      if (matching_useragent) {
        m_site->add_disallow(value);
      }

    } else if (key == "Sitemap") {
      m_site->sitemap_url_pending.insert(value);
    }
  }
}

void curl_data::process_sitemap() {
  if (buf == NULL) {
    return;
  }

  char tok_buffer_store[1024];
	struct str tok_buffer;
	str_init(&tok_buffer, tok_buffer_store, sizeof(tok_buffer_store));

  tokenizer::token_type token;
  tokenizer::tokenizer tok;

  tok.init(buf, size);

  std::optional<std::string> url_loc = {};
  std::optional<time_t> url_lastmod = {};

  bool in_url = false;
  bool in_sitemap = false;

  do {
    token = tok.next(&tok_buffer);

    if (token == tokenizer::TAG) {
      char tag_name[tokenizer::tag_name_max_len];
      tokenizer::get_tag_name(tag_name, str_c(&tok_buffer));

      if (strcmp(tag_name, "url") == 0) {
        if (url_loc) {
          auto url = process_link("https", m_site->host, "", *url_loc);
          if (url.has_value()) {
            m_site->process_sitemap_entry(*url, url_lastmod);
          }
        }

        url_loc = {};
        url_lastmod = {};

        in_url = true;
        in_sitemap = false;

      } else if (strcmp(tag_name, "sitemap") == 0) {

        in_url = false;
        in_sitemap = true;

      } else if (in_url) {
        if (strcmp(tag_name, "loc") == 0) {
          tok.load_tag_content(&tok_buffer);

          url_loc = std::string(str_c(&tok_buffer));

        } else if (strcmp(tag_name, "lastmod") == 0) {
          tok.load_tag_content(&tok_buffer);

          const char *s = str_c(&tok_buffer);
          if (strlen(s) > 10) {
            tm tm;
            strptime(s, "%Y-%m-%d", &tm);
            url_lastmod = mktime(&tm);
          }
        }

      } else if (in_sitemap) {
        if (strcmp(tag_name, "loc") == 0) {
          tok.load_tag_content(&tok_buffer);

          auto s = std::string(str_c(&tok_buffer));

          bool found = false;

          found |= m_site->sitemap_url_getting.find(s)
            != m_site->sitemap_url_getting.end();

          found |= m_site->sitemap_url_got.find(s)
            != m_site->sitemap_url_got.end();

          if (!found && m_site->sitemap_count < 5) {
            m_site->sitemap_count++;
            m_site->sitemap_url_pending.insert(s);
          }
        }
      }
    }
  } while (token != tokenizer::END);

  if (url_loc) {
    auto url = process_link("https", m_site->host, "", *url_loc);
    if (url.has_value()) {
      m_site->process_sitemap_entry(*url, url_lastmod);
    }
  }
}

void curl_data::finish(std::string effective_url) {
  if (req_type == URL) {
    printf("process page %s\n", effective_url.c_str());

    std::string title = "";
    auto urls = find_links(effective_url, title);
    save();
    m_site->finish(url, urls, title);

  } else if (req_type == ROBOTS) {
    printf("process robots %s\n", effective_url.c_str());
    process_robots();

    m_site->getting_robots = false;
    m_site->got_robots = true;

  } else if (req_type == SITEMAP) {
    printf("process sitemap %s\n", effective_url.c_str());
    process_sitemap();

    m_site->sitemap_url_got.insert(s_url);
    m_site->sitemap_url_getting.erase(s_url);
  }

  in_use = false;
}

// TODO: try http after https for robots?

void curl_data::finish_bad_http(int code) {
  if (req_type == URL) {
    m_site->finish_bad(url, true);

  } else if (req_type == ROBOTS) {
    m_site->getting_robots = false;
    m_site->got_robots = true;

  } else if (req_type == SITEMAP) {
    m_site->sitemap_url_got.insert(s_url);
    m_site->sitemap_url_getting.erase(s_url);
  }

  in_use = false;
}

void curl_data::finish_bad_net(CURLcode res) {
  if (req_type == URL) {
    if (unchanged) {
      m_site->finish_unchanged(url);

    } else {
      if (res != CURLE_WRITE_ERROR) {
        if (res != CURLE_OPERATION_TIMEDOUT) {
          printf("miss (%i) %s %s\n", (int) res, curl_easy_strerror(res), url.url.c_str());
        }
        m_site->finish_bad(url, true);
      } else {
        m_site->finish_bad(url, false);
      }
    }

  } else if (req_type == ROBOTS) {
    m_site->getting_robots = false;
    m_site->got_robots = true;

  } else if (req_type == SITEMAP) {
    m_site->sitemap_url_got.insert(s_url);
    m_site->sitemap_url_getting.erase(s_url);
  }

  in_use = false;
}

curl_data *get_curl_data(std::list<curl_data> &store)
{
  for (auto &d: store) {
    if (!d.in_use) {
      return &d;
    }
  }

  store.emplace_back();
  auto &d = store.back();
  return &d;
}

CURL *make_handle(std::list<curl_data> &store, site* s, index_url u)
{
  curl_data *d = get_curl_data(store);

  d->init_page(s, u);

  CURL *curl_handle = curl_easy_init();

  curl_easy_setopt(curl_handle, CURLOPT_PRIVATE, d);
  curl_easy_setopt(curl_handle, CURLOPT_HEADERFUNCTION, curl_cb_header_write);
  curl_easy_setopt(curl_handle, CURLOPT_HEADERDATA, d);
  curl_easy_setopt(curl_handle, CURLOPT_WRITEFUNCTION, curl_cb_buffer_write);
  curl_easy_setopt(curl_handle, CURLOPT_WRITEDATA, d);
  curl_easy_setopt(curl_handle, CURLOPT_USERAGENT, "libcurl-agent/1.0");
  curl_easy_setopt(curl_handle, CURLOPT_FOLLOWLOCATION, 1);
  curl_easy_setopt(curl_handle, CURLOPT_MAXREDIRS, 3L);
  curl_easy_setopt(curl_handle, CURLOPT_TIMEOUT, 10L);
  curl_easy_setopt(curl_handle, CURLOPT_CONNECTTIMEOUT, 5L);

  curl_easy_setopt(curl_handle, CURLOPT_HTTP_VERSION, CURL_HTTP_VERSION_2TLS);

  char url[util::max_url_len];
  strncpy(url, u.url.c_str(), sizeof(url));
  curl_easy_setopt(curl_handle, CURLOPT_URL, url);

  return curl_handle;
}

CURL *make_handle_other(std::list<curl_data> &store, site* s, request_type r, std::string url)
{
  curl_data *d = get_curl_data(store);

  d->init_other(s, r, url);

  CURL *curl_handle = curl_easy_init();

  curl_easy_setopt(curl_handle, CURLOPT_PRIVATE, d);
  curl_easy_setopt(curl_handle, CURLOPT_WRITEFUNCTION, curl_cb_buffer_write);
  curl_easy_setopt(curl_handle, CURLOPT_WRITEDATA, d);
  curl_easy_setopt(curl_handle, CURLOPT_USERAGENT, "crawlycralwer");
  curl_easy_setopt(curl_handle, CURLOPT_TIMEOUT, 15L);
  curl_easy_setopt(curl_handle, CURLOPT_CONNECTTIMEOUT, 5L);

  curl_easy_setopt(curl_handle, CURLOPT_HTTP_VERSION, CURL_HTTP_VERSION_2TLS);

  char c_url[util::max_url_len];
  strncpy(c_url, url.c_str(), sizeof(c_url));
  curl_easy_setopt(curl_handle, CURLOPT_URL, c_url);

  return curl_handle;
}

void
scraper(Channel<site*> &in, Channel<site*> &out, Channel<bool> &stat, int tid, size_t max_con)
{
  printf("thread %i started\n", tid);

  sigset_t sigpipe_mask;
  sigemptyset(&sigpipe_mask);
  sigaddset(&sigpipe_mask, SIGPIPE);
  pthread_sigmask(SIG_BLOCK, &sigpipe_mask, NULL);

  size_t max_host = 6;

  CURLM *multi_handle = curl_multi_init();
  curl_multi_setopt(multi_handle, CURLMOPT_MAX_TOTAL_CONNECTIONS, max_con);
  curl_multi_setopt(multi_handle, CURLMOPT_MAX_HOST_CONNECTIONS, max_host);

  /* enables http/2 if available */
#ifdef CURLPIPE_MULTIPLEX
  curl_multi_setopt(multi_handle, CURLMOPT_PIPELINING, CURLPIPE_MULTIPLEX);
#endif

  std::list<curl_data> curl_data_store;

  std::list<site*> sites;

  size_t active_connections = 0;

  auto last_log = std::chrono::system_clock::now();

  auto last_accepting = std::chrono::system_clock::now() - 100s;
  bool accepting = false;

  size_t empty_time = 0;

  while (true) {
    bool n_accepting = max_con - active_connections > max_host * 3;
    if (accepting != n_accepting) {
      if (!n_accepting || last_accepting + 10s < std::chrono::system_clock::now()) {
        n_accepting >> stat;
        accepting = n_accepting;
        last_accepting = std::chrono::system_clock::now();
      }
    }

    bool adding = true;
    while (active_connections < max_con && adding) {
      adding = false;
      auto s = sites.begin();
      while (s != sites.end()) {
        if ((*s)->finished()) {
          *s >> out;
          s = sites.erase(s);
          continue;

        } else if (!(*s) ->getting_robots && !(*s)->got_robots) {
          (*s)->getting_robots = true;
          std::string url = "https://" + (*s)->host + "/robots.txt";
          curl_multi_add_handle(multi_handle, make_handle_other(curl_data_store, *s, ROBOTS, url));
          active_connections++;
          adding = true;

        } else if (!(*s)->sitemap_url_pending.empty()) {
          std::string url = *(*s)->sitemap_url_pending.begin();
          (*s)->sitemap_url_pending.erase(url);
          (*s)->sitemap_url_getting.insert(url);

          curl_multi_add_handle(multi_handle, make_handle_other(curl_data_store, *s, SITEMAP, url));
          active_connections++;
          adding = true;

        } else if ((*s)->got_robots && (*s)->sitemap_url_getting.empty()) {
          auto u = (*s)->get_next();
          if (u) {
            curl_multi_add_handle(multi_handle, make_handle(curl_data_store, *s, *u));
            active_connections++;
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

      printf("start scraping site %s\n", s->host.c_str());
      sites.push_back(s);
    }

    if (sites.empty()) {
      if (++empty_time > 100) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
        curl_data_store.clear();
        empty_time = 100;
      } else {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
      }

      continue;

    } else {
      empty_time = 0;
    }

    curl_multi_wait(multi_handle, NULL, 0, 1000, NULL);
    int still_running = 0;
    curl_multi_perform(multi_handle, &still_running);

    int msgs_left;
    CURLMsg *m = NULL;
    while ((m = curl_multi_info_read(multi_handle, &msgs_left))) {
      if (m->msg == CURLMSG_DONE) {
        CURL *handle = m->easy_handle;
        curl_data *d;
        char *url;

        curl_easy_getinfo(handle, CURLINFO_PRIVATE, &d);
        curl_easy_getinfo(handle, CURLINFO_EFFECTIVE_URL, &url);

        CURLcode res = m->data.result;

        if (res == CURLE_OK) {
          long res_status;
          curl_easy_getinfo(handle, CURLINFO_RESPONSE_CODE, &res_status);
          if (res_status == 200) {
            d->finish(std::string(url));
          } else {
            d->finish_bad_http((int) res_status);
          }
        } else {
          d->finish_bad_net(res);
        }

        curl_multi_remove_handle(multi_handle, handle);
        curl_easy_cleanup(handle);

        active_connections--;
      }
    }
  }

  curl_multi_cleanup(multi_handle);
}

}

