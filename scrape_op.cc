#include <string.h>
#include <math.h>
#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <signal.h>

#include <sys/stat.h>
#include <sys/types.h>

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

site_op::~site_op() {
  m_site->push_buf(buf);
}

void site_op_page::save()
{
  std::ofstream file;

  file.open(m_page->path, std::ios::out | std::ios::binary | std::ios::trunc);

  if (!file.is_open()) {
    spdlog::warn("error opening file {} for {}", m_page->path, m_page->url);
    return;
  }

  file.write((char *) buf, size);

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

void site_op_page::process_page(const std::string &page_url,
    std::vector<std::string> &links,
    std::string &title)
{
  char tok_buffer_store[1024];
	struct str tok_buffer;
	str_init(&tok_buffer, tok_buffer_store, sizeof(tok_buffer_store));

  tokenizer::token_type token;
  tokenizer::tokenizer tok((const char *) buf, size);

  auto page_proto = util::get_proto(page_url);
  auto page_host = util::get_host(page_url);
  auto page_dir = util::get_dir(util::get_path(page_url));

  if (page_proto.empty() || page_host.empty() || page_dir.empty()) {
    spdlog::error("BAD PAGE URL '{}' : '{}' -> '{}' '{}' '{}'",
        m_page->url, page_url, page_proto, page_host, page_dir);

    return;
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
            links.push_back(*s);

            if (links.size() == links.capacity()) {
              spdlog::warn("hit max links for page {}", page_url);
              return;
            }
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
}

void site_op_page::finish(const std::string &effective_url)
{
  spdlog::debug("process page {}", effective_url);

  // Sites seem to do this to facebook for some things
  if (util::get_host(effective_url) != m_site->host) {
    spdlog::trace("redirected from {} to other site {}",
        m_page->url, effective_url);

    m_site->finish_bad(m_page, false);
  }

  std::string title = "";
  std::vector<std::string> links;
  links.reserve(m_site->max_links);

  process_page(effective_url, links, title);

  m_site->finish(m_page, links, title);

  save();
}

void site_op_robots::finish(const std::string &effective_url) {
  spdlog::debug("process robots {}", effective_url);

  m_site->getting_robots = false;
  m_site->got_robots = true;

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
      m_site->add_sitemap(value);
    }
  }
}

void site_op_sitemap::finish(const std::string &effective_url) {
  spdlog::debug("process sitemap {}", effective_url);

  m_site->sitemap_url_got.insert(m_url);
  m_site->sitemap_url_getting.erase(m_url);

  char tok_buffer_store[1024];
	struct str tok_buffer;
	str_init(&tok_buffer, tok_buffer_store, sizeof(tok_buffer_store));

  tokenizer::token_type token;
  tokenizer::tokenizer tok((const char *) buf, size);

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

          m_site->add_sitemap(s);
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

void site_op_page::finish_bad(bool bad) {
  if (unchanged) {
    m_site->finish_unchanged(m_page);
    return;
  }

  m_site->finish_bad(m_page, bad);
}

void site_op_robots::finish_bad(bool) {
  m_site->getting_robots = false;
  m_site->got_robots = true;
}

void site_op_sitemap::finish_bad(bool) {
  m_site->sitemap_url_got.insert(m_url);
  m_site->sitemap_url_getting.erase(m_url);
}

}

