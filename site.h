#ifndef SITE_H
#define SITE_H

#include <nlohmann/json.hpp>

#include <thread>
#include <vector>
#include <list>

struct page {
  std::string url;
  std::string path;
  std::string title{"unknown"};

  time_t last_scanned{0};

  std::vector<std::string> aliases;
  std::vector<std::pair<size_t, size_t>> links;

  page() {}

  page(const std::string &u, const std::string &p)
    : url(u), path(p) {}

  page(const std::string &u, const std::string &p,
      std::string tt, time_t t)
    : url(u), path(p), title(tt), last_scanned(t) {}
};

void to_json(nlohmann::json &j, const page &s);
void from_json(const nlohmann::json &j, page &s);

struct site_map {
  std::string path;
  std::string host;

  size_t page_count{0};

  std::vector<std::string> urls;
  std::list<page> pages;

  bool loaded{false};
  bool changed{false};

  void flush() {
    page_count = pages.size();

    if (changed) {
      save();
      changed = false;
    }

    if (loaded) {
      loaded = false;
      spdlog::debug("flush {}", path);
      urls.clear();
      pages.clear();
    }
  }

  void reload();
  void load_json();
  void load_capnp();
  void load();
  void save();

  site_map(site_map &&o) = default;
  site_map(site_map &o) = delete;

  site_map() {}

  site_map(const std::string &p)
    : path(p), host(""), loaded(false), changed(false)
  {}

  site_map(const std::string &p, const std::string &h)
    : path(p), host(h),
      loaded(true),
      changed(true)
  {}

  page* find_page(const std::string &url);
  page* find_page_by_path(const std::string &path);

  page* add_page(const std::string &url, const std::string &path);
};

#endif

