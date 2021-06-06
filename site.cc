#include <stdio.h>
#include <unistd.h>
#include <fcntl.h>

#include <cstdlib>
#include <cstring>

#include <vector>
#include <list>
#include <set>
#include <string>
#include <algorithm>
#include <iostream>
#include <fstream>
#include <sstream>
#include <cstdint>
#include <ctime>

#include "spdlog/spdlog.h"

#include "util.h"
#include "site.h"

#include "indexer.capnp.h"

#include <capnp/serialize-packed.h>

void site_map::load_capnp()
{
  int fd = open(path.c_str(), O_RDONLY);
  if (fd < 0) {
    spdlog::info("failed to open {}", path);
    return;
  }

  capnp::ReaderOptions opts;
  opts.traversalLimitInWords = 128 * 1024 * 1024;

  ::capnp::PackedFdMessageReader message(fd, opts);

  Site::Reader reader = message.getRoot<Site>();

  host = reader.getHost();

  for (auto url: reader.getUrls()) {
    urls.emplace_back(url);   
  }

  for (auto page: reader.getPages()) {
    pages.emplace_back(page.getUrl(), page.getPath());

    auto &n = pages.back();

    n.title = page.getTitle();
    n.last_scanned = page.getLastScanned();

    for (auto a: page.getAliases()) {
      n.aliases.emplace_back(a);
    }

    for (auto l: page.getLinks()) {
      n.links.emplace_back(l.getUrl(), l.getCount());
    }
  }
 
  close(fd);
  
  page_count = pages.size();
  spdlog::debug("loaded {} : {} with {} urls", pages.size(), path, urls.size());
}

void site_map::save() {
  if (!loaded) {
    spdlog::warn("saving unloaded {}", path);
    return;
  }

  spdlog::info("saving  {} : {} with {} urls", pages.size(), path, urls.size());

  page_count = pages.size();

  std::string new_path = path;

  if (util::has_suffix(path, "json")) {
    new_path = path.substr(0, path.length() - 4);
    new_path += "capnp";
  }

  ::capnp::MallocMessageBuilder message;

  Site::Builder n = message.initRoot<Site>();

  n.setPath(new_path);
  n.setHost(host);

  auto n_urls = n.initUrls(urls.size());
  for (size_t i = 0; i < urls.size(); i++) {
    n_urls.set(i, urls[i]);
  }

  auto n_pages = n.initPages(pages.size());
  size_t i = 0;
  for (auto &p: pages) {
    auto n_page = n_pages[i++];

    n_page.setUrl(p.url);
    n_page.setPath(p.path);
    n_page.setTitle(p.title);
    n_page.setLastScanned(p.last_scanned);

    auto aliases = n_page.initAliases(p.aliases.size());
    size_t i_a = 0;
    for (auto &a: p.aliases) {
      aliases.set(i_a++, a);
    }

    auto links = n_page.initLinks(p.links.size());
    size_t i_l = 0;
    for (auto &l: p.links) {
      links[i_l].setUrl(l.first);
      links[i_l].setCount(l.second);
      i_l++;
    }
  }

  int fd = open(new_path.c_str(), O_WRONLY|O_CREAT|O_TRUNC, 0664);
  if (fd < 0) {
    spdlog::info("failed to open {} for writing", new_path);
    throw std::runtime_error("failed to open site for writing");
  }

  writePackedMessageToFd(fd, message);
  
  close(fd);

  path = new_path;
}

void site_map::reload() {
  loaded = true;

  urls.clear();
  pages.clear();

  load_capnp();
}

void site_map::load() {
  if (loaded) return;
  reload();
}

page* site_map::find_page(const std::string &url)
{
  load();

  for (auto &p: pages) {
    if (p.url == url) {
      return &p;
    }

    for (auto &a: p.aliases) {
      if (a == url) {
        return &p;
      }
    }
  }

  return NULL;
}

page* site_map::find_page_by_path(const std::string &path)
{
  for (auto &p: pages) {
    if (p.path == path) {
      return &p;
    }
  }

  return NULL;
}

page* site_map::add_page(const std::string &url, const std::string &path)
{
  changed = true;
  page_count++;
  return &pages.emplace_back(url, path);
}


