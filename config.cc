#include <stdio.h>
#include <unistd.h>

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

#include "config.h"

using nlohmann::json;

void from_json(const json &j, crawl_level &l) {
  j.at("p").get_to(l.max_pages);
  j.at("s").get_to(l.max_add_sites);
}

config default_config() {
  config c;

  c.seed_path = "seed";
  c.blacklist_path = "blacklist";
  c.frequency_d = 1;

  c.crawler.site_data_path = "out/sites_data/";
  c.crawler.site_meta_path = "out/sites_meta/";
  c.crawler.sites_path = "out/sites.json";

  c.crawler.thread_max_sites = 5;
  c.crawler.thread_max_connections = 100;
  c.crawler.site_max_connections = 5;
  c.crawler.max_site_part_size = 100 * 1024 * 1024;
  c.crawler.max_page_size = 10 * 1024 * 1024;

  c.crawler.levels.emplace_back(10, 2);
  c.crawler.levels.emplace_back(2, 0);

  c.indexer.thread_max_mem = 100 * 1024 * 1024;
  c.indexer.max_index_part_size = 10 * 1024 * 1024;
  c.indexer.htcap = 1 << 16;
  c.indexer.sites_per_part = 100;

  c.indexer.parts_path = "out/index_parts/";
  c.indexer.meta_path = "out/index_parts.json";

  c.merger.max_index_part_size = 200 * 1024 * 1024;
  c.merger.frequency_minutes = 10;
  c.merger.htcap = 1 << 16;

  c.merger.parts_path = "out/index_merged/";
  c.merger.meta_path = "out/merged.json";

  c.index_parts = 30;
  c.index_meta_path = "out/index_meta.json";

  c.scores_path = "out/scores.json";

  return c;
}

config read_config(std::string path) {
  config c = default_config();

  std::ifstream file;

  file.open(path, std::ios::in);

  if (!file.is_open()) {
    spdlog::warn("error opening file {}", path);
    return c;
  }

  size_t s_mb;
  size_t s;

  json j = json::parse(file);

  j.at("seed").get_to(c.seed_path);
  j.at("blacklist").get_to(c.blacklist_path);
  j.at("frequency_d").get_to(c.frequency_d);

  if (!j.at("crawler").at("n_threads").is_null()) {
    size_t t;
    j.at("crawler").at("n_threads").get_to(t);
    c.crawler.n_threads = t;
  }

  j.at("crawler").at("site_data_path").get_to(c.crawler.site_data_path);
  j.at("crawler").at("site_meta_path").get_to(c.crawler.site_meta_path);
  j.at("crawler").at("sites_path").get_to(c.crawler.sites_path);

  j.at("crawler").at("thread_max_sites").get_to(c.crawler.thread_max_sites);
  j.at("crawler").at("thread_max_connections").get_to(c.crawler.thread_max_connections);
  j.at("crawler").at("site_max_connections").get_to(c.crawler.site_max_connections);
  j.at("crawler").at("max_site_part_size_mb").get_to(s_mb);
  c.crawler.max_site_part_size = s_mb * 1024 * 1024;
  j.at("crawler").at("max_page_size_mb").get_to(s_mb);
  c.crawler.max_page_size = s_mb * 1024 * 1024;

  j.at("crawler").at("levels").get_to(c.crawler.levels);

  j.at("index_parts").get_to(c.index_parts);
  j.at("index_meta_path").get_to(c.index_meta_path);

  if (!j.at("indexer").at("n_threads").is_null()) {
    size_t t;
    j.at("indexer").at("n_threads").get_to(t);
    c.indexer.n_threads = t;
  }

  j.at("indexer").at("thread_max_mem_mb").get_to(s_mb);
  c.indexer.thread_max_mem = s_mb * 1024 * 1024;
  j.at("indexer").at("max_index_part_size_mb").get_to(s_mb);
  c.indexer.max_index_part_size = s_mb * 1024 * 1024;

  j.at("indexer").at("htcap").get_to(s);
  c.indexer.htcap = 1 << s;

  j.at("indexer").at("sites_per_part").get_to(c.indexer.sites_per_part);

  j.at("indexer").at("parts_path").get_to(c.indexer.parts_path);
  j.at("indexer").at("meta_path").get_to(c.indexer.meta_path);

  if (!j.at("merger").at("n_threads").is_null()) {
    size_t t;
    j.at("merger").at("n_threads").get_to(t);
    c.merger.n_threads = t;
  }

  j.at("merger").at("max_index_part_size_mb").get_to(s_mb);
  c.merger.max_index_part_size = s_mb * 1024 * 1024;

  j.at("merger").at("frequency_minutes").get_to(c.merger.frequency_minutes);
  j.at("merger").at("htcap").get_to(s);
  c.merger.htcap = 1 << s;

  j.at("merger").at("parts_path").get_to(c.merger.parts_path);
  j.at("merger").at("meta_path").get_to(c.merger.meta_path);

  j.at("scores_path").get_to(c.scores_path);

  file.close();

  return c;
}


