#ifndef CONFIG_H
#define CONFIG_H

#include <nlohmann/json.hpp>

struct crawl_level {
  size_t max_pages;
  size_t max_add_sites;

  crawl_level() {}
  crawl_level(size_t p, size_t s)
    : max_pages(p), max_add_sites(s)
  {}
};

void from_json(const nlohmann::json &j, crawl_level &l);

struct config {
  std::string seed_path;
  std::string blacklist_path;
  size_t frequency_d;

  struct {
    std::optional<size_t> n_threads;
    size_t thread_max_sites;
    size_t thread_max_connections;
    size_t site_max_connections;
    size_t max_site_part_size;
    size_t max_page_size;

    std::vector<crawl_level> levels;

    std::string site_data_path;
    std::string site_meta_path;
    std::string sites_path;
  } crawler;

  size_t index_parts;

  struct {
    std::optional<size_t> n_threads;
    size_t thread_max_mem;
    size_t max_index_part_size;
    size_t pages_per_part;

    size_t htcap;

    std::string meta_path;
    std::string parts_path;
  } indexer;

  struct {
    std::optional<size_t> n_threads;
    size_t max_index_part_size;

    size_t frequency_minutes;

    size_t htcap;

    std::string meta_path;
    std::string parts_path;
  } merger;

  std::string scores_path;
};

config read_config(std::string path = "config.json");

#endif

