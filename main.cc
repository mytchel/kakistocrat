#include <stdio.h>
#include <unistd.h>

#include <cstdlib>
#include <cstring>

#include <vector>
#include <list>
#include <set>
#include <map>
#include <string>
#include <algorithm>
#include <thread>
#include <future>
#include <optional>
#include <iostream>
#include <fstream>
#include <sstream>
#include <cstdint>

#include <curl/curl.h>

#include <nlohmann/json.hpp>
#include "spdlog/spdlog.h"

#include "util.h"
#include "config.h"
#include "crawl.h"
#include "index.h"
#include "scorer.h"
#include "search.h"

using nlohmann::json;

void merge(const config &c, const std::list<std::string> &part_paths) {
  size_t n_threads;
  if (c.indexer.n_threads) {
    n_threads = *c.indexer.n_threads;
  } else {
    n_threads = std::thread::hardware_concurrency();
    if (n_threads > 1) n_threads--;
  }

  spdlog::info("starting {} threads", n_threads);

  std::vector<std::thread> threads(n_threads);
  Channel<size_t> done_channel;

  auto split_at = search::get_split_at(c.index_parts);

  search::index_info info(c.merger.meta_path);

  util::make_path(c.merger.parts_path);

  auto it = split_at.begin();
  while (it != split_at.end()) {
    if (!done_channel.empty()) {
      size_t id;

      id << done_channel;

      threads[id].join();
    }

    size_t free_thread = n_threads;
    for (size_t i = 0; i < n_threads; i++) {
      if (!threads[i].joinable()) {
        free_thread = i;
        break;
      }
    }

    if (free_thread == n_threads) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
      continue;
    }

    std::string start = *it;

    std::optional<std::string> end;

    it++;
    if (it != split_at.end()) {
      end = *it;
    }

    auto w_p = fmt::format("{}/index.words.{}.dat", c.merger.parts_path, start);
    auto p_p = fmt::format("{}/index.pairs.{}.dat", c.merger.parts_path, start);
    auto t_p = fmt::format("{}/index.trines.{}.dat", c.merger.parts_path, start);

    auto th = std::thread(search::merge, c,
          part_paths,
          w_p, p_p, t_p,
          start, end,
          std::ref(done_channel), free_thread);

    threads[free_thread] = std::move(th);

    info.word_parts.emplace_back(w_p, start, end);
    info.pair_parts.emplace_back(p_p, start, end);
    info.trine_parts.emplace_back(t_p, start, end);
  }

  info.average_page_length = 0;

  for (auto &path: part_paths) {
    search::index_info index(path);
    index.load();

    for (auto &p: index.page_lengths) {
      info.average_page_length += p.second;
      info.page_lengths.emplace(p.first, p.second);
    }
  }

  if (info.page_lengths.size() > 0) {
    info.average_page_length /= info.page_lengths.size();
  } else {
    info.average_page_length = 0;
  }

  info.save();

  for (auto &t: threads) {
    if (t.joinable()) {
      t.join();
    }
  }

  spdlog::info("done");
}

void update_scores(const config &c, crawl::crawler &crawler)
{
  scorer::scores scores(c.scores_path, crawler);

  for (int i = 0; i < 10; i++) {
    spdlog::debug("score iteration {}", i);
    scores.iteration();
  }

  scores.save();
}

void run(const config &config, crawl::crawler &crawler)
{
  size_t thread_max_sites = config.crawler.thread_max_sites;
  size_t thread_max_con = config.crawler.thread_max_connections;
  size_t site_max_con = config.crawler.site_max_connections;
  size_t max_site_part_size = config.crawler.max_site_part_size;
  size_t max_page_size = config.crawler.max_page_size;

  size_t n_scrape_threads;
  if (config.crawler.n_threads) {
    n_scrape_threads = *config.crawler.n_threads;
  } else {
    n_scrape_threads = std::thread::hardware_concurrency();
  }

  size_t n_index_threads;
  if (config.indexer.n_threads) {
    n_index_threads = *config.indexer.n_threads;
  } else {
    n_index_threads = std::thread::hardware_concurrency();
    if (n_index_threads > 1) n_index_threads--;
  }

  spdlog::info("starting {} index threads", n_index_threads);

  Channel<std::string*> index_in[n_index_threads];
  Channel<bool> index_ready[n_index_threads];
  Channel<std::string*> index_out[n_index_threads];

  std::list<crawl::site*> indexing[n_index_threads];

  std::vector<std::thread> index_threads;

  for (size_t i = 0; i < n_index_threads; i++) {
    auto th = std::thread(
        search::indexer_run,
        config,
        std::ref(index_in[i]),
        std::ref(index_ready[i]),
        std::ref(index_out[i]),
        i);

    index_threads.emplace_back(std::move(th));
  }

  spdlog::info("starting {} scrape threads", n_scrape_threads);

  Channel<scrape::site*> scrape_in[n_scrape_threads];
  Channel<scrape::site*> scrape_out[n_scrape_threads];
  Channel<bool> scrape_stat[n_scrape_threads];

  std::vector<std::thread> scrape_threads;

  for (size_t i = 0; i < n_scrape_threads; i++) {
    auto th = std::thread(scrape::scraper, i,
        std::ref(scrape_in[i]),
        std::ref(scrape_out[i]),
        std::ref(scrape_stat[i]),
        thread_max_sites,
        thread_max_con);

    scrape_threads.emplace_back(std::move(th));
  }

  std::list<scrape::site> scrapping_sites;
  std::list<crawl::site *> index_pending_sites;

  auto last_save = std::chrono::system_clock::now();
  bool have_changes = true;

  crawl::site *next_site = nullptr;

  std::list<std::string> index_parts =
    search::load_parts(config.indexer.meta_path);

  bool all_indexed = true;
  for (auto &s: crawler.sites) {
    if (s.scraped && !s.indexed) {
      all_indexed = false;
    }
  }

  if (all_indexed) {
    spdlog::info("everything is indexed so clearing partital paths");
    index_parts.clear();
  }


  while (true) {
    if (have_changes && last_save + 10s < std::chrono::system_clock::now()) {
      spdlog::info("periodic save");

      for (auto &s: crawler.sites) {
        s.flush();
      }

      // Save the current index so exiting early doesn't loose
      // all the work that has been done
      crawler.save();

      spdlog::info("periodic save finished");

      have_changes = false;
      last_save = std::chrono::system_clock::now();
    }

    if (next_site == nullptr) {
      next_site = crawler.get_next_site();
    }

    for (size_t i = 0; next_site != nullptr && i < n_scrape_threads; i++) {
      if (scrape_stat[i].empty()) continue;

      while (!scrape_stat[i].empty()) {
        bool b;
        b << scrape_stat[i];
      }

      spdlog::info("send site {} to be scraped ({} active)",
              next_site->host, scrapping_sites.size());

      next_site->load();

      next_site->scraping = true;

      scrapping_sites.emplace_back(
          std::move(
            crawler.make_scrape_site(next_site,
              site_max_con, max_site_part_size, max_page_size)));

      auto &s = scrapping_sites.back();

      &s >> scrape_in[i];

      next_site = crawler.get_next_site();
    }

    for (size_t i = 0; i < n_scrape_threads; i++) {
      if (scrape_out[i].empty()) continue;

      scrape::site *s;
      s << scrape_out[i];

      spdlog::info("site finished {}", s->host);

      auto site = crawler.find_site(s->host);
      if (site == nullptr) {
        spdlog::info("site '{}' not found in map.", s->host);
        exit(1);
      }

      site->load();

      site->changed = true;
      site->max_pages = 0;
      site->scraped = true;
      site->scraping = false;

      site->last_scanned = time(NULL);

      crawler.update_site(site, s->url_scanned);

      if (site->level + 1 < crawler.levels.size()) {
        auto level = crawler.levels[site->level];
        auto next_level = crawler.levels[site->level + 1];

        crawler.enable_references(site, level.max_add_sites, next_level.max_pages);
      }

      spdlog::info("site finished for level {} with {:3} (+ {:3} unchanged) pages : {}",
          site->level, s->url_scanned.size(), s->url_unchanged.size(),
          site->host);

      scrapping_sites.remove_if([s](const scrape::site &ss) {
          return &ss == s;
          });

      // Need to write the changes for this site
      // so the indexer has something to load.
      site->flush();

      spdlog::info("transition {} to indexing", site->host);
      site->indexing_part = true;
      site->indexed_part = false;
      site->indexed = false;

      index_pending_sites.emplace_back(site);

      have_changes = true;
    }

    for (size_t i = 0; !index_pending_sites.empty() && i < n_index_threads; i++) {
      if (index_ready[i].empty()) continue;

      auto site = index_pending_sites.front();
      index_pending_sites.pop_front();

      spdlog::info("send to indexer {}", site->host);

      indexing[i].push_back(site);

      &site->path >> index_in[i];

      bool ready;
      ready << index_ready[i];
    }

    for (size_t i = 0; i < n_index_threads; i++) {
      if (index_out[i].empty()) continue;

      std::string *p;

      p << index_out[i];
      if (p != NULL) {
        spdlog::info("GOT INDEX PART");

        spdlog::info("got {} from {}", *p, i);
        index_parts.push_back(*p);

        spdlog::info("save index parts");
        search::save_parts(config.indexer.meta_path, index_parts);

        for (auto s: indexing[i]) {
          spdlog::info("transition {} to indexed part", s->host);
          s->indexing_part = false;
          s->indexed_part = true;
          s->indexed = false;
        }

        indexing[i].clear();

        have_changes = true;
      }
    }

    if (next_site == nullptr
        && scrapping_sites.empty()
        && index_pending_sites.empty())
    {
      spdlog::info("have nothing, can finish index");

      bool all_indexed = true;
      for (auto &s: crawler.sites) {
        if (s.scraped && !s.indexed) {
          all_indexed = false;
          break;
        }
      }

      if (all_indexed) {
        spdlog::info("everything is indexed, nothing to do");
        std::this_thread::sleep_for(5s);
        continue;
      }

      bool have_indexing = false;
      for (auto &s: crawler.sites) {
        if (!s.scraped) continue;

        if (s.indexing_part) {
          have_indexing = true;

        } else if (!s.indexing_part && !s.indexed_part) {
          spdlog::info("put site {} to indexer without rescrapping", s.host);

          s.indexing_part = true;
          s.indexed_part = false;
          s.indexed = false;
          index_pending_sites.push_back(&s);
        }
      }

      if (!index_pending_sites.empty()) {
        spdlog::info("waiting for indexer to finish");
        continue;
      }

      if (have_indexing) {
        spdlog::info("waiting for indexer to finish / flush");

        // flush the indexers
        std::string *sync = NULL;
        for (size_t i = 0; i < n_index_threads; i++) {
          if (!indexing[i].empty()) {
            spdlog::info("flushing indexer {}", i);
            sync >> index_in[i];
          }
        }

        std::this_thread::sleep_for(100ms);
        continue;
      }

      if (index_parts.empty()) {
        spdlog::info("no parts to merge");
        std::this_thread::sleep_for(5s);
        continue;
      }

      spdlog::info("all indexed, start merging {} parts", index_parts.size());

      merge(config, index_parts);
      index_parts.clear();

      spdlog::info("transition sites");
      for (auto &s: crawler.sites) {
        spdlog::info("check {}  scraped = {} indexed part = {}",
            s.host, s.scraped, s.indexed_part);
        if (s.scraped && s.indexed_part) {
          spdlog::info("transition {} to indexed", s.host);
          s.indexing_part = false;
          s.indexed_part = true;
          s.indexed = true;
        }
      }

      crawler.save();

      update_scores(config, crawler);

      time_t now = time(NULL);

      bool have_something = false;
      for (auto &s: crawler.sites) {
        size_t min = (config.frequency_d * (1 + s.level)) * 24 * 60 * 60;
        size_t max = min * 5;
        if (s.scraped && s.last_scanned + min < now) {
          int r = rand() % (max - min);
          bool should_scrape = s.last_scanned + min + r < now;
          if (should_scrape) {
            s.scraped = false;
            spdlog::info("transition {} from scraped to ready", s.host);

            if (s.level == 0) {
              spdlog::info("give site {} at level 0 more pages", s.host);
              s.max_pages = crawler.levels[0].max_pages;

              have_something = true;
            }
          }
        }
      }
    }

    std::this_thread::sleep_for(10ms);
  }
}

int main(int argc, char *argv[]) {
  spdlog::set_level(spdlog::level::debug);

  curl_global_init(CURL_GLOBAL_ALL);

  spdlog::info("load config");

  config settings = read_config();

  spdlog::info("load lists");

  std::vector<std::string> blacklist = util::load_list(settings.blacklist_path);
  std::vector<std::string> initial_seed = util::load_list(settings.seed_path);

  spdlog::info("loading");

  crawl::crawler crawler(settings);
  crawler.load();

  spdlog::info("loading lists to crawler");

  crawler.load_blacklist(blacklist);
  crawler.load_seed(initial_seed);

  run(settings, crawler);

  curl_global_cleanup();

  return 0;
}
