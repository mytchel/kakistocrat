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

using namespace std::chrono_literals;

void do_crawl(config &config, crawl::crawler &crawler)
{
  size_t thread_max_sites = config.crawler.thread_max_sites;
  size_t thread_max_con = config.crawler.thread_max_connections;
  size_t site_max_con = config.crawler.site_max_connections;
  size_t max_site_part_size = config.crawler.max_site_part_size;
  size_t max_page_size = config.crawler.max_page_size;

  size_t n_threads;
  if (config.crawler.n_threads) {
    n_threads = *config.crawler.n_threads;
  } else {
    n_threads = std::thread::hardware_concurrency();
  }

  spdlog::info("starting {} threads", n_threads);

  Channel<scrape::site*> in_channels[n_threads];
  Channel<scrape::site*> out_channels[n_threads];
  Channel<bool> stat_channels[n_threads];
  bool thread_stats[n_threads];

  std::vector<std::thread> threads;
  for (size_t i = 0; i < n_threads; i++) {
    auto th = std::thread(scrape::scraper, i,
        std::ref(in_channels[i]),
        std::ref(out_channels[i]),
        std::ref(stat_channels[i]),
        thread_max_sites,
        thread_max_con);

    threads.emplace_back(std::move(th));
  }

  std::list<scrape::site> scrapping_sites;

  auto last_save = std::chrono::system_clock::now();
  bool have_changes = true;

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

    for (size_t i = 0; i < n_threads; i++) {
      if (!stat_channels[i].empty()) {
        thread_stats[i] << stat_channels[i];
      }

      if (thread_stats[i]) {
        auto site = crawler.get_next_site();
        if (site != NULL) {
          thread_stats[i] = false;

          spdlog::info("send site {} to be scraped ({} active)",
              site->host, scrapping_sites.size());

          site->load();

          site->scraping = true;

          scrapping_sites.emplace_back(
              std::move(crawler.make_scrape_site(site,
                site_max_con, max_site_part_size, max_page_size)));

          auto &s = scrapping_sites.back();

          &s >> in_channels[i];
        }
      }

      if (!out_channels[i].empty()) {
        scrape::site *s;
        s << out_channels[i];

        spdlog::info("site finished {}", s->host);

        auto site = crawler.find_site(s->host);
        if (site == NULL) {
          spdlog::info("site '{}' not found in map.", s->host);
          exit(1);
        }

        site->load();

        site->max_pages = 0;
        site->scraped = true;
        site->scraping = false;

        site->last_scanned = time(NULL);
        site->changed = true;

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

        have_changes = true;
      }
    }

    if (scrapping_sites.empty() && !crawler.have_next_site()) {
      time_t now = time(NULL);

      bool have_something = false;
      for (auto &s: crawler.sites) {
        size_t min = (4 * (1 + s.level)) * 60 * 60;
        if (s.scraped && s.last_scanned + min < now) {
          int r = rand() % ((24 * (1 + s.level) - 4) * 60 * 60);
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

      if (!have_something) {
        std::this_thread::sleep_for(60s);
      }
    }

    std::this_thread::sleep_for(10ms);
  }
}

int main(int argc, char *argv[]) {
  spdlog::set_level(spdlog::level::debug);

  config c = read_config();

  std::vector<std::string> blacklist = util::load_list(c.blacklist_path);
  std::vector<std::string> initial_seed = util::load_list(c.seed_path);

  crawl::crawler crawler(c);

  crawler.load();

  crawler.load_blacklist(blacklist);
  crawler.load_seed(initial_seed);

  curl_global_init(CURL_GLOBAL_ALL);

  do_crawl(c, crawler);

  curl_global_cleanup();

  return 0;
}

