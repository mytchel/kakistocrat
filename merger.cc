#include <stdio.h>
#include <unistd.h>

#include <cstdlib>
#include <cstring>

#include <chrono>
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

#include <nlohmann/json.hpp>
#include "spdlog/spdlog.h"

#include "channel.h"
#include "util.h"
#include "scrape.h"
#include "crawl.h"
#include "tokenizer.h"

#include "index.h"

using nlohmann::json;

void merge(
    std::string w_p,
    std::string p_p,
    std::string t_p,
    std::string start)
{
  spdlog::info("starting");

  crawl::crawler crawler;
  crawler.load();

  search::index_part out_word(search::words, w_p, start, "");
  search::index_part out_pair(search::pairs, p_p, start, "");
  search::index_part out_trine(search::trines, t_p, start, "");

  using namespace std::chrono_literals;

  for (auto &s: crawler.sites) {
    if (s.last_scanned == 0) continue;

    spdlog::info("load {} for merging", s.host);

    auto path = fmt::format("meta/sites/{}/{}/index.meta.json", util::host_hash(s.host), s.host);

    search::index_info site_index(path);
    site_index.load();

    spdlog::info("merge {} index", s.host);

    for (auto &p: site_index.word_parts) {
      spdlog::debug("check part starting {} == {}", p.start, start);
      if (p.start <= start) {
        search::index_part in(search::words, p.path, p.start, p.end);
        in.load();

        out_word.merge(in);
      }
    }

    for (auto &p: site_index.pair_parts) {
      spdlog::debug("check part starting {} == {}", p.start, start);
      if (p.start <= start) {
        search::index_part in(search::pairs, p.path, p.start, p.end);
        in.load();

        out_pair.merge(in);
      }
    }

    for (auto &p: site_index.trine_parts) {
      spdlog::debug("check part starting {} == {}", p.start, start);
      if (p.start <= start) {
        search::index_part in(search::trines, p.path, p.start, p.end);
        in.load();

        out_trine.merge(in);
      }
    }
  }

  spdlog::info("saving");
  out_word.save();
  out_pair.save();
  out_trine.save();
  spdlog::info("thread done");
}

int main(int argc, char *argv[]) {
  crawl::crawler crawler;
  crawler.load();

  auto n_threads = std::thread::hardware_concurrency();

  spdlog::info("starting {} threads", n_threads);

  std::vector<std::thread> threads;

  std::vector<std::string> split_at;
  split_at.emplace_back("f");
  split_at.emplace_back("m");
  split_at.emplace_back("t");

  search::index_info info("meta/full.index.meta.json");

  for (auto &s: split_at) {
    auto start = s;

    auto w_p = fmt::format("meta/full.index.words.{}.dat", start);
    auto p_p = fmt::format("meta/full.index.pairs.{}.dat", start);
    auto t_p = fmt::format("meta/full.index.trines.{}.dat", start);

    auto th = std::thread(merge, w_p, p_p, t_p, start);

    threads.emplace_back(std::move(th));

    info.word_parts.emplace_back(w_p, start, "");
    info.pair_parts.emplace_back(p_p, start, "");
    info.trine_parts.emplace_back(t_p, start, "");
  }

  for (auto &t: threads) {
    t.join();
  }

  info.save();

  spdlog::info("done");

  return 0;
}

