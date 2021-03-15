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
    std::string start,
    std::string end)
{
  spdlog::info("starting");

  crawl::crawler crawler;
  crawler.load();

  search::index_part out_word(search::words, w_p, start, end);
  search::index_part out_pair(search::pairs, p_p, start, end);
  search::index_part out_trine(search::trines, t_p, start, end);

  using namespace std::chrono_literals;

  for (auto &s: crawler.sites) {
    if (s.last_scanned == 0) continue;

    spdlog::info("load {} for merging", s.host);

    auto path = fmt::format("meta/sites/{}/{}/index.meta.json", util::host_hash(s.host), s.host);

    search::index_info site_index(path);
    site_index.load();

    spdlog::info("merge {} index", s.host);

    spdlog::debug("merge word");
    for (auto &p: site_index.word_parts) {
      spdlog::debug("check part starting {} <= {} / {} < {}", p.start, start, end, p.end);
      if (start <= p.start && p.end <= end) {
        spdlog::debug("merging part as {} <= {} <= {}", p.start, start, p.end);
        search::index_part in(search::words, p.path, p.start, p.end);
        in.load();

        out_word.merge(in);
      }
    }

    spdlog::debug("merge pair");
    for (auto &p: site_index.pair_parts) {
      spdlog::debug("check part starting {} <= {} / {} < {}", p.start, start, end, p.end);
      if (start <= p.start && p.end <= end) {
        spdlog::debug("merging part as {} <= {} <= {}", p.start, start, p.end);
        search::index_part in(search::pairs, p.path, p.start, p.end);
        in.load();

        out_pair.merge(in);
      }
    }

    spdlog::debug("merge trine");
    for (auto &p: site_index.trine_parts) {
      spdlog::debug("check part starting {} <= {} / {} < {}", p.start, start, end, p.end);
      if (start <= p.start && p.end <= end) {
        spdlog::debug("merging part as {} <= {} <= {}", p.start, start, p.end);
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
  spdlog::set_level(spdlog::level::debug);

  crawl::crawler crawler;
  crawler.load();

  auto n_threads = std::thread::hardware_concurrency();

  spdlog::info("starting {} threads", n_threads);

  std::vector<std::thread> threads;

  std::vector<std::string> split_at;
  split_at.emplace_back("0");
  split_at.emplace_back("f");
  split_at.emplace_back("m");
  split_at.emplace_back("t");
  split_at.emplace_back("{");

  search::index_info info("meta/index.json");

  for (int i = 0; i < split_at.size() - 1; i++) {
    auto start = split_at[i];
    auto end = split_at[i+1];

    auto w_p = fmt::format("meta/index.words.{}.dat", start);
    auto p_p = fmt::format("meta/index.pairs.{}.dat", start);
    auto t_p = fmt::format("meta/index.trines.{}.dat", start);

    auto th = std::thread(merge, w_p, p_p, t_p, start, end);
    //merge(w_p, p_p, t_p, start, end);

    threads.emplace_back(std::move(th));

    info.word_parts.emplace_back(w_p, start, end);
    info.pair_parts.emplace_back(p_p, start, end);
    info.trine_parts.emplace_back(t_p, start, end);
  }

  info.average_page_length = 0;

  for (auto &s: crawler.sites) {
    if (s.last_scanned == 0) continue;

    spdlog::info("load {} for merging", s.host);

    auto path = fmt::format("meta/sites/{}/{}/index.meta.json", util::host_hash(s.host), s.host);

    search::index_info site_index(path);
    site_index.load();

    for (auto &p: site_index.page_lengths) {
      info.average_page_length += p.second;
      info.page_lengths.emplace(p);
    }
  }

  info.average_page_length /= info.page_lengths.size();

  for (auto &t: threads) {
    t.join();
  }

  info.save();

  spdlog::info("done");

  return 0;
}

