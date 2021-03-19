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
    std::optional<std::string> start,
    std::optional<std::string> end,
    Channel<size_t> &done_channel,
    size_t id)
{
  spdlog::info("starting");

  crawl::crawler crawler;
  crawler.load();

  search::index_part out_word(search::words, w_p, start, end);
  search::index_part out_pair(search::pairs, p_p, start, end);
  search::index_part out_trine(search::trines, t_p, start, end);

  using namespace std::chrono_literals;

  std::chrono::nanoseconds load_total{0ms};
  std::chrono::nanoseconds merge_total{0ms};
  std::chrono::nanoseconds save_total{0ms};

  for (auto &s: crawler.sites) {
    if (s.last_scanned == 0) continue;

    spdlog::info("load {} for merging", s.host);

    auto path = fmt::format("meta/sites/{}/{}/index.meta.json", util::host_hash(s.host), s.host);

    search::index_info site_index(path);
    site_index.load();

    spdlog::info("merge {} index", s.host);

    spdlog::debug("merge word");
    for (auto &p: site_index.word_parts) {
      if ((!end || p.start <= *end) && (!start || *start <= p.end)) {
        auto tstart = std::chrono::system_clock::now();
        search::index_part in(search::words, p.path, p.start, p.end);
        in.load();
        auto tmid = std::chrono::system_clock::now();

        out_word.merge(in);
        auto tend = std::chrono::system_clock::now();

        load_total += tmid - tstart;
        merge_total += tend - tmid;
      }
    }

    spdlog::debug("merge pair");
    for (auto &p: site_index.pair_parts) {
      if ((!end || p.start <= *end) && (!start || *start <= p.end)) {
        auto tstart = std::chrono::system_clock::now();
        search::index_part in(search::pairs, p.path, p.start, p.end);
        in.load();

        auto tmid = std::chrono::system_clock::now();
        out_pair.merge(in);
        auto tend = std::chrono::system_clock::now();

        load_total += tmid - tstart;
        merge_total += tend - tmid;
      }
    }

    spdlog::debug("merge trine");
    for (auto &p: site_index.trine_parts) {
      if ((!end || p.start <= *end) && (!start || *start <= p.end)) {
        auto tstart = std::chrono::system_clock::now();
        search::index_part in(search::trines, p.path, p.start, p.end);
        in.load();

        auto tmid = std::chrono::system_clock::now();
        out_trine.merge(in);
        auto tend = std::chrono::system_clock::now();

        load_total += tmid - tstart;
        merge_total += tend - tmid;
      }
    }
  }

  auto tstart = std::chrono::system_clock::now();
  spdlog::info("saving");
  out_word.save();
  out_pair.save();
  out_trine.save();
  spdlog::info("thread done");
  auto tend = std::chrono::system_clock::now();

  save_total = tend - tstart;

  spdlog::info("STAT load        took {}", load_total.count() / 1000000);
  spdlog::info("STAT merge       took {}", merge_total.count() / 1000000);
  spdlog::info("STAT save        took {}", save_total.count() / 1000000);

  spdlog::info("STAT word index  took {}", out_word.index_total.count() / 1000000);
  spdlog::info("STAT word merge  took {}", out_word.merge_total.count() / 1000000);
  spdlog::info("STAT word find   took {}", out_word.find_total.count() / 1000000);

  spdlog::info("STAT trine index took {}", out_trine.index_total.count() / 1000000);
  spdlog::info("STAT trine merge took {}", out_trine.merge_total.count() / 1000000);
  spdlog::info("STAT trine find  took {}", out_trine.find_total.count() / 1000000);

  id >> done_channel;
}

int main(int argc, char *argv[]) {
  spdlog::set_level(spdlog::level::debug);

  crawl::crawler crawler;
  crawler.load();

  auto n_threads = 2;//std::thread::hardware_concurrency();

  spdlog::info("starting {} threads", n_threads);

  std::vector<std::thread> threads(n_threads);
  Channel<size_t> done_channel;

  std::vector<std::string> split_at;
  split_at.emplace_back("a");
  split_at.emplace_back("b");
  split_at.emplace_back("c");
  split_at.emplace_back("d");
  split_at.emplace_back("e");
  split_at.emplace_back("f");
  split_at.emplace_back("g");
  split_at.emplace_back("h");
  split_at.emplace_back("i");
  split_at.emplace_back("j");
  split_at.emplace_back("k");
  split_at.emplace_back("l");
  split_at.emplace_back("m");
  split_at.emplace_back("n");
  split_at.emplace_back("o");
  split_at.emplace_back("p");
  split_at.emplace_back("q");
  split_at.emplace_back("r");
  split_at.emplace_back("s");
  split_at.emplace_back("t");
  split_at.emplace_back("u");
  split_at.emplace_back("v");
  split_at.emplace_back("w");
  split_at.emplace_back("x");
  split_at.emplace_back("y");
  split_at.emplace_back("z");

  search::index_info info("meta/index.json");

  std::optional<std::string> start, end;
  auto it = split_at.begin();
  do {
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

    spdlog::info("thread available");

    if (it != split_at.end()) {
      end = *it;
      it++;
    } else {
      end = {};
    }

    std::string w_p = "meta/index.words..dat";
    std::string p_p = "meta/index.pairs..dat";
    std::string t_p = "meta/index.trines..dat";
    if (start) {
      w_p = fmt::format("meta/index.words.{}.dat", *start);
      p_p = fmt::format("meta/index.pairs.{}.dat", *start);
      t_p = fmt::format("meta/index.trines.{}.dat", *start);
    }

    auto th = std::thread(merge, w_p, p_p, t_p, start, end, std::ref(done_channel), free_thread);
    threads[free_thread] = std::move(th);

    //merge(w_p, p_p, t_p, start, end);

    info.word_parts.emplace_back(w_p, start, end);
    info.pair_parts.emplace_back(p_p, start, end);
    info.trine_parts.emplace_back(t_p, start, end);

    start = end;

//    break;

  } while (end);


  info.average_page_length = 0;

  for (auto &s: crawler.sites) {
    if (s.last_scanned == 0) continue;

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
    if (t.joinable()) {
      t.join();
    }
  }

  info.save();

  spdlog::info("done");

  return 0;
}

