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
#include "tokenizer.h"

#include "index.h"

using nlohmann::json;

void merge(
    std::list<std::string> part_paths,
    std::string w_p,
    std::string p_p,
    std::string t_p,
    std::string start,
    std::optional<std::string> end,
    Channel<size_t> &done_channel,
    size_t id)
{
  spdlog::info("starting");

  search::index_part out_word(search::words, w_p, start, end);
  search::index_part out_pair(search::pairs, p_p, start, end);
  search::index_part out_trine(search::trines, t_p, start, end);

  using namespace std::chrono_literals;

  std::chrono::nanoseconds load_total{0ms};
  std::chrono::nanoseconds merge_total{0ms};
  std::chrono::nanoseconds save_total{0ms};

  for (auto &index_path: part_paths) {
    spdlog::info("load {} for merging", index_path);

    search::index_info index(index_path);
    index.load();

    spdlog::info("merge {} index", index_path);

    spdlog::info("index {} word part usage {} kb", start, out_word.usage() / 1024);
    spdlog::info("index {} pair part usage {} kb", start, out_pair.usage() / 1024);
    spdlog::info("index {} trine part usage {} kb", start, out_trine.usage() / 1024);

    for (auto &p: index.word_parts) {
      if ((!end || p.start < *end) && start < p.end) {
        spdlog::info("MERGE {}  from {}", start, p.start);

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

    for (auto &p: index.pair_parts) {
      if ((!end || p.start < *end) && start < p.end) {
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

    for (auto &p: index.trine_parts) {
      if ((!end || p.start < *end) && start < p.end) {
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

  spdlog::info("saving");
  auto tstart = std::chrono::system_clock::now();

  out_word.save();
  out_pair.save();
  out_trine.save();

  auto tend = std::chrono::system_clock::now();
  spdlog::info("saved");

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

  auto n_threads = std::thread::hardware_concurrency();
  if (n_threads > 1) n_threads--;

  spdlog::info("starting {} threads", n_threads);

  std::vector<std::thread> threads(n_threads);
  Channel<size_t> done_channel;

  auto split_at = search::get_split_at();

  auto part_paths = search::load_parts("meta/index_parts.json");

  search::index_info info("meta/index.json");

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

    auto w_p = fmt::format("meta/index.words.{}.dat", start);
    auto p_p = fmt::format("meta/index.pairs.{}.dat", start);
    auto t_p = fmt::format("meta/index.trines.{}.dat", start);

    auto th = std::thread(merge, part_paths,
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

  info.average_page_length /= info.page_lengths.size();

  info.save();

  for (auto &t: threads) {
    if (t.joinable()) {
      t.join();
    }
  }

  spdlog::info("done");

  return 0;
}

