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

int main(int argc, char *argv[]) {
  spdlog::set_level(spdlog::level::debug);

  config c = read_config();

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

  auto part_paths = search::load_parts(c.indexer.meta_path);

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

