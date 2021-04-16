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

namespace search {

void merge(
    config settings,
    std::list<std::string> part_paths,
    std::string w_p,
    std::string p_p,
    std::string t_p,
    std::string start,
    std::optional<std::string> end,
    Channel<size_t> &done_channel,
    size_t id)
{
  spdlog::info("starting {}", start);

  size_t buf_len = settings.merger.max_index_part_size;
  uint8_t *buf = (uint8_t *) malloc(buf_len);
  if (buf == nullptr) {
    throw std::bad_alloc();
  }

  size_t htcap = settings.merger.htcap;

  search::index_part out_word(w_p, htcap, start, end);
  search::index_part out_pair(p_p, htcap, start, end);
  search::index_part out_trine(t_p, htcap, start, end);

  using namespace std::chrono_literals;

  std::chrono::nanoseconds load_total{0ms};
  std::chrono::nanoseconds merge_total{0ms};
  std::chrono::nanoseconds save_total{0ms};

  for (auto &index_path: part_paths) {
    spdlog::info("load {} for merging", index_path);

    search::index_info index(index_path);
    index.load();

    spdlog::info("index {} word part usage {} kb", start, out_word.usage() / 1024);
    spdlog::info("index {} pair part usage {} kb", start, out_pair.usage() / 1024);
    spdlog::info("index {} trine part usage {} kb", start, out_trine.usage() / 1024);

    for (auto &p: index.word_parts) {
      if ((!end || p.start < *end) && (!p.end || start < *p.end)) {
        auto tstart = std::chrono::system_clock::now();
        search::index_part in(p.path, htcap, p.start, p.end);
        in.load();
        auto tmid = std::chrono::system_clock::now();

        out_word.merge(in);
        auto tend = std::chrono::system_clock::now();

        load_total += tmid - tstart;
        merge_total += tend - tmid;
      }
    }

    for (auto &p: index.pair_parts) {
      if ((!end || p.start < *end) && (!p.end || start < *p.end)) {
        auto tstart = std::chrono::system_clock::now();
        search::index_part in(p.path, htcap,  p.start, p.end);
        in.load();

        auto tmid = std::chrono::system_clock::now();
        out_pair.merge(in);
        auto tend = std::chrono::system_clock::now();

        load_total += tmid - tstart;
        merge_total += tend - tmid;
      }
    }

    for (auto &p: index.trine_parts) {
      if ((!end || p.start < *end) && (!p.end || start < *p.end)) {
        auto tstart = std::chrono::system_clock::now();
        search::index_part in(p.path, htcap,  p.start, p.end);
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

  out_word.save(buf, buf_len);
  out_pair.save(buf, buf_len);
  out_trine.save(buf, buf_len);

  free(buf);

  auto tend = std::chrono::system_clock::now();
  spdlog::info("saved");

  save_total = tend - tstart;

  id >> done_channel;
}

}

