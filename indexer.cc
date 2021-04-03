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

#include <nlohmann/json.hpp>
#include "spdlog/spdlog.h"

#include "channel.h"
#include "util.h"
#include "scrape.h"
#include "crawl.h"
#include "tokenizer.h"

#include "index.h"

using nlohmann::json;

void
indexer_run(Channel<std::string*> &in,
    Channel<bool> &out_ready,
    Channel<std::string*> &out,
    int tid)
{
  spdlog::info("thread {} started", tid);

  util::make_path(fmt::format("meta/index_parts/{}", tid));

  search::indexer indexer(fmt::format("meta/index_parts/{}/part", tid),
      search::get_split_at(4));

  char *file_buf = (char *) malloc(scrape::max_file_size);
  if (file_buf == NULL) {
    throw std::bad_alloc();
  }

  while (true) {
    true >> out_ready;

    std::string *name;
    name << in;

    if (name == NULL) {
      spdlog::info("{} got fin", tid);
      break;
    }

    crawl::site site(*name);
    spdlog::info("{} load  {}", tid, site.host);
    site.load();

    spdlog::info("{} index {}", tid, site.host);
    indexer.index_site(site, file_buf, scrape::max_file_size);
    spdlog::info("{} done  {}", tid, site.host);
  }

  free(file_buf);

  spdlog::info("indexer using {}", indexer.usage());

  indexer.flush();

  for (auto &p: indexer.paths) {
    &p >> out;
  }

  spdlog::info("{} send fin", tid);
  std::string *end = NULL;
  end >> out;

  spdlog::info("{} got fin ack", tid);
  std::string *sync;
  sync << in;
}

int main(int argc, char *argv[]) {
  spdlog::info("loading");

  crawl::crawler crawler;
  crawler.load();

  util::make_path("meta/index_parts");

  auto n_threads = 1;//std::thread::hardware_concurrency();
  if (n_threads > 1) n_threads--;

  spdlog::info("starting {} threads", n_threads);

  Channel<std::string*> in_channels[n_threads];
  Channel<bool> out_ready_channels[n_threads];
  Channel<std::string*> out_channels[n_threads];

  std::vector<std::thread> threads;

  for (size_t i = 0; i < n_threads; i++) {
    auto th = std::thread(
        indexer_run,
        std::ref(in_channels[i]),
        std::ref(out_ready_channels[i]),
        std::ref(out_channels[i]),
        i);

    threads.emplace_back(std::move(th));
  }

  auto site = crawler.sites.begin();
  while (site != crawler.sites.end()) {
    if (site->last_scanned == 0) {
      site++;
      continue;
    }

    bool found = false;
    for (size_t i = 0; !found && i < n_threads; i++) {
      if (!out_ready_channels[i].empty()) {
        found = true;

        if (site != crawler.sites.end()) {
          &site->host >> in_channels[i];
          site++;
        }

        bool ready;
        ready << out_ready_channels[i];
      }
    }

    if (!found) {
      std::this_thread::sleep_for(10ms);
    }
  }

  std::string *sync = NULL;
  for (size_t i = 0; i < n_threads; i++) {
    sync >> in_channels[i];
  }

  std::list<std::string> index_parts;
  for (size_t i = 0; i < n_threads; i++) {
    spdlog::info("get out from {}", i);
    std::string *s;
    do {
      s << out_channels[i];
      if (s != NULL) {
        spdlog::info("got {} from {}", *s, i);
        index_parts.push_back(*s);
      }
    } while (s != NULL);

    sync >> in_channels[i];
  }

  spdlog::info("wait for threads");

  search::save_parts("meta/index_parts.json", index_parts);

  for (auto &t: threads) {
    t.join();
  }

  return 0;
}

