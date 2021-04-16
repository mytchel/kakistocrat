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

namespace search {

void
indexer_run(
    config settings,
    Channel<std::string*> &in,
    Channel<bool> &out_ready,
    Channel<std::string*> &out,
    int tid)
{
  spdlog::info("thread {} started", tid);

  util::make_path(fmt::format("{}/{}",
        settings.indexer.parts_path, tid));

  search::indexer indexer(
      fmt::format("{}/{}/part", settings.indexer.parts_path, tid),
      search::get_split_at(settings.index_parts),
      settings.indexer.htcap,
      settings.indexer.thread_max_mem
        - settings.indexer.max_index_part_size
        - settings.crawler.max_page_size,
      settings.indexer.max_index_part_size);

  size_t file_buf_len = settings.crawler.max_page_size;
  char *file_buf = (char *) malloc(file_buf_len);
  if (file_buf == NULL) {
    throw std::bad_alloc();
  }

  while (true) {
    true >> out_ready;

    std::string *path;
    path << in;

    if (path == NULL) {
      spdlog::info("{} got fin", tid);
      break;
    }

    crawl::site site(*path);

    spdlog::info("{} load  {}", tid, site.host);
    site.load();

    spdlog::info("{} index {}", tid, site.host);
    indexer.index_site(site, file_buf, file_buf_len);

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

}

