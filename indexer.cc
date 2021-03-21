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

#include "hash_table.h"
#include "index.h"

using nlohmann::json;

void index_site(search::indexer &indexer, crawl::site &site) {
  spdlog::info("index site {}", site.host);

  char *file_buf = (char *) malloc(scrape::max_file_size);

  const size_t buf_len = 80;

  char tok_buffer_store[buf_len]; // Provide underlying storage for tok_buffer
	struct str tok_buffer;
	str_init(&tok_buffer, tok_buffer_store, sizeof(tok_buffer_store));

  char pair_buffer[buf_len * 2 + 1];
	struct str tok_buffer_pair;
	str_init(&tok_buffer_pair, pair_buffer, sizeof(pair_buffer));

  char trine_buffer[buf_len * 3 + 2];
	struct str tok_buffer_trine;
	str_init(&tok_buffer_trine, trine_buffer, sizeof(trine_buffer));

	tokenizer::token_type token;
  tokenizer::tokenizer tok;

  spdlog::info("process pages for {}", site.host);
  for (auto &page: site.pages) {
    if (page.last_scanned == 0) continue;

    uint64_t page_id = crawl::page_id(site.id, page.id).to_value();
    uint32_t index_id = indexer.pages.size();

    std::ifstream pfile;

    pfile.open(page.path, std::ios::in | std::ios::binary);

    if (!pfile.is_open() || pfile.fail() || !pfile.good() || pfile.bad()) {
      spdlog::warn("error opening file {}", page.path);
      continue;
    }

    pfile.read(file_buf, scrape::max_file_size);

    spdlog::debug("process page {} / {} : {}", page_id, index_id, page.url);
    size_t len = pfile.gcount();

    if (indexer.usage > 1024 * 1024 * 200) {
      spdlog::info("indexer using {}", indexer.usage);
      indexer.flush();
    }

    tok.init(file_buf, len);

    bool in_head = false, in_title = false;

    str_resize(&tok_buffer_pair, 0);
    str_resize(&tok_buffer_trine, 0);

    size_t page_length = 0;

    do {
      token = tok.next(&tok_buffer);

      if (token == tokenizer::TAG) {
        char tag_name[tokenizer::tag_name_max_len];
        tokenizer::get_tag_name(tag_name, str_c(&tok_buffer));

        auto t = std::string(tag_name);

        if (t == "head") {
          in_head = true;

        } else if (t == "/head") {
          in_head = false;

        } else if (in_head && t == "title") {
          in_title = true;

        } else if (in_head && t == "/title") {
          in_title = false;
        }

        // TODO: others
        if (t != "a" && t != "strong") {
          str_resize(&tok_buffer_pair, 0);
          str_resize(&tok_buffer_trine, 0);
        }

      } else if ((in_title || !in_head) && token == tokenizer::WORD) {
        str_tolower(&tok_buffer);
        str_tostem(&tok_buffer);

        std::string s(str_c(&tok_buffer));

        page_length++;

        indexer.insert(search::words, s, index_id);

        if (str_length(&tok_buffer_trine) > 0) {
          str_cat(&tok_buffer_trine, " ");
          str_cat(&tok_buffer_trine, str_c(&tok_buffer));

          std::string s(str_c(&tok_buffer_trine));

          indexer.insert(search::trines, s, index_id);

          str_resize(&tok_buffer_trine, 0);
        }

        if (str_length(&tok_buffer_pair) > 0) {
          str_cat(&tok_buffer_pair, " ");
          str_cat(&tok_buffer_pair, str_c(&tok_buffer));

          std::string s(str_c(&tok_buffer_pair));

          indexer.insert(search::pairs, s, index_id);

          str_cat(&tok_buffer_trine, str_c(&tok_buffer_pair));
        }

        str_resize(&tok_buffer_pair, 0);
        str_cat(&tok_buffer_pair, str_c(&tok_buffer));
      }
    } while (token != tokenizer::END);

    pfile.close();

    indexer.pages.emplace_back(page_id, page_length);
  }

  free(file_buf);

  spdlog::info("finished indexing site {}", site.host);
}

void
indexer_run(Channel<std::string*> &in,
    Channel<bool> &out_ready,
    Channel<std::string*> &out,
    int tid)
{
  spdlog::info("thread {} started", tid);

  search::indexer indexer(fmt::format("meta/index_parts/index.{}", tid));

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
    index_site(indexer, site);
    spdlog::info("{} done  {}", tid, site.host);
  }

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
  crawl::crawler crawler;
  crawler.load();

  util::make_path("meta/index_parts");

  auto n_threads = std::thread::hardware_concurrency();
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
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
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

