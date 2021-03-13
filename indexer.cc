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

#include "channel.h"
#include "util.h"
#include "scrape.h"
#include "crawl.h"
#include "tokenizer.h"

#include "hash_table.h"
#include "index.h"

using nlohmann::json;

void index_site(crawl::site &s) {
  printf("index site %s\n", s.host.c_str());

  size_t max_size = 1024 * 1024 * 10;
  char *file_buf = (char *) malloc(max_size);

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

  search::indexer indexer;

  for (auto &page: s.pages) {
    if (!page.valid) continue;

    uint64_t id = crawl::page_id(s.id, page.id).to_value();

    std::ifstream pfile;

    pfile.open(page.path, std::ios::in | std::ios::binary);

    if (!pfile.is_open() || pfile.fail() || !pfile.good() || pfile.bad()) {
      fprintf(stderr, "error opening file %s\n", page.path.c_str());
      continue;
    }

    pfile.read(file_buf, max_size);

    printf("process page %lu : %s\n", id, page.url.c_str());
    size_t len = pfile.gcount();

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

        page_length++;

        std::string s(str_c(&tok_buffer));

        indexer.words.insert(s, id);

        if (str_length(&tok_buffer_trine) > 0) {
          str_cat(&tok_buffer_trine, " ");
          str_cat(&tok_buffer_trine, str_c(&tok_buffer));

          std::string s(str_c(&tok_buffer_trine));

          indexer.trines.insert(s, id);

          str_resize(&tok_buffer_trine, 0);
        }

        if (str_length(&tok_buffer_pair) > 0) {
          str_cat(&tok_buffer_pair, " ");
          str_cat(&tok_buffer_pair, str_c(&tok_buffer));

          std::string s(str_c(&tok_buffer_pair));

          indexer.pairs.insert(s, id);

          str_cat(&tok_buffer_trine, str_c(&tok_buffer_pair));
        }

        str_resize(&tok_buffer_pair, 0);
        str_cat(&tok_buffer_pair, str_c(&tok_buffer));
      }
    } while (token != tokenizer::END);

    pfile.close();

    indexer.page_lengths.emplace(id, page_length);
  }

  free(file_buf);

  printf("finished indexing site %s\n", s.host.c_str());

  std::string path = "meta/sites/" + util::host_hash(s.host) + "/" + s.host;

  indexer.save(path);
}

void
indexer_run(Channel<std::string*> &in, Channel<std::string*> &out, int tid)
{
  printf("thread %i started\n", tid);

  std::string *b = NULL;
  b >> out;

  while (true) {
    std::string *name;

    name << in;

    if (name == NULL) {
      break;
    }

    printf("%i start on %s\n", tid, name->c_str());

    std::string path = "meta/sites/" + util::host_hash(*name) + "/" + *name;

    crawl::site site(path);
    site.load();

    index_site(site);

    name >> out;
  }
}

int main(int argc, char *argv[]) {
  crawl::crawler crawler;
  crawler.load();

  auto n_threads = std::thread::hardware_concurrency();

  printf("starting %i threads\n", n_threads);

  Channel<std::string*> merger_channel;
  Channel<std::string*> in_channels[n_threads];
  Channel<std::string*> out_channels[n_threads];

  std::vector<std::thread> threads;

  for (size_t i = 0; i < n_threads; i++) {
    auto th = std::thread(
        [](Channel<std::string*> &in,
           Channel<std::string*> &out, int i) {
          indexer_run(in, out, i);
        },
        std::ref(in_channels[i]),
        std::ref(out_channels[i]), i);

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
      if (!out_channels[i].empty()) {
        found = true;

        if (site != crawler.sites.end()) {
          &site->host >> in_channels[i];
          site++;
        }

        std::string *host;

        host << out_channels[i];
      }
    }

    if (!found) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
  }

  std::string *b = NULL;
  for (size_t i = 0; i < n_threads; i++) {
    b >> in_channels[i];
  }

  printf("wait for threads\n");

  for (auto &t: threads) {
    t.join();
  }

  return 0;
}

