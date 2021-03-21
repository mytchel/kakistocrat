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

void index_site(crawl::site &s, search::indexer &indexer) {
  spdlog::info("index site {}", s.host);

  char *file_buf = (char *) malloc(scrape::max_file_size);

  const size_t buf_len = 512;

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

  for (auto &page: s.pages) {
    if (page.last_scanned == 0) continue;

    uint64_t page_id = crawl::page_id(s.id, page.id).to_value();
    uint32_t index_id = indexer.pages.size();

    std::ifstream pfile;

    pfile.open(page.path, std::ios::in | std::ios::binary);

    if (!pfile.is_open() || pfile.fail() || !pfile.good() || pfile.bad()) {
      spdlog::warn("error opening file {}", page.path);
      continue;
    }

    pfile.read(file_buf, scrape::max_file_size);

    spdlog::debug("process page {} : {}", page_id, page.url);
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

        indexer.words.insert(s, index_id);

        if (str_length(&tok_buffer_trine) > 0) {
          str_cat(&tok_buffer_trine, " ");
          str_cat(&tok_buffer_trine, str_c(&tok_buffer));

          std::string s(str_c(&tok_buffer_trine));

          indexer.trines.insert(s, index_id);

          str_resize(&tok_buffer_trine, 0);
        }

        if (str_length(&tok_buffer_pair) > 0) {
          str_cat(&tok_buffer_pair, " ");
          str_cat(&tok_buffer_pair, str_c(&tok_buffer));

          std::string s(str_c(&tok_buffer_pair));

          indexer.pairs.insert(s, index_id);

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

  spdlog::info("finished indexing site {}", s.host);
}

int main(int argc, char *argv[]) {
  crawl::crawler crawler;
  crawler.load();

  search::indexer indexer;

  auto site = crawler.sites.begin();
  while (site != crawler.sites.end()) {
    if (site->last_scanned == 0) {
      site++;
      continue;
    }

    site->load();

    index_site(*site, indexer);

    site->flush();
    site++;
  }

  indexer.save("meta/full");

  return 0;
}

