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

void index_write(crawl::site &s, std::string part, hash_table &dict)
{
  std::string path = s.host + ".index." + part + ".dat";
  printf("write %s\n", path.c_str());

  uint8_t *buffer = (uint8_t *) malloc(1024 * 1024 * 512);
  size_t len = search::index_save(dict, buffer);

  std::ofstream file;

  file.open(path, std::ios::out | std::ios::binary | std::ios::trunc);

  if (!file.is_open()) {
    fprintf(stderr, "error opening file %s\n", path.c_str());
    return;
  }

  printf("writing %i bytes\n", len);
  file.write((const char *) buffer, len);

  file.close();

  free(buffer);
}

void index_site(crawl::site &s) {
  printf("index site %s\n", s.host.c_str());

  size_t max_size = 1024 * 1024 * 10;
  char *file_buf = (char *) malloc(max_size);

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

  hash_table words, pairs, trines;

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

    size_t len = pfile.gcount();

    tok.init(file_buf, len);

    bool in_head = false, in_title = false;

    str_resize(&tok_buffer_pair, 0);
    str_resize(&tok_buffer_trine, 0);

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

        words.insert(s, id);

        if (str_length(&tok_buffer_trine) > 0) {
          str_cat(&tok_buffer_trine, " ");
          str_cat(&tok_buffer_trine, str_c(&tok_buffer));

          std::string s(str_c(&tok_buffer_trine));

          trines.insert(s, id);

          str_resize(&tok_buffer_trine, 0);
        }

        if (str_length(&tok_buffer_pair) > 0) {
          str_cat(&tok_buffer_pair, " ");
          str_cat(&tok_buffer_pair, str_c(&tok_buffer));

          std::string s(str_c(&tok_buffer_pair));

          pairs.insert(s, id);

          str_cat(&tok_buffer_trine, str_c(&tok_buffer_pair));
        }

        str_resize(&tok_buffer_pair, 0);
        str_cat(&tok_buffer_pair, str_c(&tok_buffer));
      }
    } while (token != tokenizer::END);

    pfile.close();
  }

  free(file_buf);

  printf("finished indexing site %s\n", s.host.c_str());
  index_write(s, "words", words);
  index_write(s, "pairs", pairs);
  index_write(s, "trines", trines);
}

int main(int argc, char *argv[]) {
  crawl::crawler crawler;
  crawler.load();

  int i = 0;

  for (auto &s: crawler.sites) {
    if (!s.enabled) continue;

    printf("site %lu %s\n", s.id, s.host.c_str());

    s.load();

    index_site(s);

    s.unload();
  }

/*
  printf("saving index\n");

  char *out_buffer = (char *)malloc(1024*1024*1024);
  index_write("index.dat", out_buffer, &docNos, &dictionary, &dictionary_pair, &dictionary_trine);
*/
  return 0;
}

