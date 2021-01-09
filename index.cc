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

extern "C" {

#include "str.h"
#include "x_cocomel/dynamic_array_kv_64.h"
#include "x_cocomel/posting.h"
#include "x_cocomel/hash_table.h"
}

#include "util.h"
#include "crawl.h"
#include "tokenizer.h"

void index_write(char const *filename, char *buffer,
    struct dynamic_array_kv_64 *docNos,
    struct hash_table *dictionary,
    struct hash_table *dictionary_pair)
{
  FILE *fh = fopen(filename, "w");
  if (fh == NULL) {
    fprintf(stderr, "ERROR: Failed to open %s. for writing\n", filename);
    exit(1);
  }

  // Write to output buffer
  uint32_t offset = sizeof(uint32_t) * 3;

  ((uint32_t *)buffer)[0] = docNos->length;

  uint32_t docNos_offset = offset;
	offset += docNos->length * sizeof(uint32_t) * 2;
	for (size_t i = 0; i < docNos->length; i++) {

		((uint32_t *)&buffer[docNos_offset])[0] = offset;
		((uint32_t *)&buffer[docNos_offset])[1] = dynamic_array_kv_64_at(docNos, i)[1];
		docNos_offset += sizeof(uint32_t) * 2;

		*((uint64_t *)&buffer[offset]) = dynamic_array_kv_64_at(docNos, i)[0];
    offset += sizeof(uint64_t);
	}

	((uint32_t *)buffer)[1] = offset;
  offset += hash_table_write(dictionary, &buffer[offset]);

  ((uint32_t *)buffer)[2] = offset;
  offset += hash_table_write(dictionary_pair, &buffer[offset]);

	fwrite(buffer, sizeof(char), offset, fh);

	fclose(fh);
}

int main(int argc, char *argv[]) {
  crawl::index index;
  index.load("index.scrape");

	struct dynamic_array_kv_64 docNos;
	dynamic_array_kv_64_init(&docNos);

  struct hash_table dictionary;
	hash_table_init(&dictionary);

  struct hash_table dictionary_pair;
	hash_table_init(&dictionary_pair);

  size_t max_size = 1024 * 1024 * 10;
  char *buf = (char *) malloc(max_size);

  const size_t buf_len = 512;

  char tok_buffer_store[buf_len]; // Provide underlying storage for tok_buffer
	struct str tok_buffer;
	str_init(&tok_buffer, tok_buffer_store, sizeof(tok_buffer_store));

  char pair_buffer[buf_len * 2 + 1];
	struct str tok_buffer_pair;
	str_init(&tok_buffer_pair, pair_buffer, sizeof(pair_buffer));

  char prev_buffer[buf_len];

	tokenizer::token_type token;
  tokenizer::tokenizer tok;

  int i = 0;

  for (auto &site: index.sites) {
    printf("site %lu %s\n", site.id, site.host.c_str());
    for (auto &page: site.pages) {
      if (!page.valid) continue;

      uint64_t id = crawl::page_id(site.id, page.id).to_value();

      std::ifstream pfile;

      pfile.open(page.path, std::ios::in | std::ios::binary);

      if (!pfile.is_open() || pfile.fail() || !pfile.good() || pfile.bad()) {
        fprintf(stderr, "error opening file %s\n", page.path.c_str());
        continue;
      }

      pfile.read(buf, max_size);

      size_t len = pfile.gcount();

			tok.init(buf, len);

		  dynamic_array_kv_64_append(&docNos, id, 0);

      bool in_head = false, in_title = false;
      bool break_pair = false;

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

          } else if (t == "title") {
            in_title = true;

          } else if (t == "/title") {
            in_title = false;
          }

          if (t == "a" || t == "strong") {
            break_pair = false;
          } else {
            break_pair = true;
          }

        } else if ((in_title || !in_head) && token == tokenizer::WORD) {
					dynamic_array_kv_64_back(&docNos)[1]++;

          str_tolower(&tok_buffer);

          hash_table_insert(&dictionary, &tok_buffer, docNos.length);

          if (!break_pair) {
            str_resize(&tok_buffer_pair, 0);
            str_cat(&tok_buffer_pair, prev_buffer);
            str_cat(&tok_buffer_pair, " ");
            str_cat(&tok_buffer_pair, str_c(&tok_buffer));

            hash_table_insert(&dictionary_pair, &tok_buffer_pair, docNos.length);
          }

          break_pair = false;
          strcpy(prev_buffer, str_c(&tok_buffer));
				}
			} while (token != tokenizer::END);

      pfile.close();
    }
  }

  printf("saving index\n");

  char *out_buffer = (char *)malloc(1024*1024*1024);
  index_write("index.dat", out_buffer, &docNos, &dictionary, &dictionary_pair);

  return 0;
}

