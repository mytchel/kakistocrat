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

#include "x_cocomel/dynamic_array_kv_64.h"
#include "x_cocomel/posting.h"
#include "x_cocomel/hash_table.h"
}

#include "util.h"
#include "crawl.h"
#include "tokenizer.h"

void index_write(char const *filename, char *buffer, struct dynamic_array_kv_64 *docNos, struct hash_table *dictionary)
{
  FILE *fh = fopen(filename, "w");
  if (fh == NULL) {
    fprintf(stderr, "ERROR: Failed to open %s. for writing\n", filename);
    exit(1);
  }

  // Write to output buffer
  uint32_t offset = sizeof(uint32_t) * 2;

  ((uint32_t *)buffer)[1] = docNos->length;
  uint32_t docNos_offset = offset;
	offset += docNos->length * sizeof(uint32_t) * 2;
	for (size_t i = 0; i < docNos->length; i++) {

		((uint32_t *)&buffer[docNos_offset])[0] = offset;
		((uint32_t *)&buffer[docNos_offset])[1] = dynamic_array_kv_64_at(docNos, i)[1];
		docNos_offset += sizeof(uint32_t) * 2;

		*((uint64_t *)&buffer[offset]) = dynamic_array_kv_64_at(docNos, i)[0];
    offset += sizeof(uint64_t);
	}

	((uint32_t *)buffer)[0] = offset;
  offset += hash_table_write(dictionary, &buffer[offset]);

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

  size_t max_size = 1024 * 1024 * 10;
  char *buf = (char *) malloc(max_size);

  char tok_buffer_store[516]; // Provide underlying storage for tok_buffer
	struct str tok_buffer;
	tok_buffer.store = tok_buffer_store;
	tokenizer::token_type token;

  tokenizer::tokenizer tok;

  int i = 0;

  for (auto &site: index.sites) {
    printf("site %lu %s\n", site.id, site.host.c_str());
    for (auto &page: site.pages) {
      if (!page.scraped) continue;

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

      do {
				token = tok.next(tok_buffer);

        if (token == tokenizer::TAG) {
          char tag_name[32];
          tokenizer::get_tag_name(tag_name, str_c(tok_buffer));
          if (tokenizer::should_skip_tag(tag_name)) {
            tok.skip_tag(tag_name, tok_buffer);
          }

        } else if (token == tokenizer::WORD) {
					dynamic_array_kv_64_back(&docNos)[1]++;
					hash_table_insert(&dictionary, tok_buffer, docNos.length);
				}
			} while (token != tokenizer::END);

      pfile.close();
    }
  }

  printf("saving index\n");

  char *out_buffer = (char *)malloc(1024*1024*1024);
  index_write("index.dat", out_buffer, &docNos, &dictionary);

  return 0;
}

