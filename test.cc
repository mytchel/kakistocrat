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

#include "util.h"
#include "scrape.h"
#include "crawl.h"

extern "C" {

#include "x_cocomel/dynamic_array_kv_64.h"
#include "x_cocomel/posting.h"
#include "x_cocomel/hash_table.h"
#include "x_cocomel/tokenizer.h"
}

void get_tag_name(char *buf, char *s) {
  size_t i;
  for (i = 0; i < 31; i++) {
    char c = s[i];
    if (c == '\0' || c == ' ' || c == '\t') break;
    else buf[i] = c;
  }

  buf[i] = '\0';
}

bool should_skip_tag(char *t) {
  return strcmp(t, "script") == 0 ||
         strcmp(t, "style") == 0 ||
         strcmp(t, "head") == 0;
}

void skip_tag(char *tag_name_main, tokenizer *tok, str tok_buffer) {
	enum token_type token;

  char tag_name_end[33];
  tag_name_end[0] = '/';
  strcpy(tag_name_end + 1, tag_name_main);

  int i = 0;
  do {
    token = tokenizer_next(tok, tok_buffer);

    if (token == TAG) {
      char tag_name[32];
      get_tag_name(tag_name, str_c(tok_buffer));

      if (strcmp(tag_name_end, tag_name) == 0) {
        break;

      } else if (should_skip_tag(tag_name)) {
        skip_tag(tag_name, tok, tok_buffer);
      }
    }
  } while (token != END);
}

void index_write(char const *filename, char *buffer, struct dynamic_array_kv_64 *docNos, struct hash_table *dictionary)
{
  FILE *fh = fopen(filename, "w");
  if (fh == NULL) {
    fprintf(stderr, "ERROR: Failed to open index.dat for writing\n");
    exit(1);
  }

  // Write to output buffer
  uint32_t offset = sizeof(uint32_t) * 2;

  printf("save docnos map\n");

  ((uint32_t *)buffer)[1] = docNos->length;
  uint32_t docNos_offset = offset;
	offset += docNos->length * sizeof(uint32_t) * 2;
	for (size_t i = 0; i < docNos->length; i++) {

    printf("save docno %i (offset = 0x%x)\n", i, offset);

		((uint32_t *)&buffer[docNos_offset])[0] = offset;
		((uint32_t *)&buffer[docNos_offset])[1] = dynamic_array_kv_64_at(docNos, i)[1];
		docNos_offset += sizeof(uint32_t) * 2;

    printf("write name 0x%p (offset = 0x%x)\n", (char *)dynamic_array_kv_64_at(docNos, i)[0], offset);
		((uint64_t *)&buffer[offset] = dynamic_array_kv_64_at(docNos, i)[0];
    offset += sizeof(uint64_t);
	}

  printf("save dictionary\n");

	((uint32_t *)buffer)[0] = offset;
  offset += hash_table_write(dictionary, &buffer[offset]);

	fwrite(buffer, sizeof(char), offset, fh);
	fclose(fh);
}

int main(int argc, char *argv[]) {
  crawl::index index;
  crawl::load_index(index, "full_index");

  std::ofstream file;

  std::string path = "cocomel_list";
  file.open(path, std::ios::out | std::ios::trunc | std::ios::binary);

  if (!file.is_open()) {
    fprintf(stderr, "error opening file %s\n", path.c_str());
    return 1;
  }

	struct dynamic_array_kv_64 docNos;
	dynamic_array_kv_64_init(&docNos);

  struct hash_table dictionary;
	hash_table_init(&dictionary);

  size_t max_size = 1024 * 1024 * 10;
  char *buf = (char *) malloc(max_size);

  char tok_buffer_store[516]; // Provide underlying storage for tok_buffer
	struct str tok_buffer;
	tok_buffer.store = tok_buffer_store;
	enum token_type token;

  struct tokenizer tok;

  int i = 0;

  for (auto &site: index.sites) {
    if (i > 10000) break;

    printf("site %lu %s\n", site.id, site.host.c_str());
    for (auto &page: site.pages) {
      uint64_t id = ((uint64_t) site.id) << 32 | page.id;

      i++;

      std::ifstream pfile;

      pfile.open(page.path, std::ios::in | std::ios::binary);

      if (!pfile.is_open() || pfile.fail() || !pfile.good() || pfile.bad()) {
        fprintf(stderr, "error opening file %s\n", page.path.c_str());
        continue;
      }

      std::streamsize size = pfile.tellg();
      pfile.seekg(0, std::ios::beg);

      pfile.read(buf, max_size);

      size_t len = pfile.gcount();

			tokenizer_init(&tok, buf, len);

		  dynamic_array_kv_64_append(&docNos, id, 0);

      do {
				token = tokenizer_next(&tok, tok_buffer);

        if (token == TAG) {
          char tag_name[32];
          get_tag_name(tag_name, str_c(tok_buffer));
          if (should_skip_tag(tag_name)) {
            skip_tag(tag_name, &tok, tok_buffer);
          }

        } else if (token == WORD) {
					dynamic_array_kv_64_back(&docNos)[1]++;
					hash_table_insert(&dictionary, tok_buffer, docNos.length);
				}
			} while (token != END);

      pfile.close();
    }
  }

  file.close();

  printf("saving index\n");

  char *out_buffer = (char *)malloc(1024*1024*1024);
  index_write("index.dat", out_buffer, &docNos, &dictionary);

  return 0;
}

