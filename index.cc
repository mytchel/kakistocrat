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

extern "C" {

#include "str.h"
/*
#include "x_cocomel/dynamic_array_kv_64.h"
#include "x_cocomel/hash_table.h"
*/
}

#include "util.h"
#include "crawl.h"
#include "tokenizer.h"

#include "posting.h"
#include "bst.h"
#include "hash_table.h"

using nlohmann::json;

/*
void index_write(char const *filename, char *buffer,
    struct dynamic_array_kv_64 *docNos,
    struct hash_table *dictionary,
    struct hash_table *dictionary_pair,
    struct hash_table *dictionary_trine)
{
  FILE *fh = fopen(filename, "w");
  if (fh == NULL) {
    fprintf(stderr, "ERROR: Failed to open %s. for writing\n", filename);
    exit(1);
  }

  // Write to output buffer
  uint32_t offset = sizeof(uint32_t) * 4;

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

  ((uint32_t *)buffer)[3] = offset;
  offset += hash_table_write(dictionary_trine, &buffer[offset]);

	fwrite(buffer, sizeof(char), offset, fh);

	fclose(fh);
}
*/

void index_write(crawl::site &s,
  std::map<std::string, std::list<std::uint64_t>> dict)
  //std::map<std::string, std::vector<std::uint64_t>> dict_pair,
  //std::map<std::string, std::vector<std::uint64_t>> dict_trine)
{
  std::string path = s.host + ".dat.json";
  printf("write %s\n", path.c_str());

  json j = {
    {"words", dict}};
    //{"pairs", dict_pair},
    //{"trines", dict_trine}};

  std::ofstream file;

  file.open(path, std::ios::out | std::ios::trunc);

  if (!file.is_open()) {
    fprintf(stderr, "error opening file %s\n", path.c_str());
    return;
  }

  file << j;

  file.close();
}

void index_site(crawl::site &s) {
  printf("index site %s\n", s.host.c_str());

  size_t max_size = 1024 * 1024 * 10;
  char *file_buf = (char *) malloc(max_size);

  const size_t buf_len = 512;

  char tok_buffer_store[buf_len]; // Provide underlying storage for tok_buffer
	struct str tok_buffer;
	str_init(&tok_buffer, tok_buffer_store, sizeof(tok_buffer_store));

  /*
  char pair_buffer[buf_len * 2 + 1];
	struct str tok_buffer_pair;
	str_init(&tok_buffer_pair, pair_buffer, sizeof(pair_buffer));

  char trine_buffer[buf_len * 3 + 2];
	struct str tok_buffer_trine;
	str_init(&tok_buffer_trine, trine_buffer, sizeof(trine_buffer));
*/
	tokenizer::token_type token;
  tokenizer::tokenizer tok;

  hash_table dict;

  //std::map<std::string, std::vector<std::uint64_t>> dict_pair;
  //std::map<std::string, std::vector<std::uint64_t>> dict_trine;

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

    //str_resize(&tok_buffer_pair, 0);
    //str_resize(&tok_buffer_trine, 0);

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
        //  str_resize(&tok_buffer_pair, 0);
        //  str_resize(&tok_buffer_trine, 0);
        }

      } else if ((in_title || !in_head) && token == tokenizer::WORD) {
        str_tolower(&tok_buffer);
        str_tostem(&tok_buffer);

        std::string s(str_c(&tok_buffer));

        dict.insert(s, id);

        /*
        if (str_length(&tok_buffer_trine) > 0) {
          str_cat(&tok_buffer_trine, " ");
          str_cat(&tok_buffer_trine, str_c(&tok_buffer));

          std::string s(str_c(&tok_buffer_trine));

          auto it = dict_trine.try_emplace(s, id);
          if (!it.second) it.first->second.push_back(id);

          str_resize(&tok_buffer_trine, 0);
        }

        if (str_length(&tok_buffer_pair) > 0) {
          str_cat(&tok_buffer_pair, " ");
          str_cat(&tok_buffer_pair, str_c(&tok_buffer));

          std::string s(str_c(&tok_buffer_pair));

          auto it = dict_pair.try_emplace(s, id);
          if (!it.second) it.first->second.push_back(id);

          str_cat(&tok_buffer_trine, str_c(&tok_buffer_pair));
        }

        str_resize(&tok_buffer_pair, 0);
        str_cat(&tok_buffer_pair, str_c(&tok_buffer));

        */
      }
    } while (token != tokenizer::END);

    pfile.close();
  }

  free(file_buf);

  printf("finished indexing site %s\n", s.host.c_str());
  //index_write(s, dict);//, dict_pair, dict_trine);
}

int main(int argc, char *argv[]) {
  crawl::index index;
  index.load();

  int i = 0;

  for (auto &s: index.sites) {
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

