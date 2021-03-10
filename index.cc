#include <string>
#include <cstring>
#include <map>
#include <utility>
#include <algorithm>
#include <vector>
#include <iostream>
#include <fstream>
#include <sstream>
#include <cstdint>

#include "posting.h"
#include "bst.h"
#include "hash_table.h"
#include "index.h"
#include "tokenizer.h"

using nlohmann::json;

namespace search {

void to_json(nlohmann::json &j, const index_part_info &i)
{
  j = json{{"path", i.path}};
}

void from_json(const nlohmann::json &j, index_part_info &i)
{
  j.at("path").get_to(i.path);
}

void to_json(nlohmann::json &j, const index_info &i)
{
  j = json{
      {"page_lengths", i.page_lengths},
      {"word_parts", i.word_parts},
      {"pair_parts", i.pair_parts},
      {"trine_parts", i.trine_parts}};
}

void from_json(const nlohmann::json &j, index_info &i)
{
  j.at("page_lengths").get_to(i.page_lengths);
  j.at("word_parts").get_to(i.word_parts);
  j.at("pair_parts").get_to(i.pair_parts);
  j.at("trine_parts").get_to(i.trine_parts);
}

static size_t hash_to_buf(hash_table &t, uint8_t *buffer)
{
  size_t offset = sizeof(uint32_t);

  size_t lists = 0;
  size_t n_postings = 0;

  for (size_t i = 0; i < HTCAP; i++) {
    if (t.store[i]) {
      lists++;

      std::map<std::string, posting> postings;

      t.store[i]->get_postings(postings);

	    ((uint32_t *) (buffer + offset))[0] = postings.size();

      offset += sizeof(uint32_t);

      for (auto &p: postings) {
        memcpy(buffer + offset, p.first.c_str(), p.first.size());
        offset += p.first.size();
        buffer[offset++] = 0;

        offset += p.second.save(buffer + offset);

        n_postings++;
      }
    }
  }

  ((uint32_t *) buffer)[0] = lists;

  printf("have %i lists of %i postings\n",
      lists, n_postings);

  return offset;
}

void write_buf(std::string path, uint8_t *buf, size_t len)
{
  printf("write %s\n", path.c_str());

  std::ofstream file;

  file.open(path, std::ios::out | std::ios::binary | std::ios::trunc);

  if (!file.is_open()) {
    fprintf(stderr, "error opening file %s\n", path.c_str());
    return;
  }

  printf("writing %i bytes\n", len);
  file.write((const char *) buf, len);

  file.close();
}

void index_part_save(hash_table &t, std::string path)
{
  printf("write %s\n", path.c_str());

  uint8_t *buffer = (uint8_t *) malloc(1024 * 1024 * 1024);

  size_t len = hash_to_buf(t, buffer);

  write_buf(path, buffer, len);

  free(buffer);
}

void indexer::save(std::string base_path)
{
  printf("indexer save %s\n", base_path.c_str());

  auto words_path = base_path + ".words.dat";
  auto pairs_path = base_path + ".pairs.dat";
  auto trines_path = base_path + ".trines.dat";

  index_part_save(words, words_path);
  index_part_save(pairs, pairs_path);
  index_part_save(trines, trines_path);

  printf("indexer save meta data for %s\n", base_path.c_str());

  index_info info;

  info.page_lengths = page_lengths;

  info.word_parts.emplace_back(words_path);
  info.pair_parts.emplace_back(pairs_path);
  info.trine_parts.emplace_back(trines_path);

  printf("to json\n");
  json j = info;
  printf("open file\n");

  std::string path = base_path + ".meta.json";
  std::ofstream file;

  file.open(path, std::ios::out | std::ios::trunc);

  if (!file.is_open()) {
    fprintf(stderr, "error opening file %s\n", path.c_str());
    return;
  }

  printf("write json\n");
  file << j;

  printf("close file\n");
  file.close();
  printf("done\n");
}

bool index_part::load_backing()
{
  backing = (uint8_t *) malloc(1024*1024*1024);

  std::ifstream file;

  printf("load %s\n", path.c_str());

  file.open(path.c_str(), std::ios::in | std::ios::binary);

  if (!file.is_open() || file.fail() || !file.good() || file.bad()) {
    fprintf(stderr, "error opening file %s\n",  path.c_str());
    return false;
  }

  file.read((char *) backing, 1024*1024*1024);

  file.close();

  return true;
}

void index_part::load()
{
  if (!load_backing()) {
    return;
  }

  uint32_t lists = ((uint32_t *)backing)[0];

  size_t offset = sizeof(uint32_t);

  for (size_t i = 0; i < lists; i++) {
    size_t n_postings = ((uint32_t *) (backing + offset))[0];

    offset += sizeof(uint32_t);

    auto pairs = new std::vector<std::pair<std::string, posting>>();
    pairs->reserve(n_postings);

    for (size_t j = 0; j < n_postings; j++) {
      uint8_t *c_key = backing + offset;
      std::string key((char *) c_key);

      offset += key.size() + 1;

      posting p;

      offset += p.load(backing + offset);

      pairs->emplace_back(key, p);
    }

    uint32_t index = hash((*pairs)[0].first);
    store[index] = pairs;
  }
}

posting *index_part::find(std::string key)
{
  printf("find %s\n", key.c_str());

  uint32_t index = hash(key);
  if (store[index] == NULL) {
    return NULL;
  }

  printf("have match, check list %s\n", key.c_str());
  for (auto &p: *store[index]) {
  printf("have match, check list %s == %s\n", key.c_str(), p.first.c_str());
    if (p.first == key) {
      return &p.second;
    }
  }

  return NULL;
}

static index_part load_part(index_type type, index_part_info &info)
{
  index_part part(type, info.path, info.start, info.end);

  part.load();

  return part;
}

void index::load()
{
  std::ifstream file;

  file.open(path, std::ios::in);

  if (!file.is_open()) {
    fprintf(stderr, "error opening file %s\n", path.c_str());
    return;
  }

  json j = json::parse(file);

  file.close();

  index_info info = j;

  page_lengths = info.page_lengths;

  average_page_length = 0;

  for (auto &p: page_lengths) {
    average_page_length += p.second;
  }

  average_page_length /= page_lengths.size();

  for (auto &p: info.word_parts) {
    word_parts.push_back(load_part(words, p));
  }

  for (auto &p: info.pair_parts) {
    pair_parts.push_back(load_part(pairs, p));
  }

  for (auto &p: info.trine_parts) {
    trine_parts.push_back(load_part(trines, p));
  }
}

struct terms {
  std::vector<std::string> words;
  std::vector<std::string> pairs;
  std::vector<std::string> trines;
};

static terms split_terms(char *line)
{
  const size_t buf_len = 512;

  char tok_buffer_store[buf_len];
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

  tok.init(line, strlen(line));

  terms t;

	do {
		token = tok.next(&tok_buffer);
    if (token == tokenizer::WORD) {
		  str_tolower(&tok_buffer);
		  str_tostem(&tok_buffer);

      std::string s(str_c(&tok_buffer));

      t.words.push_back(s);

      if (str_length(&tok_buffer_trine) > 0) {
        str_cat(&tok_buffer_trine, " ");
        str_cat(&tok_buffer_trine, str_c(&tok_buffer));

        std::string s(str_c(&tok_buffer_trine));

        t.trines.push_back(s);

        str_resize(&tok_buffer_trine, 0);
      }

      if (str_length(&tok_buffer_pair) > 0) {
        str_cat(&tok_buffer_pair, " ");
        str_cat(&tok_buffer_pair, str_c(&tok_buffer));

        std::string s(str_c(&tok_buffer_pair));

        t.pairs.push_back(s);

        str_cat(&tok_buffer_trine, str_c(&tok_buffer_pair));
      }

      str_resize(&tok_buffer_pair, 0);
      str_cat(&tok_buffer_pair, str_c(&tok_buffer));
    }
	} while (token != tokenizer::END);

  return t;
}

/* TODO: The rank uses the doc length to bias. Should it use seperate
 * doc lengths for words, pairs, and trines? */

/*
 * Atire BM25
 * Trotman, A., X. Jia, M. Crane, Towards an Efficient and Effective Search Engine,
 * SIGIR 2012 Workshop on Open Source Information Retrieval, p. 40-47
 */
static void rank(std::vector<std::pair<uint64_t, double>> &postings,
    std::map<uint64_t, size_t> &page_lengths, double avgdl)
{
	// IDF = ln(N/df_t)
	double wt = log(page_lengths.size() / postings.size());
	for (auto &p: postings) {
		uint64_t page_id = p.first;
		double tf = p.second;

    auto it = page_lengths.find(page_id);
    if (it == page_lengths.end()) {
      tf = 0;
      continue;
    }

		double docLength = it->second;

		//                   (k_1 + 1) * tf_td
		// IDF * ----------------------------------------- (over)
		//       k_1 * (1 - b + b * (L_d / L_avg)) + tf_td
		double k1 = 0.9;
		double b = 0.4;
		double dividend = (k1 + 1.0) * tf;
		double divisor = k1 * (1 - b + b * (docLength / avgdl) + tf);
		double rsv = wt * dividend / divisor;

		p.second = rsv;
	}
}

void index::find_part_matches(
    search::index_part &part,
    std::vector<std::string> &terms,
    std::vector<std::vector<std::pair<uint64_t, double>>> &postings)
{
	for (auto &term: terms) {
    printf("find term %s\n", term.c_str());
		posting *post = part.find(term);
    if (post != NULL) {
      std::vector<std::pair<uint64_t, double>> pairs;

      pairs.reserve(post->counts.size());
      for (auto &p: post->counts) {
        pairs.emplace_back(p.first, p.second);
      }

      rank(pairs, page_lengths, average_page_length);

      postings.push_back(pairs);
		}
	}
}

std::vector<std::vector<std::pair<uint64_t, double>>> index::find_matches(char *line)
{
  auto terms = split_terms(line);

  printf("got terms\n");

  std::vector<std::vector<std::pair<uint64_t, double>>> postings;

  printf("get postings\n");
  for (auto &p: word_parts) {
  printf("find matches from %s\n", p.path.c_str());
    find_part_matches(p, terms.words, postings);
  }

  for (auto &p: pair_parts) {
  printf("find matches from %s\n", p.path.c_str());
    find_part_matches(p, terms.pairs, postings);
  }

  for (auto &p: trine_parts) {
  printf("find matches from %s\n", p.path.c_str());
    find_part_matches(p, terms.trines, postings);
  }

  return postings;
}

}
