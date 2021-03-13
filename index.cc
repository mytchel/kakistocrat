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
#include <chrono>

#include "posting.h"
#include "bst.h"
#include "hash_table.h"
#include "index.h"
#include "tokenizer.h"

using namespace std::chrono_literals;

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

void index_part::load()
{
  if (!load_backing()) {
    return;
  }

  uint32_t count = ((uint32_t *)backing)[0];

  printf("loading %i postings\n", count);

  size_t offset = sizeof(uint32_t);

  for (size_t i = 0; i < count; i++) {
    size_t key_len = backing[offset];
    offset++;

    const char *c_key = (const char *) backing + offset;
    offset += key_len;

    posting p(backing + offset);

    offset += p.backing_size();

    store.emplace_back(std::piecewise_construct,
        std::forward_as_tuple(c_key, key_len),
        std::forward_as_tuple(p));

    update_index(std::prev(store.end()));
  }

  printf("loaded %i\n", store.size());
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

static size_t hash_to_buf(hash_table &t, uint8_t *buffer)
{
  printf("get postings\n");

  auto postings = t.get_postings();

  printf("got %i postings\n", postings.size());

  std::sort(postings.begin(), postings.end(),
      [](auto &a, auto &b) {
        return a.first < b.first;
      });

  printf("posings sorted, start saving\n");

  ((uint32_t *) buffer)[0] = postings.size();
  size_t offset = sizeof(uint32_t);

  for (auto &p: postings) {
    size_t len = p.first.size();
    if (len > 255) {
      printf("what the hell: %i : '%s'\n",
          len, p.first.c_str());
      len = 255;
    }

    buffer[offset] = len;

    offset++;

    memcpy(buffer + offset, p.first.data(), len);
    offset += len;

    offset += p.second.save(buffer + offset);
  }

  return offset;
}

size_t index_part::save_to_buf(
    uint8_t *buffer)
{
  ((uint32_t *) buffer)[0] = store.size();
  size_t offset = sizeof(uint32_t);

  for (auto &p: store) {
    buffer[offset] = p.first.size();
    offset++;

    memcpy(buffer + offset, p.first.data(), p.first.size());
    offset += p.first.size();

    offset += p.second.save(buffer + offset);
  }

  return offset;
}

void index_part::save()
{
  printf("write %s\n", path.c_str());

  uint8_t *buffer = (uint8_t *) malloc(1024 * 1024 * 1024);

  size_t len = save_to_buf(buffer);

  write_buf(path, buffer, len);

  free(buffer);
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

  auto words_path = base_path + ".index.words.dat";
  auto pairs_path = base_path + ".index.pairs.dat";
  auto trines_path = base_path + ".index.trines.dat";
  auto meta_path = base_path + ".index.meta.json";

  index_part_save(words, words_path);
  index_part_save(pairs, pairs_path);
  index_part_save(trines, trines_path);

  printf("indexer save meta data %s\n", meta_path.c_str());

  index_info info;

  info.page_lengths = page_lengths;

  info.word_parts.emplace_back(words_path);
  info.pair_parts.emplace_back(pairs_path);
  info.trine_parts.emplace_back(trines_path);

  printf("to json\n");
  json j = info;
  printf("open file\n");

  std::ofstream file;

  file.open(meta_path, std::ios::out | std::ios::trunc);

  if (!file.is_open()) {
    fprintf(stderr, "error opening file %s\n", meta_path.c_str());
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

void index_part::update_index(std::list<std::pair<key, posting>>::iterator ref)
{
  uint32_t hash_key = hash(ref->first.data(), ref->first.size());

  if (index[hash_key] == NULL) {
    index[hash_key] = new std::vector<
        std::list<std::pair<key, posting>>::iterator>();

    index[hash_key]->reserve(5);

  } else if (index[hash_key]->size() == index[hash_key]->capacity()) {
    index[hash_key]->reserve(index[hash_key]->size() * 2);
  }

  auto it = index[hash_key]->begin();
  while (it != index[hash_key]->end()) {
    if (ref->first > (*it)->first) {
      index[hash_key]->insert(it, ref);
      return;
    }

    it++;
  }

  index[hash_key]->push_back(ref);
}

std::list<std::pair<key, posting>>::iterator index_part::find(key k)
{
  uint32_t hash_key = hash(k.data(), k.size());
  if (index[hash_key] == NULL) {
    return store.end();
  }

  for (auto i: *index[hash_key]) {
    if (i->first == k) {
      return i;
    } else if (k > i->first) {
      return store.end();
    }
  }

  return store.end();
}

static index_part load_part(index_type type, index_part_info &info)
{
  index_part part(type, info.path, info.start, info.end);

  part.load();

  printf("loaded part %i\n", part.store.size());

  return part;
}

void index::load()
{
  std::ifstream file;

  auto meta_path = base_path + ".index.meta.json";

  printf("load index %s\n", meta_path.c_str());

  file.open(meta_path, std::ios::in);

  if (!file.is_open()) {
    fprintf(stderr, "error opening file %s\n", meta_path.c_str());
    return;
  }

  json j = json::parse(file);

  file.close();

  index_info info = j;

  page_lengths = info.page_lengths;

  average_page_length = 0;

  if (page_lengths.size() > 0) {
    for (auto &p: page_lengths) {
      average_page_length += p.second;
    }

    average_page_length /= page_lengths.size();
  }

  for (auto &p: info.word_parts) {
    word_parts.push_back(load_part(words, p));
    printf("loaded part %i\n", word_parts.back().store.size());
  }

  for (auto &p: info.pair_parts) {
    pair_parts.push_back(load_part(pairs, p));
  }

  for (auto &p: info.trine_parts) {
    trine_parts.push_back(load_part(trines, p));
  }
}

void index::save()
{
  index_info info;

  info.page_lengths = page_lengths;

  for (auto &p: word_parts) {
    p.save();
    info.word_parts.emplace_back(p.path, p.start, p.end);
  }

  for (auto &p: pair_parts) {
    p.save();
    info.pair_parts.emplace_back(p.path, p.start, p.end);
  }

  for (auto &p: trine_parts) {
    p.save();
    info.trine_parts.emplace_back(p.path, p.start, p.end);
  }

  auto meta_path = base_path + ".index.meta.json";

  printf("indexer save meta data %s\n", meta_path.c_str());

  printf("to json\n");
  json j = info;
  printf("open file\n");

  std::ofstream file;

  file.open(meta_path, std::ios::out | std::ios::trunc);

  if (!file.is_open()) {
    fprintf(stderr, "error opening file %s\n", meta_path.c_str());
    return;
  }

  printf("write json\n");
  file << j;

  printf("close file\n");
  file.close();
  printf("done\n");
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
static std::vector<std::pair<uint64_t, double>>
rank(
    std::list<std::pair<uint64_t, uint8_t>> &postings,
    std::map<uint64_t, size_t> &page_lengths, double avgdl)
{
  std::vector<std::pair<uint64_t, double>> pairs_ranked;

  pairs_ranked.reserve(postings.size());

	// IDF = ln(N/df_t)
	double wt = log(page_lengths.size() / postings.size());
	for (auto &p: postings) {
		uint64_t page_id = p.first;

    auto it = page_lengths.find(page_id);
    if (it == page_lengths.end()) {
      continue;
    }

		double docLength = it->second;

    double tf = p.second;

		//                   (k_1 + 1) * tf_td
		// IDF * ----------------------------------------- (over)
		//       k_1 * (1 - b + b * (L_d / L_avg)) + tf_td
		double k1 = 0.9;
		double b = 0.4;
		double dividend = (k1 + 1.0) * tf;
		double divisor = k1 * (1 - b + b * (docLength / avgdl) + tf);
		double rsv = wt * dividend / divisor;

    pairs_ranked.emplace_back(page_id, rsv);
	}

  return pairs_ranked;
}

void index::find_part_matches(
    search::index_part &part,
    std::vector<std::string> &terms,
    std::vector<std::vector<std::pair<uint64_t, double>>> &postings)
{
	for (auto &term: terms) {
    printf("find term %s\n", term.c_str());

    key k(term);

		auto pair = part.find(k);
    if (pair != part.store.end()) {
      auto &pairs = pair->second.to_pairs();
      auto pairs_ranked = rank(pairs, page_lengths, average_page_length);

      postings.push_back(pairs_ranked);
		}
	}
}

std::vector<std::vector<std::pair<uint64_t, double>>> index::find_matches(char *line)
{
  auto terms = split_terms(line);

  std::vector<std::vector<std::pair<uint64_t, double>>> postings;

  for (auto &p: word_parts) {
    find_part_matches(p, terms.words, postings);
  }

  for (auto &p: pair_parts) {
    find_part_matches(p, terms.pairs, postings);
  }

  for (auto &p: trine_parts) {
    find_part_matches(p, terms.trines, postings);
  }

  return postings;
}

void index_part::merge(index_part &other)
{
  printf("merge part '%s' into '%s'\n", other.path.c_str(), path.c_str());

  printf("need %i + %i\n", store.size(), other.store.size());

  auto s_it = store.begin();
  auto o_it = other.store.begin();

  size_t added = 0;

  std::chrono::nanoseconds index_total = 0ms;
  std::chrono::nanoseconds merge_total = 0ms;
  std::chrono::nanoseconds find_total = 0ms;
  std::chrono::nanoseconds skip_total = 0ms;

  while (s_it != store.end() && o_it != other.store.end()) {

    auto start = std::chrono::system_clock::now();
    auto it = find(o_it->first);
    auto end = std::chrono::system_clock::now();
    find_total += end - start;

    if (it != store.end()) {
      s_it = it;

    } else {

      auto start = std::chrono::system_clock::now();
      while (s_it != store.end() && o_it->first > s_it->first) {
        s_it++;
      }

      auto end = std::chrono::system_clock::now();
      skip_total += end - start;
    }

    if (s_it->first == o_it->first) {

      auto start = std::chrono::system_clock::now();

      s_it->second.merge(o_it->second);

      auto end = std::chrono::system_clock::now();
      merge_total += end - start;

      o_it++;

    } else if (s_it->first > o_it->first) {
      s_it = store.emplace(s_it, *o_it);

      auto start = std::chrono::system_clock::now();
      update_index(s_it);
      auto end = std::chrono::system_clock::now();
      index_total += end - start;


      o_it++;

      added++;
    }
  }

  while (o_it != other.store.end()) {
    store.emplace_back(o_it->first, o_it->second);

    auto start = std::chrono::system_clock::now();
    update_index(std::prev(store.end()));
    auto end = std::chrono::system_clock::now();
    index_total += end - start;


    s_it = store.end();
    o_it++;

    added++;
  }

  printf("finished, added %lu postings\n", added);

  printf("total index took %15lu\n", index_total.count());
  printf("total merge took %15lu\n", merge_total.count());
  printf("total skip  took %15lu\n", skip_total.count());
  printf("total find  took %15lu\n", find_total.count());
}

void index::merge(index &other)
{
  printf("merge\n");

  // Average the page lengths here?
  // or keep a sum for quick averaging

  if (word_parts.empty()) {
    word_parts.emplace_back(words, base_path + ".index.words.dat",
        "", "");
  }

  if (pair_parts.empty()) {
    pair_parts.emplace_back(pairs, base_path + ".index.pairs.dat",
        "", "");
  }

  if (trine_parts.empty()) {
    trine_parts.emplace_back(trines, base_path + ".index.trines.dat",
        "", "");
  }

  word_parts.front().merge(other.word_parts.front());
  pair_parts.front().merge(other.pair_parts.front());
  trine_parts.front().merge(other.trine_parts.front());
}

}
