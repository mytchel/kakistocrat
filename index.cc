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

#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <fcntl.h>

#include "spdlog/spdlog.h"

#include "util.h"
#include "posting.h"
#include "bst.h"
#include "hash_table.h"
#include "index.h"
#include "tokenizer.h"

using namespace std::chrono_literals;

using nlohmann::json;

namespace search {

std::vector<std::string> get_split_at() {
  std::vector<std::string> split_at;

  if (true) {
    split_at.emplace_back("a");
    split_at.emplace_back("b");
    split_at.emplace_back("c");
    split_at.emplace_back("d");
    split_at.emplace_back("e");
    split_at.emplace_back("f");
    split_at.emplace_back("g");
    split_at.emplace_back("h");
    split_at.emplace_back("i");
    split_at.emplace_back("j");
    split_at.emplace_back("k");
    split_at.emplace_back("l");
    split_at.emplace_back("m");
    split_at.emplace_back("n");
    split_at.emplace_back("o");
    split_at.emplace_back("p");
    split_at.emplace_back("q");
    split_at.emplace_back("r");
    split_at.emplace_back("s");
    split_at.emplace_back("t");
    split_at.emplace_back("u");
    split_at.emplace_back("v");
    split_at.emplace_back("w");
    split_at.emplace_back("x");
    split_at.emplace_back("y");
    split_at.emplace_back("z");
  } else if (true) {
    split_at.emplace_back("f");
    split_at.emplace_back("m");
    split_at.emplace_back("s");
  }

  return split_at;
}

std::list<std::string> load_parts(std::string path)
{
  std::list<std::string> parts;

  std::ifstream file;

  file.open(path, std::ios::in);

  if (!file.is_open()) {
    spdlog::warn("error opening file {}", path);
    return parts;
  }

  json j = json::parse(file);

  file.close();

  j.at("parts").get_to(parts);

  return parts;
}

void save_parts(std::string path, std::list<std::string> parts)
{
  json j = json{{"parts", parts}};

  std::ofstream file;

  file.open(path, std::ios::out | std::ios::trunc);

  if (!file.is_open()) {
    spdlog::warn("error opening file {}", path);
    return;
  }

  file << j;

  file.close();
}

void to_json(nlohmann::json &j, const index_part_info &i)
{
  std::string start = "", end = "";
  if (i.start) start = *i.start;
  if (i.end) end = *i.end;
  j = json{
    {"path", i.path},
    {"start", start},
    {"end", end}};
}

void from_json(const nlohmann::json &j, index_part_info &i)
{
  std::string start = "", end = "";

  j.at("path").get_to(i.path);
  j.at("start").get_to(start);
  j.at("end").get_to(end);

  if (start != "") {
    i.start = start;
  } else {
    i.start = {};
  }

  if (end != "") {
    i.end = end;
  } else {
    i.end = {};
  }
}

void index_info::save()
{
  json j = json{
      {"average_page_length", average_page_length},
      {"page_lengths", page_lengths},
      {"word_parts", word_parts},
      {"pair_parts", pair_parts},
      {"trine_parts", trine_parts}};

  std::ofstream file;

  file.open(path, std::ios::out | std::ios::trunc);

  if (!file.is_open()) {
    spdlog::warn("error opening file {}", path);
    return;
  }

  file << j;

  file.close();
}

void index_info::load()
{
  std::ifstream file;

  file.open(path, std::ios::in);

  if (!file.is_open()) {
    spdlog::warn("error opening file {}", path);
    return;
  }

  json j = json::parse(file);

  file.close();

  j.at("average_page_length").get_to(average_page_length);
  j.at("page_lengths").get_to(page_lengths);
  j.at("word_parts").get_to(word_parts);
  j.at("pair_parts").get_to(pair_parts);
  j.at("trine_parts").get_to(trine_parts);
}

void index_part::load()
{
  if (!load_backing()) {
    return;
  }

  spdlog::info("loading");
  uint32_t page_count = ((uint32_t *)backing)[0];
  uint32_t posting_count = ((uint32_t *)backing)[1];
  spdlog::info("loading {}, {}", page_count, posting_count);

  size_t offset = sizeof(uint32_t) * 2;

  page_ids.reserve(page_count);

  for (size_t i = 0; i < page_count; i++) {
    page_ids.push_back(((uint64_t *) (backing + offset))[i]);
  }
  offset += sizeof(uint64_t) * page_count;

  spdlog::info("got pages {}", page_ids.size());

  for (size_t i = 0; i < posting_count; i++) {
    key k(backing + offset);
    offset += k.size();

    posting p(backing + offset);

    offset += p.size();

    store.emplace_back(k, std::move(p));

    update_index(std::prev(store.end()));
  }

  spdlog::info("got postings {}", store.size());
}

void write_buf(std::string path, uint8_t *buf, size_t len)
{
  std::ofstream file;

  file.open(path, std::ios::out | std::ios::binary | std::ios::trunc);

  if (!file.is_open()) {
    spdlog::info("error opening file {}", path);
    return;
  }

  spdlog::info("writing {:4} kb to {}", len / 1024, path);
  file.write((const char *) buf, len);

  file.close();
}

static size_t hash_to_buf(
    std::vector<std::pair<uint64_t, uint32_t>> &pages,
    std::list<std::pair<key, posting>>::iterator &start,
    std::list<std::pair<key, posting>>::iterator &end,
    size_t count,
    uint8_t *buffer, size_t buffer_len)
{
  size_t offset = sizeof(uint32_t) * 2;

  for (auto &p: pages) {
    if (offset + sizeof(uint64_t) >= buffer_len) {
      spdlog::warn("buffer too small");
      return 0;
    }

    *((uint64_t *) (buffer + offset)) = p.first;
    offset += sizeof(uint64_t);
  }

  for (auto p = start; p != end; p++) {
    if (offset + p->first.size() + p->second.size() >= buffer_len) {
      spdlog::warn("buffer too small");
      return 0;
    }

    memcpy(buffer + offset, p->first.data(), p->first.size());
    offset += p->first.size();

    offset += p->second.save(buffer + offset);
  }

  ((uint32_t *) buffer)[0] = pages.size();
  ((uint32_t *) buffer)[1] = count;

  return offset;
}

size_t index_part::save_to_buf(uint8_t *buffer, size_t buffer_len)
{
  /*
  store.sort(
      [](auto &a, auto &b) {
        return a.first < b.first;
      });
*/

  size_t offset = sizeof(uint32_t) * 2;

  for (auto p: page_ids) {
    if (offset + sizeof(uint64_t) >= buffer_len) {
      spdlog::warn("buffer too small");
      return 0;
    }

    *((uint64_t *) (buffer + offset)) = p;
    offset += sizeof(uint64_t);
  }

  size_t total_dict = offset;
  size_t total_key = 0;
  size_t total_ids = 0;

  uint32_t count = 0;

  for (auto &p: store) {
    if (p.second.only_one()) continue;
    
    if (offset + p.first.size() + p.second.size() >= buffer_len) {
      spdlog::warn("buffer too small");
      return 0;
    }

    memcpy(buffer + offset, p.first.data(), p.first.size());
    offset += p.first.size();
    total_key += p.first.size();

    auto l = p.second.save(buffer + offset);
    offset += l;
    total_ids += l;
    
    count++;
  }

  ((uint32_t *) buffer)[0] = page_ids.size();
  ((uint32_t *) buffer)[1] = count;

  if (offset > 0) {
    spdlog::info("saving with {} dict {} key {} ids",
        (float) total_dict / offset,
        (float) total_key / offset,
        (float) total_ids / offset);
  }

  return offset;
}

void index_part::save()
{
  uint8_t *buffer = (uint8_t *) malloc(max_index_part_size);
  if (buffer == NULL) {
    throw std::bad_alloc();
  }

  size_t len = save_to_buf(buffer, max_index_part_size);

  write_buf(path, buffer, len);

  free(buffer);
}

std::vector<index_part_info> index_part_save(
    std::vector<std::pair<uint64_t, uint32_t>> &pages,
    hash_table &t, std::string base_path)
{
  std::vector<index_part_info> parts;

  auto postings = t.get_postings();

  if (postings.empty()) {
    return parts;
  }

  postings.sort(
      [](auto &a, auto &b) {
        return a.first < b.first;
      });

  uint8_t *buffer = (uint8_t *) malloc(max_index_part_size);
  if (buffer == NULL) {
    spdlog::warn("part buffer malloc failed");
    throw std::bad_alloc();
  }

  auto split_at = get_split_at();

  auto split = split_at.begin();
  auto start = postings.begin();
  auto end = postings.begin();

  size_t count = 0;

  while (true) {
    if (end == postings.end() || (split != split_at.end() && end->first >= *split)) {
      if (start != end) {
        size_t len = hash_to_buf(pages, start, end, count, buffer, max_index_part_size);

        auto path = fmt::format("{}.{}.dat", base_path, start->first.str());

        write_buf(path, buffer, len);

        parts.emplace_back(path, start->first.str(), std::prev(end)->first.str());
      }

      count = 0;
      start = end;
      split++;

      if (end == postings.end()) {
        break;
      }
    }

    count++;
    end++;
  }

  free(buffer);

  return parts;
}

std::string indexer::save()
{
  auto words_path = fmt::format("{}.{}.words", base_path, flush_count);
  auto pairs_path = fmt::format("{}.{}.pairs", base_path, flush_count);
  auto trines_path = fmt::format("{}.{}.trines", base_path, flush_count);
  auto meta_path = fmt::format("{}.{}.meta.json", base_path, flush_count);

  auto word_parts = index_part_save(pages, word_t, words_path);
  auto pair_parts = index_part_save(pages, pair_t, pairs_path);
  auto trine_parts = index_part_save(pages, trine_t, trines_path);

  index_info info(meta_path);

  size_t average_page_length = 0;

  if (pages.size() > 0) {
    for (auto &p: pages) {
      average_page_length += p.second;
      info.page_lengths.emplace(p.first, p.second);
    }

    average_page_length /= pages.size();
  }

  info.average_page_length = average_page_length;

  info.word_parts = word_parts;
  info.pair_parts = pair_parts;
  info.trine_parts = trine_parts;

  info.save();

  flush_count++;

  return meta_path;
}

bool index_part::load_backing()
{
  struct stat s;
  if (stat(path.c_str(), &s) == -1) {
    spdlog::warn("load backing failed {}, no file", path);
    return false;
  }

  size_t part_size = s.st_size;

  backing = (uint8_t *) malloc(part_size);
  if (backing == NULL) {
    spdlog::warn("load backing failed {}, malloc failed for {}", path, part_size);
    throw std::bad_alloc();
  }

  std::ifstream file;

  spdlog::info("load {:4} mb from {}", part_size / 1024 / 1024, path);

  file.open(path, std::ios::in | std::ios::binary);

  if (!file.is_open() || file.fail() || !file.good() || file.bad()) {
    spdlog::warn("error opening file {}",  path);
    return false;
  }

  file.read((char *) backing, part_size);

  file.close();

  return true;
}

void index_part::update_index(std::list<std::pair<key, posting>>::iterator ref)
{
  uint32_t hash_key = hash(ref->first.c_str(), ref->first.len());
  size_t key_len = ref->first.len();

  if (index[hash_key].size() + 1 >= index[hash_key].capacity()) {
    index[hash_key].reserve((index[hash_key].size() + 1) * 2);
  }

  auto it = index[hash_key].begin();
  auto end = index[hash_key].end();
  while (it != end) {
    if (it->first >= key_len) {
      break;
    }
    it++;
  }

  index[hash_key].emplace(it, key_len, ref);
}

std::list<std::pair<key, posting>>::iterator index_part::find(key k)
{
  uint32_t hash_key = hash(k.c_str(), k.len());

  for (auto &i: index[hash_key]) {
    if (i.first == k.len()) {
      if (i.second->first == k) {
        return i.second;
      }
    } else if (i.first > k.len()) {
      break;
    }
  }

  return store.end();
}

std::list<std::pair<key, posting>>::iterator index_part::find(std::string s)
{
  uint32_t hash_key = hash(s);

  for (auto &i: index[hash_key]) {
    if (i.first == s.size()) {
      if (i.second->first == s) {
        return i.second;
      }
    } else if (i.first > s.size()) {
      break;
    }
  }

  return store.end();
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

  index_info info(path);

  info.load();

  page_lengths = info.page_lengths;

  average_page_length = info.average_page_length;

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

void index::save()
{
  index_info info(path);

  size_t average_page_length = 0;

  if (page_lengths.size() > 0) {
    for (auto &p: page_lengths) {
      average_page_length += p.second;
    }

    average_page_length /= page_lengths.size();
  }

  info.average_page_length = average_page_length;
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

  info.save();
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
    std::vector<std::pair<uint32_t, uint8_t>> &postings,
    std::vector<uint64_t> &page_ids,
    std::map<uint64_t, uint32_t> &page_lengths,
    double avgdl)
{
  std::vector<std::pair<uint64_t, double>> pairs_ranked;

  pairs_ranked.reserve(postings.size());

	// IDF = ln(N/df_t)
	double wt = log(page_lengths.size() / postings.size());
	for (auto &p: postings) {
		uint32_t index_id = p.first;
    //spdlog::info("posting has id {} check from {}", index_id, page_ids.size());
		uint64_t page_id = page_ids.at(index_id);

    //spdlog::info("posting has page id {}", page_id);
    auto it = page_lengths.find(page_id);
    if (it == page_lengths.end()) {
      spdlog::info("didnt find page length for {}", page_id);
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

  std::sort(pairs_ranked.begin(), pairs_ranked.end(),
    [](auto &a, auto &b) {
        return a.first < b.first;
      });

  return pairs_ranked;
}

void index::find_part_matches(
    search::index_part &part,
    std::vector<std::string> &terms,
    std::vector<std::vector<std::pair<uint64_t, double>>> &postings)
{
  spdlog::info("check part {} {}", part.path, part.page_ids.size());

	for (auto &term: terms) {
    if ((part.start && term < *part.start || (part.end && term > *part.end)))
      continue;

    spdlog::info("find term {} in {}", term, part.path);

		auto pair = part.find(term);
    if (pair != part.store.end()) {
      auto pairs = pair->second.decompress();
      spdlog::info("have pair {} with {} docs", pair->first.str(), pairs.size());
      auto pairs_ranked = rank(pairs, part.page_ids, page_lengths, average_page_length);

      spdlog::info("have ranked {} with {} docs", pair->first.str(), pairs_ranked.size());
      postings.push_back(pairs_ranked);
		}
	}
}

std::vector<std::vector<std::pair<uint64_t, double>>> index::find_matches(char *line)
{
  spdlog::info("find matches for {}", line);

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
  auto o_it = other.store.begin();

  size_t added = 0;

  size_t key_buf_size = 1024 * 1024;

  uint32_t page_id_offset = page_ids.size();

  page_ids.reserve(page_ids.size() + other.page_ids.size());
  for (auto p: other.page_ids) {
    page_ids.push_back(p);
  }

  while (o_it != other.store.end()) {
    if (start && o_it->first < *start) {
      o_it++;
      continue;
    }
    if (end && o_it->first >= *end) break;

    auto start = std::chrono::system_clock::now();
    auto it = find(o_it->first);
    auto end = std::chrono::system_clock::now();
    find_total += end - start;

    if (it != store.end()) {
      auto start = std::chrono::system_clock::now();

      store.back().second.merge(o_it->second, page_id_offset);

      auto end = std::chrono::system_clock::now();
      merge_total += end - start;

    } else {
      auto start = std::chrono::system_clock::now();

      size_t c_len = o_it->first.size();

      uint8_t *c_buf = extra_backing.get(c_len);
      if (c_buf == NULL) {
        spdlog::warn("out of mem");
        break;
      }

      memcpy(c_buf, o_it->first.data(), c_len);

      store.emplace_back(key(c_buf), posting());

      store.back().second.merge(o_it->second, page_id_offset);

      update_index(std::prev(store.end()));
      auto end = std::chrono::system_clock::now();
      index_total += end - start;

      added++;
    }

    o_it++;
  }
}

}
