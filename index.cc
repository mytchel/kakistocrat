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
#include "hash.h"
#include "index.h"
#include "tokenizer.h"

using namespace std::chrono_literals;

using nlohmann::json;

namespace search {

std::vector<std::string> alphabet() {
  std::vector<std::string> a;

  a.push_back(".");

  for (size_t i = 0; i < 10; i++) {
    a.push_back(std::string(1, '0' + i));
  }

  for (size_t i = 0; i < 26; i++) {
    a.push_back(std::string(1, 'a' + i));
  }

  return a;
}

std::vector<std::string> get_split_at(size_t parts) {
  std::vector<std::string> total_split_at;
  auto a = alphabet();

  while (parts > 1) {
    std::vector<std::string> split_at;

    if (parts > a.size()) {
      split_at = a;

      parts /= a.size();

    } else {
      size_t step = a.size() / parts;
      for (size_t i = 0; i < a.size(); i += step) {
        split_at.push_back(a[i]);
      }

      parts = 1;
    }

    if (total_split_at.empty()) {
      total_split_at = split_at;

    } else if (split_at.size() > 1) {

      std::vector<std::string> new_split_at;

      for (auto &s: total_split_at) {
        new_split_at.push_back(s);

        auto ss = split_at.begin();
        ss++;
        while (ss != split_at.end()) {
          new_split_at.push_back(s + *ss++);
        }
      }

      total_split_at = new_split_at;
    }
  }

  size_t i = 0;
  for (auto &s: total_split_at) {
    spdlog::info("split at {} : {}", i++, s);
  }

  return total_split_at;
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
  std::string end = "";
  if (i.end) end = *i.end;
  j = json{
    {"path", i.path},
    {"start", i.start},
    {"end", end}};
}

void from_json(const nlohmann::json &j, index_part_info &i)
{
  std::string end;

  j.at("path").get_to(i.path);
  j.at("start").get_to(i.start);
  j.at("end").get_to(end);

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

void save_part(
    std::string path,
    std::list<std::pair<uint64_t, uint32_t>> &pages,
    std::list<std::pair<key, posting>> &store,
    uint8_t *buffer, size_t buffer_len)
{
  size_t page_count = 0;
  size_t post_count = 0;
  size_t offset = sizeof(uint32_t) * 2;

  auto rpage = save_pages_to_buf(pages,
      buffer + offset, max_index_part_size - offset);

  offset += rpage.first;
  page_count = rpage.second;

  auto rpost = save_postings_to_buf(store.begin(), store.end(),
      buffer + offset, max_index_part_size - offset);

  offset += rpost.first;
  post_count = rpost.second;

  ((uint32_t *) buffer)[0] = page_count;
  ((uint32_t *) buffer)[1] = post_count;

  write_buf(path, buffer, offset);
}

std::vector<index_part_info> index_part_save(
    std::list<std::pair<uint64_t, uint32_t>> &pages,
    index_part &t, std::string base_path)
{
  std::vector<index_part_info> parts;

  spdlog::info("save part {}", base_path);

  uint8_t *buffer = (uint8_t *) malloc(max_index_part_size);
  if (buffer == NULL) {
    spdlog::warn("part buffer malloc failed");
    throw std::bad_alloc();
  }

  auto start = t.store_split.begin();
  for (auto &store: t.stores) {
    spdlog::info("save part {} {}", base_path, *start);

    if (!store.empty()) {

      auto path = fmt::format("{}.{}.dat", base_path, *start);

      save_part(path, pages, store,
          buffer, max_index_part_size);

      std::optional<std::string> end;
      if (start + 1 != t.store_split.end()) {
        end = *(start + 1);
      }

      parts.emplace_back(path, *start, end);

    } else {
      spdlog::warn("save part {} {} part is empy", base_path, *start);
    }

    start++;
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

  return meta_path;
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
  std::list<std::string> words;
  std::list<std::string> pairs;
  std::list<std::string> trines;
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

	// IDF = ln(N/df_t)
	double wt = log(page_lengths.size() / postings.size());
	for (auto &p: postings) {
		uint32_t index_id = p.first;
    //spdlog::info("posting has id {} check from {}", index_id, page_ids.size());
		uint64_t page_id = page_ids.at(index_id);

    spdlog::info("index id {} -. page id {}", index_id, page_id);
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
    std::list<std::string> &terms,
    std::vector<std::vector<std::pair<uint64_t, double>>> &postings)
{
	for (auto &term: terms) {
    if ((term < part.start || (part.end && term > *part.end)))
      continue;

    spdlog::info("find term {} in {}", term, part.path);

		auto pair = part.find(term);
    if (pair != part.stores[0].end()) {
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

}
