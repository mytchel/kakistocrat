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

static bool word_allow_extra(std::string s)
{
  if (s.size() < 4) return false;
  if (s.size() > 30) return false;

  for (auto c: s) {
    if ('a' <= c && c <= 'z') {
      continue;
    } else {
      return false;
    }
  }

  return true;
}

void indexer::index_site(crawl::site &site, char *file_buf, size_t file_buf_len) {
  spdlog::info("index site {}", site.host);

  const size_t buf_len = 80;

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

  spdlog::info("process pages for {}", site.host);
  for (auto &page: site.pages) {
    if (page.last_scanned == 0) continue;

    uint64_t page_id = crawl::page_id(site.id, page.id).to_value();
    uint32_t index_id = next_id();
    size_t page_length = 0;

    std::ifstream pfile;

    pfile.open(page.path, std::ios::in | std::ios::binary);

    if (!pfile.is_open() || pfile.fail() || !pfile.good() || pfile.bad()) {
      spdlog::warn("error opening file {}", page.path);
      continue;
    }

    pfile.read(file_buf, file_buf_len);

    size_t len = pfile.gcount();

    spdlog::debug("process page {} / {} : {} kb : {}",
      page_id, index_id,
      len / 1024, page.url);

    check_usage();

    tokenizer::tokenizer tok(file_buf, len);

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

        if (s.size() > 2) {

          page_length++;

          insert(search::words, s, index_id);

          if (word_allow_extra(s)) {
            if (str_length(&tok_buffer_trine) > 0) {
              str_cat(&tok_buffer_trine, " ");
              str_cat(&tok_buffer_trine, str_c(&tok_buffer));

              std::string s(str_c(&tok_buffer_trine));

              insert(search::trines, s, index_id);

              str_resize(&tok_buffer_trine, 0);
            }

            if (str_length(&tok_buffer_pair) > 0) {
              str_cat(&tok_buffer_pair, " ");
              str_cat(&tok_buffer_pair, str_c(&tok_buffer));

              std::string s(str_c(&tok_buffer_pair));

              insert(search::pairs, s, index_id);

              str_cat(&tok_buffer_trine, str_c(&tok_buffer_pair));
            }

            str_resize(&tok_buffer_pair, 0);
            str_cat(&tok_buffer_pair, str_c(&tok_buffer));

          } else {
            str_resize(&tok_buffer_pair, 0);
            str_resize(&tok_buffer_trine, 0);
          }
        }
      }
    } while (token != tokenizer::END);

    pfile.close();

    add_page(page_id, page_length);
  }

  spdlog::info("finished indexing site {}", site.host);
}

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

      parts = (parts / a.size()) + 1;

    } else {
      size_t step = a.size() / parts;
      for (size_t i = 0; i + step < a.size(); i += step) {
        split_at.push_back(a[i]);
      }

      parts = 0;
    }

    if (total_split_at.empty()) {
      total_split_at = split_at;

    } else if (split_at.size() > 1) {
      std::vector<std::string> new_split_at;

      for (auto &s: total_split_at) {
        new_split_at.push_back(s);

        auto ss = std::next(split_at.begin());
        while (ss != split_at.end()) {
          new_split_at.push_back(s + *ss++);
        }
      }

      total_split_at = new_split_at;
    }
  }

  if (total_split_at.empty()) {
    total_split_at.push_back(a[0]);
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

void index_part_save(
    std::string path,
    std::list<std::pair<uint64_t, uint32_t>> &pages,
    forward_list<std::pair<key, posting>, fixed_memory_pool> &store,
    uint8_t *buffer, size_t buffer_len)
{
  size_t page_count = 0;
  size_t post_count = 0;
  size_t offset = sizeof(uint32_t) * 2;

  auto rpage = save_pages_to_buf(pages,
      buffer + offset, buffer_len - offset);

  offset += rpage.first;
  page_count = rpage.second;

  auto rpost = save_postings_to_buf(store.begin(), store.end(),
      buffer + offset, buffer_len - offset);

  offset += rpost.first;
  post_count = rpost.second;

  ((uint32_t *) buffer)[0] = page_count;
  ((uint32_t *) buffer)[1] = post_count;

  write_buf(path, buffer, offset);
}

std::vector<index_part_info> indexer::save_part(
    index_part &t, std::string base_path)
{
  std::vector<index_part_info> parts;

  spdlog::info("save part {}", base_path);

  auto start = t.store_split.begin();
  for (auto &store: t.stores) {
    auto path = fmt::format("{}.{}.dat", base_path, *start);

    if (!store.empty()) {

      index_part_save(path, pages, store, file_buf, file_buf_size);

      std::optional<std::string> end;
      if (start + 1 != t.store_split.end()) {
        end = *(start + 1);
      }

      parts.emplace_back(path, *start, end);

    } else {
      spdlog::warn("save part {} part is empy", path);
    }

    start++;
  }

  return parts;
}

std::string indexer::save()
{
  auto words_path = fmt::format("{}.{}.words", base_path, flush_count);
  auto pairs_path = fmt::format("{}.{}.pairs", base_path, flush_count);
  auto trines_path = fmt::format("{}.{}.trines", base_path, flush_count);
  auto meta_path = fmt::format("{}.{}.meta.json", base_path, flush_count);

  auto word_parts = save_part(word_t, words_path);
  auto pair_parts = save_part(pair_t, pairs_path);
  auto trine_parts = save_part(trine_t, trines_path);

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
  tokenizer::tokenizer tok(line, strlen(line));

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
		uint64_t page_id = 0;

    try {
      page_id = page_ids.at(index_id);
    } catch (const std::exception& e) {
      spdlog::info("bad index id {} / {}", index_id, page_ids.size());
      continue;
    }

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
    std::string &term,
    std::vector<std::vector<std::pair<uint64_t, double>>> &postings)
{
  spdlog::info("find term {} in {}", term, part.path);

  auto pair = part.find(term);
  if (pair != part.stores[0].end()) {
    auto pairs = pair->second.decompress();
    spdlog::info("have pair {} with {} docs", pair->first.str(), pairs.size());
    auto pairs_ranked = rank(pairs, part.page_ids, info.page_lengths, info.average_page_length);

    spdlog::info("have ranked {} with {} docs", pair->first.str(), pairs_ranked.size());
    postings.push_back(pairs_ranked);
	}
}

static index_part load_part(index_part_info &info)
{
  index_part part(info.path, info.start, info.end);

  part.load();

  return part;
}

void index::find_matches(
    std::vector<index_part_info> &part_info,
    std::list<std::string> &terms,
    std::vector<std::vector<std::pair<uint64_t, double>>> &postings)
{
  terms.sort();

  auto term = terms.begin();

  // This does not support index parts overlapping.
  // Index parts must be ordered.

  for (auto &part: part_info) {
    if (term == terms.end()) {
      break;
    }

    if (*term < part.start) {
      term++;
    }

    if (part.end && *term > *part.end) {
      continue;
    }

    auto index = load_part(part);

    while (term != terms.end()) {
      if (part.start <= *term && (part.end && *term < *part.end)) {
        find_part_matches(index, *term, postings);
      } else {
        break;
      }

      term++;
    }
  }
}

std::vector<std::vector<std::pair<uint64_t, double>>> index::find_matches(char *line)
{
  spdlog::info("find matches for {}", line);

  auto terms = split_terms(line);

  std::vector<std::vector<std::pair<uint64_t, double>>> postings;

  find_matches(info.word_parts, terms.words, postings);
  find_matches(info.pair_parts, terms.pairs, postings);
  find_matches(info.trine_parts, terms.trines, postings);

  return postings;
}

}
