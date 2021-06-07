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

#include "index.h"
#include "tokenizer.h"

using namespace std::chrono_literals;

using nlohmann::json;

namespace search {

index_type from_str(const std::string &s) {
  if (s == "words") {
    return words;
  } else if (s == "pairs") {
    return pairs;
  } else if (s == "trines") {
    return trines;
  } else {
    throw std::runtime_error(fmt::format("bad type: {}", s));
  }
}

std::string to_str(index_type type) {
  if (type == words) {
    return "words";
  } else if (type == pairs) {
    return "pairs";
  } else if (type == trines) {
    return "trines";
  } else {
    throw std::runtime_error(fmt::format("bad type"));
  }
}

static bool word_allow_extra(std::string s)
{
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

void indexer::index_site(site_map &site, char *file_buf, size_t file_buf_len) {
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

  spdlog::info("process {} pages for {}", site.pages.size(), site.host);
  for (auto &page: site.pages) {
    if (page.last_scanned == 0) {
      spdlog::info("skip unscanned page {}", page.url);
      continue;
    }

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

    spdlog::debug("process page {} : {} kb : {}",
      index_id, len / 1024, page.url);

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
    } while (token != tokenizer::END);

    pfile.close();

    add_page(page.url, page_length);
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
    std::list<std::pair<std::string, uint32_t>> &pages,
    forward_list<std::pair<key, posting>, fixed_memory_pool> &store,
    uint8_t *buffer, size_t buffer_len)
{
  size_t page_count = 0;
  size_t post_count = 0;
  size_t offset = sizeof(uint32_t) * 2;

  std::vector<std::string> pages_raw;
  pages_raw.reserve(pages.size());
  for (auto &p: pages) {
    pages_raw.emplace_back(p.first);
  }

  auto rpage = save_pages_to_buf(pages_raw,
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
  auto words_path = fmt::format("{}/part.{}.words", base_path, flush_count);
  auto pairs_path = fmt::format("{}/part.{}.pairs", base_path, flush_count);
  auto trines_path = fmt::format("{}/part.{}.trines", base_path, flush_count);
  auto meta_path = fmt::format("{}/part.{}.meta.json", base_path, flush_count);

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
static std::vector<std::pair<std::string, double>>
rank(
    std::vector<std::pair<uint32_t, uint8_t>> &postings,
    std::vector<std::string> &pages,
    std::map<std::string, uint32_t> &page_lengths,
    double avgdl)
{
  std::vector<std::pair<std::string, double>> pairs_ranked;

	// IDF = ln(N/df_t)
	double wt = log(page_lengths.size() / postings.size());
	for (auto &p: postings) {
		uint32_t index_id = p.first;
    std::string page_url;

    try {
      page_url = pages.at(index_id);
    } catch (const std::exception& e) {
      spdlog::info("bad index id {} / {}", index_id, pages.size());
      continue;
    }

    auto it = page_lengths.find(page_url);
    if (it == page_lengths.end()) {
      spdlog::info("didnt find page length for {}", page_url);
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

    pairs_ranked.emplace_back(page_url, rsv);
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
    std::vector<std::vector<std::pair<std::string, double>>> &postings)
{
  spdlog::debug("find term {} in {}", term, part.path);

  auto pair = part.find(term);
  if (pair != nullptr) {
    auto pairs = pair->second.decompress();
    spdlog::debug("have pair {} with {} docs", pair->first.str(), pairs.size());
    auto pairs_ranked = rank(pairs, part.pages, info.page_lengths, info.average_page_length);

    spdlog::debug("have ranked {} with {} docs", pair->first.str(), pairs_ranked.size());
    postings.push_back(pairs_ranked);
	} else {
    spdlog::debug("nothing found");
  }
}

static index_part load_part(index_part_info &info, size_t htcap)
{
  index_part part(info.path, htcap, info.start, info.end);

  part.load();

  return part;
}

void index::find_matches(
    std::vector<index_part_info> &part_info,
    std::list<std::string> &terms,
    std::vector<std::vector<std::pair<std::string, double>>> &postings)
{
  terms.sort();

  auto term = terms.begin();

  // This does not support index parts overlapping.
  // Index parts must be ordered.

  for (auto &part: part_info) {
    if (term == terms.end()) {
      spdlog::info("checked all terms");
      break;
    }

    if (*term < part.start) {
      spdlog::info("term {} all checked", *term);
      term++;
    }

    if (part.end && *term > *part.end) {
      spdlog::info("term {} > part end {}", *term, *part.end);
      continue;
    }

    spdlog::info("load part {} to check terms", part.path);

    auto index = load_part(part, htcap);

    while (term != terms.end()) {
      spdlog::info("is term {} in {}", *term, part.start);

      if (part.start <= *term && (!part.end || *term < *part.end)) {
        spdlog::info("search {} for {}", part.path, *term);
        find_part_matches(index, *term, postings);
      } else {
        spdlog::info("stop?");
        break;
      }

      term++;
    }
  }
}

std::vector<std::vector<std::pair<std::string, double>>> index::find_matches(char *line)
{
  spdlog::info("find matches for {}", line);

  auto terms = split_terms(line);

  for (auto &t: terms.words)
    spdlog::info("word '{}'", t);

  for (auto &t: terms.pairs)
    spdlog::info("pair '{}'", t);

  for (auto &t: terms.trines)
    spdlog::info("trine '{}'", t);

  std::vector<std::vector<std::pair<std::string, double>>> postings;

  find_matches(info.word_parts, terms.words, postings);
  find_matches(info.pair_parts, terms.pairs, postings);
  find_matches(info.trine_parts, terms.trines, postings);

  return postings;
}

// This only returns documents that match all the terms.
// Which may not be the best?
// It is certainly not what I want.
std::list<std::pair<std::string, double>>
intersect_postings_strict(std::vector<std::vector<std::pair<std::string, double>>> &postings)
{
  std::list<std::pair<std::string, double>> result;

  spdlog::info("interset {}", postings.size());

  if (postings.size() == 0) {
    spdlog::info("have nothing");
    return result;
  }

  std::vector<size_t> indexes(postings.size(), 0);

  double sum_scores = 0;

  bool done = false;

	while (indexes[0] < postings[0].size()) {
		auto url = postings[0][indexes[0]].first;
		bool canAdd = true;
		for (size_t i = 1; i < postings.size(); i++) {
			while (indexes[i] < postings[i].size() && postings[i][indexes[i]].first < url) {
				indexes[i]++;
      }

			if (indexes[i] == postings[i].size()) {
        done = true;
        break;
      }

			if (postings[i][indexes[i]].first != url)
				canAdd = false;
		}

    if (done) {
      break;
    }

    if (canAdd) {
	    double rsv = 0;
			for (size_t i = 0; i < postings.size(); i++) {
				double w = postings[i][indexes[i]].second;
        rsv += w;
      }

      sum_scores += rsv;
      result.emplace_back(url, rsv);
		}

		indexes[0]++;
	}

  spdlog::info("total sum {} for {} postings", sum_scores, result.size());

  if (sum_scores > 0) {
    for (auto &p: result) {
      spdlog::info("{} : {}", p.second, p.first);
      p.second /= sum_scores;
    }
  }

	return result;
}

std::list<std::pair<std::string, double>>
intersect_postings(std::vector<std::vector<std::pair<std::string, double>>> &postings)
{
  spdlog::info("interset {}", postings.size());

  if (postings.size() == 0) {
    spdlog::info("have nothing");
    return {};
  }

  std::vector<size_t> indexes(postings.size(), 0);

  for (size_t i = 0; i < postings.size(); i++) {
    spdlog::debug("posting {}", i);
    for (size_t j = 0; j < postings[i].size(); j++) {
      spdlog::debug("    {}", postings[i][j].first);
    }
  }

  size_t url_max_len = 0;
  std::list<std::pair<std::string, double>> result;

  while (true) { 
		std::string url = "";

		for (size_t i = 0; i < postings.size(); i++) {
      if (indexes[i] < postings[i].size()) {
        if (url == "" || postings[i][indexes[i]].first < url) {
          url = postings[i][indexes[i]].first;
        }
      }
    }

    if (url == "") {
      break;
    }

    size_t p_len = util::get_path(url).length();
    if (p_len > url_max_len) {
      url_max_len = p_len;
    }

    double score = 0;
    size_t matches = 0;

		for (size_t i = 0; i < postings.size(); i++) {
      if (indexes[i] < postings[i].size()) {
        if (postings[i][indexes[i]].first == url) {
          score += postings[i][indexes[i]].second;
          matches++;
          indexes[i]++;
        }
      }
    }

    score *= matches * matches;
    spdlog::debug("url {} -- {} : {}", matches, score, url);

    result.emplace_back(url, score);
  }

  if (url_max_len  == 0) {
    spdlog::info("bad url max len");
    return {};
  }

  double sum_scores = 0;

  for (auto &p: result) {
    spdlog::info("raw        {} : {}", p.second, p.first);

    size_t p_len = util::get_path(p.first).length();

    if (p_len > 0) {
      
      float c = p_len  / url_max_len;

      float a = 1.0 - 0.8 * c;

      p.second *= a;
    
      spdlog::info("adjust url {} / {}", p_len, url_max_len);
    }

    if (p.first.find("?") != std::string::npos) {
      p.second *= 0.1;
      spdlog::info("q   adjust");
    }

    spdlog::info("url adjust {} : {}", p.second, p.first);

    sum_scores += p.second;
  }

  if (sum_scores <= 0) {
    spdlog::warn("bad sum score");
    return {};
  }

  for (auto &p: result) {
    p.second /= sum_scores;

    spdlog::info("sum adjust {} : {}", p.second, p.first);
  }

	return result;
}

}
