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
#include <assert.h>

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
    std::vector<post> &postings,
    std::vector<std::pair<std::string, uint32_t>> &pages,
    double avgdl)
{
  if (postings.empty()) {
    return {};
  }

  std::vector<std::pair<std::string, double>> pairs_ranked;

  pairs_ranked.reserve(postings.size());

  // IDF = ln(N/df_t)
  double wt = log(pages.size() / postings.size());
  size_t p_i = 0;
  for (auto &p: postings) {
    spdlog::trace("have pair {} : {},{} ({})", p_i++, p.id, p.count, pages.size());

    assert(p.id < pages.size());

    auto &page = pages.at(p.id);
    spdlog::trace("{} = {} : {}", p.id, page.first, page.second);

    std::string &page_url = page.first;
    double docLength = page.second;
    double tf = p.count;

    if (docLength == 0 || page_url == "") {
      continue;
    }

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

void searcher::find_part_matches(
    index_reader &part,
    const std::string &term,
    std::vector<std::vector<std::pair<std::string, double>>> &postings)
{
  spdlog::debug("find term {} in {}", term, part.path);

  auto pairs = part.find(term);

  spdlog::debug("have pair {} with {} docs", term, pairs.size());
  auto pairs_ranked = rank(pairs, info.pages, info.average_page_length);

  spdlog::debug("have ranked {} with {} docs", term, pairs_ranked.size());
  postings.push_back(pairs_ranked);
}

void searcher::find_matches(
    std::map<uint32_t, std::string> &parts,
    std::list<std::string> &terms,
    std::vector<std::vector<std::pair<std::string, double>>> &postings)
{
  spdlog::info("search");
  for (auto &term: terms) {
    spdlog::info("search {}", term);
    uint32_t h = part_split(term, info.parts);

    spdlog::info("search {} -> part {}", term, h);

    auto it = parts.find(h);
    if (it != parts.end()) {
      spdlog::info("have part {}", h);

      auto &path = it->second;
      spdlog::info("load {}", path);
      search::index_reader part(path, max_part_size);
      part.load();
      find_part_matches(part, term, postings);
    } else {
      spdlog::info("no part for {}", h);
    }
  }
}

std::vector<std::vector<std::pair<std::string, double>>> searcher::find_matches(char *line)
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
      spdlog::trace("{} : {}", p.second, p.first);
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
    spdlog::trace("url {} -- {} : {}", matches, score, url);

    result.emplace_back(url, score);
  }

  if (url_max_len  == 0) {
    spdlog::info("bad url max len");
    return {};
  }

  double sum_scores = 0;

  for (auto &p: result) {
    spdlog::trace("raw        {} : {}", p.second, p.first);

    size_t p_len = util::get_path(p.first).length();

    if (p_len > 0) {

      float c = p_len  / url_max_len;

      float a = 1.0 - 0.8 * c;

      p.second *= a;

      spdlog::trace("adjust url {} / {}", p_len, url_max_len);
    }

    if (p.first.find("?") != std::string::npos) {
      p.second *= 0.1;
      spdlog::trace("q   adjust");
    }

    spdlog::trace("url adjust {} : {}", p.second, p.first);

    sum_scores += p.second;
  }

  if (sum_scores <= 0) {
    spdlog::warn("bad sum score");
    return {};
  }

  for (auto &p: result) {
    p.second /= sum_scores;

    spdlog::trace("sum adjust {} : {}", p.second, p.first);
  }

  return result;
}

}
