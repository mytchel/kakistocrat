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

void indexer::index_site(site_map &site) {
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

    size_t page_length = 0;

    std::ifstream pfile;

    pfile.open(page.path, std::ios::in | std::ios::binary);

    if (!pfile.is_open() || pfile.fail() || !pfile.good() || pfile.bad()) {
      spdlog::warn("error opening file {}", page.path);
      continue;
    }

    pfile.read((char *) file_buf, file_buf_size);

    size_t len = pfile.gcount();

    uint32_t page_id = add_page(page.url);

    spdlog::debug("process page {} kb : {}",
      len / 1024, page.url);

    tokenizer::tokenizer tok((char *) file_buf, len);

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

        insert(word_t, s, page_id);

        if (word_allow_extra(s)) {
          if (str_length(&tok_buffer_trine) > 0) {
            str_cat(&tok_buffer_trine, " ");
            str_cat(&tok_buffer_trine, str_c(&tok_buffer));

            std::string s(str_c(&tok_buffer_trine));

            insert(trine_t, s, page_id);

            str_resize(&tok_buffer_trine, 0);
          }

          if (str_length(&tok_buffer_pair) > 0) {
            str_cat(&tok_buffer_pair, " ");
            str_cat(&tok_buffer_pair, str_c(&tok_buffer));

            std::string s(str_c(&tok_buffer_pair));

            insert(pair_t, s, page_id);

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

    set_page_size(page.url, page_length);
  }

  spdlog::info("finished indexing site {}", site.host);
}

std::map<uint32_t, std::string>
indexer::save_parts(
    std::vector<index_writer> &t,
    const std::string &base_path,
    uint8_t *buf, size_t buf_len)
{
  std::map<uint32_t, std::string> paths;

  for (size_t i = 0; i < t.size(); i++) {
    auto &p = t[i];

    auto path = fmt::format("{}.{}.dat", base_path, i);

    spdlog::info("save part {}", path);

    // TODO: don't save empty parts
    p.save(path, buf, buf_len);

    paths.emplace(i, path);
  }

  return paths;
}

std::string indexer::save(const std::string &base_path)
{
  size_t buf_len = 1024 * 1024 * 30;
  uint8_t *buf = (uint8_t *) malloc(buf_len);
  if (buf == nullptr) {
    throw std::bad_alloc();
  }

  auto words_path = fmt::format("{}.words", base_path);
  auto pairs_path = fmt::format("{}.pairs", base_path);
  auto trines_path = fmt::format("{}.trines", base_path);
  auto meta_path = fmt::format("{}.meta.json", base_path);

  index_info info(meta_path);

  info.word_parts = save_parts(word_t, words_path, buf, buf_len);
  info.pair_parts = save_parts(pair_t, pairs_path, buf, buf_len);
  info.trine_parts = save_parts(trine_t, trines_path, buf, buf_len);

  free(buf);

  info.htcap = htcap;
  info.parts = splits;

  size_t average_page_length = 0;

  if (pages.size() > 0) {
    info.pages.reserve(pages.size());

    for (auto &p: pages) {
      average_page_length += p.second;
      info.pages.emplace_back(p.first, p.second);
    }

    average_page_length /= pages.size();
  }

  info.average_page_length = average_page_length;

  info.save();

  return meta_path;
}

void indexer::insert(std::vector<index_writer> &t,
      const std::string &s, uint32_t page_id)
{
	size_t h = part_split(s, splits);

  return t[h].insert(s, page_id);
}

}
