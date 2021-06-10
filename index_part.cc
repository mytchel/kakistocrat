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

using namespace std::chrono_literals;

using nlohmann::json;

namespace search {

void index_part::load()
{
  if (!load_backing()) {
    return;
  }

  uint32_t page_count = ((uint32_t *)backing)[0];
  uint32_t posting_count = ((uint32_t *)backing)[1];
  uint32_t page_data_size = ((uint32_t *)backing)[2];

  size_t offset = sizeof(uint32_t) * 3;

  size_t offsets_size = sizeof(uint32_t) * 2 * page_count;

  uint32_t *page_offsets = (uint32_t *) (backing + offset);
  uint8_t *page_data = backing + offset + offsets_size;

  uint8_t *posting_data = backing + offset + offsets_size + page_data_size;

  pages.reserve(page_count);

  for (size_t i = 0; i < page_count; i++) {
    uint32_t p_offset = page_offsets[i*2];
    uint32_t p_len = page_offsets[i*2+1];

    pages.emplace_back((const char *) page_data + p_offset, p_len);
  }

  size_t posting_offset = 0;
  for (size_t i = 0; i < posting_count; i++) {
    key k(posting_data + posting_offset);
    posting_offset += k.size();

    posting p(posting_data + posting_offset);
    posting_offset += p.size();

    stores[0].emplace_front(k, std::move(p));

    update_index(&(*stores[0].begin()));
  }
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

std::pair<size_t, size_t> save_postings_to_buf(
    forward_list<std::pair<key, posting>, fixed_memory_pool>::iterator start,
    forward_list<std::pair<key, posting>, fixed_memory_pool>::iterator end,
    uint8_t *buffer, size_t buffer_len)
{
  size_t count = 0;
  size_t offset = 0;

  for (auto p = start; p != end; p++) {
    if (offset + p->first.size() + p->second.size() >= buffer_len) {
      spdlog::warn("buffer too small to save");
      throw std::runtime_error("index part buffer too small for postings");
    }

    memcpy(buffer + offset, p->first.data(), p->first.size());
    offset += p->first.size();

    offset += p->second.save(buffer + offset);

    count++;
  }

  return std::make_pair(offset, count);
}

std::pair<size_t, size_t> save_pages_to_buf(
    std::vector<std::string> &pages,
    uint8_t *buffer, size_t buffer_len)
{
  uint32_t *offsets = ((uint32_t *) buffer) + 1;
  size_t offsets_size = sizeof(uint32_t) * 2 * pages.size();

  if (offsets_size + sizeof(uint32_t) >= buffer_len) {
    spdlog::warn("buffer too small to save");
    throw std::runtime_error("index part buffer too small for pages");
  }

  uint8_t *data = buffer + sizeof(uint32_t) + offsets_size;

  uint32_t index = 0;
  uint32_t offset = 0;

  for (auto p: pages) {
    if (offsets_size + offset + p.size() >= buffer_len) {
      spdlog::warn("buffer too small to save");
      throw std::runtime_error("index part buffer too small for pages");
    }

    offsets[index++] = offset;
    offsets[index++] = p.size();

    size_t i = 0;
    for (auto c: p) {
      data[offset++] = c;
      i++;
    }

    data[offset++] = 0;
    i++;
  }

  ((uint32_t *) buffer)[0] = offset;

  return std::make_pair(sizeof(uint32_t) + offsets_size + offset, pages.size());
}

void index_part::save(uint8_t *buffer, size_t buffer_len)
{
  if (store_split.size() > 0) {
    spdlog::critical("index_part::save not implimented for split stores");
    return;
  }

  size_t page_count = 0;
  size_t post_count = 0;
  size_t offset = sizeof(uint32_t) * 2;

  auto rpage = save_pages_to_buf(pages,
      buffer + offset, buffer_len - offset);

  offset += rpage.first;
  page_count = rpage.second;

  auto rpost = save_postings_to_buf(
      stores[0].begin(), stores[0].end(),
      buffer + offset, buffer_len - offset);

  offset += rpost.first;
  post_count = rpost.second;

  ((uint32_t *) buffer)[0] = page_count;
  ((uint32_t *) buffer)[1] = post_count;

  write_buf(path, buffer, offset);
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

  spdlog::info("load {:4} kb from {}", part_size / 1024, path);

  file.open(path, std::ios::in | std::ios::binary);

  if (!file.is_open() || file.fail() || !file.good() || file.bad()) {
    spdlog::warn("error opening file {}",  path);
    return false;
  }

  file.read((char *) backing, part_size);

  file.close();

  return true;
}

void index_part::insert(std::string s, uint32_t val) {
  if (s.size() == 0 || s.size() > key_max_len)
    return;

  size_t key_len = key_size(s);

  uint32_t hash_key = hash(s, htcap);

  auto it = index[hash_key].before_begin();
  auto end = index[hash_key].end();

  while (true) {
    auto i = std::next(it);

    if (i == end) {
      break;
    } else if ((*i)->first == s) {
      return (*i)->second.append(val, 1,
          [this](size_t len) {
            return post_backing.get(len);
          });
    } else if ((*i)->first.size() > key_len) {
      break;
    }

    it++;
  }

  key k(key_backing.get(key_len), s);

  auto store  = get_store(k);
  store->emplace_front(k, posting());

  auto &ref = store->front();

  ref.second.append(val, 1,
      [this](size_t len) {
        return post_backing.get(len);
      });

  index[hash_key].emplace_after(it, &ref);
}

void index_part::update_index(std::pair<key, posting> *ref)
{
  uint32_t hash_key = hash(ref->first.c_str(), ref->first.len(), htcap);

  size_t s = ref->first.size();

  auto it = index[hash_key].before_begin();
  auto end = index[hash_key].end();

  while (true) {
    auto i = std::next(it);

    if (i == end) {
      break;
    } else if ((*i)->first.size() >= s) {
      break;
    }

    it++;
  }

  index[hash_key].emplace_after(it, ref);
}


std::tuple<
  bool,

  forward_list<
    std::pair<key, posting> *,
    fixed_memory_pool
  > *,

  forward_list<
    std::pair<key, posting> *,
    fixed_memory_pool
  >::iterator
>
index_part::find(key k)
{
  uint32_t hash_key = hash(k.c_str(), k.len(), htcap);

  auto s = k.size();

  auto in = &index[hash_key];

  auto it = in->before_begin();
  auto end = in->end();

  while (true) {
    auto i = std::next(it);

    if (i == end) {
      break;
    } else if ((*i)->first == k) {
      return std::make_tuple(true, in, i);
    } else if ((*i)->first.size() > s) {
      break;
    }

    it++;
  }

  return std::make_tuple(false, in, it);
}

std::pair<key, posting> * index_part::find(std::string ss)
{
  uint32_t hash_key = hash(ss, htcap);
  size_t s = key_size(ss);

  for (auto i: index[hash_key]) {
    if (i->first == ss) {
      return i;
    } else if (i->first.size() > s) {
      break;
    }
  }

  return nullptr;
}

void index_part::merge(index_part &other)
{
  size_t added = 0;
  size_t skipped = 0;
  size_t total_keys = 0;

  size_t key_buf_size = 1024 * 1024;

  uint32_t pages_offset = pages.size();

  pages.reserve(pages.size() + other.pages.size());
  for (auto p: other.pages) {
    pages.push_back(p);
  }

  auto o_it = other.stores[0].begin();
  while (o_it != other.stores[0].end()) {
    total_keys++;

    if (o_it->first < start) {
      o_it++;
      skipped++;
      continue;
    }

    if (end && o_it->first >= *end) {
      o_it++;
      skipped++;
      continue;
    }

    auto start = std::chrono::system_clock::now();

    auto f = find(o_it->first);

    auto end = std::chrono::system_clock::now();
    find_total += end - start;

    if (std::get<0>(f)) {
      auto start = std::chrono::system_clock::now();

      auto r = std::get<2>(f);
      (*r)->second.merge(o_it->second, pages_offset,
          [this](size_t s) {
            return post_backing.get(s);
          });

      auto end = std::chrono::system_clock::now();
      merge_total += end - start;

    } else {
      auto start = std::chrono::system_clock::now();

      size_t c_size = o_it->first.size();

      uint8_t *c_buf = key_backing.get(c_size);

      memcpy(c_buf, o_it->first.data(), c_size);

      stores[0].emplace_front(key(c_buf), posting());
      auto &n_it = stores[0].front();

      n_it.second.merge(o_it->second, pages_offset,
          [this](size_t s) {
            return post_backing.get(s);
          });

      auto in = std::get<1>(f);
      auto it = std::get<2>(f);
      in->emplace_after(it, &n_it);

      auto end = std::chrono::system_clock::now();
      index_total += end - start;

      added++;
    }

    o_it++;
  }

  if (skipped > 0) {
    spdlog::info("merge skipped {} / {} keys", skipped, total_keys);
  }
}

}
