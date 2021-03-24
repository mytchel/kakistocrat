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

std::pair<size_t, size_t> save_postings_to_buf(
    std::list<std::pair<key, posting>>::iterator start,
    std::list<std::pair<key, posting>>::iterator end,
    uint8_t *buffer, size_t buffer_len)
{
  size_t count = 0;
  size_t offset = 0;

  for (auto p = start; p != end; p++) {
    if (offset + p->first.size() + p->second.size() >= buffer_len) {
      spdlog::warn("buffer too small");
      return std::make_pair(offset, 0);
    }

    memcpy(buffer + offset, p->first.data(), p->first.size());
    offset += p->first.size();

    offset += p->second.save(buffer + offset);

    count++;
  }

  return std::make_pair(offset, count);
}

std::pair<size_t, size_t> save_pages_to_buf(
    std::vector<uint64_t> &pages,
    uint8_t *buffer, size_t buffer_len)
{
  size_t offset = 0;

  for (auto p: pages) {
    if (offset + sizeof(uint64_t) >= buffer_len) {
      spdlog::warn("buffer too small");
      return std::make_pair(offset, 0);
    }

    *((uint64_t *) (buffer + offset)) = p;
    offset += sizeof(uint64_t);
  }

  return std::make_pair(offset, pages.size());
}

std::pair<size_t, size_t> save_pages_to_buf(
    std::list<std::pair<uint64_t, uint32_t>> &pages,
    uint8_t *buffer, size_t buffer_len)
{
  size_t offset = 0;

  for (auto &p: pages) {
    if (offset + sizeof(uint64_t) >= buffer_len) {
      spdlog::warn("buffer too small");
      return std::make_pair(offset, 0);
    }

    *((uint64_t *) (buffer + offset)) = p.first;
    offset += sizeof(uint64_t);
  }

  return std::make_pair(offset, pages.size());
}

void index_part::save()
{
  uint8_t *buffer = (uint8_t *) malloc(max_index_part_size);
  if (buffer == NULL) {
    throw std::bad_alloc();
  }

  size_t page_count = 0;
  size_t post_count = 0;
  size_t offset = sizeof(uint32_t) * 2;

  auto rpage = save_pages_to_buf(page_ids,
      buffer + offset, max_index_part_size - offset);

  offset += rpage.first;
  page_count = rpage.second;

  auto rpost = save_postings_to_buf(
      store.begin(), store.end(),
      buffer + offset, max_index_part_size - offset);

  offset += rpost.first;
  post_count = rpost.second;

  ((uint32_t *) buffer)[0] = page_count;
  ((uint32_t *) buffer)[1] = post_count;

  write_buf(path, buffer, offset);

  free(buffer);
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

size_t index_part::insert(std::string s, uint32_t val) {
  if (s.size() == 0 || s.size() > key_max_len)
    return 0;

  size_t key_len = key_size(s);

  uint32_t hash_key = hash(s);

  auto it = index[hash_key].begin();
  auto end = index[hash_key].end();

  while (it != end) {
    if (it->first == key_len) {
      if (it->second->first == s) {
        return it->second->second.append(val);
      }

    } else if (it->first > key_len) {
      break;
    }

    it++;
  }

  key k(key_backing.get(key_len), s);

  store.emplace_back(k, posting(val));

  auto ref = std::prev(store.end());

  index[hash_key].emplace(it, key_len, ref);

  return ref->first.size() + ref->second.size() + 32;
}

void index_part::update_index(std::list<std::pair<key, posting>>::iterator ref)
{
  uint32_t hash_key = hash(ref->first.c_str(), ref->first.len());
  size_t key_len = ref->first.len();

  size_t l = ref->first.size();

  auto it = index[hash_key].begin();
  auto end = index[hash_key].end();
  while (it != end) {
    if (it->first >= l) {
      break;
    }

    it++;
  }

  index[hash_key].emplace(it, l, ref);
}


std::tuple<
  bool,

  std::list<
    std::pair<
      uint8_t,
      std::list<std::pair<key, posting>>::iterator
    >
  > *,

  std::list<
    std::pair<uint8_t,
      std::list<std::pair<key, posting>>::iterator
    >
  >::iterator
>
index_part::find(key k)
{
  uint32_t hash_key = hash(k.c_str(), k.len());

  auto in = &index[hash_key];

  auto it = in->begin();
  auto end = in->end();
  while (it != end) {
    if (it->first == k.size()) {
      if (it->second->first == k) {
        return std::make_tuple(true, in, it);
      }
    } else if (it->first > k.size()) {
      break;
    }

    it++;
  }

  return std::make_tuple(false, in, it);
}

std::list<std::pair<key, posting>>::iterator index_part::find(std::string s)
{
  uint32_t hash_key = hash(s);
  size_t l = key_size(s);

  for (auto &i: index[hash_key]) {
    if (i.first == l) {
      if (i.second->first == s) {
        return i.second;
      }
    } else if (i.first > l) {
      break;
    }
  }

  return store.end();
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

    auto f = find(o_it->first);

    auto end = std::chrono::system_clock::now();
    find_total += end - start;

    if (std::get<0>(f)) {
      auto start = std::chrono::system_clock::now();

      auto r = std::get<2>(f);
      r->second->second.merge(o_it->second, page_id_offset);

      auto end = std::chrono::system_clock::now();
      merge_total += end - start;

    } else {
      auto start = std::chrono::system_clock::now();

      size_t c_len = o_it->first.size();

      uint8_t *c_buf = key_backing.get(c_len);

      memcpy(c_buf, o_it->first.data(), c_len);

      store.emplace_back(key(c_buf), posting());
      auto n_it = std::prev(store.end());

      n_it->second.merge(o_it->second, page_id_offset);

      auto in = std::get<1>(f);
      auto it = std::get<2>(f);
      in->emplace(it, c_len, n_it);

      auto end = std::chrono::system_clock::now();
      index_total += end - start;

      added++;
    }

    o_it++;
  }
}

}
