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

void index_reader::load()
{
  struct stat s;
  if (stat(path.c_str(), &s) == -1) {
    throw std::runtime_error(fmt::format("load backing failed {}, no file", path));
  }

  part_size = s.st_size;

  if (part_size > buf_len) {
    throw std::runtime_error("part to big");
  }

  std::ifstream file;

  spdlog::info("load {:4} kb from {}", part_size / 1024, path);

  file.open(path, std::ios::in | std::ios::binary);

  if (!file.is_open() || file.fail() || !file.good() || file.bad()) {
    spdlog::warn("error opening file {}",  path);
  }

  file.read((char *) buf, part_size);

  file.close();

  meta = *((index_meta *) buf);

  keys = (key_block_reader*) (buf + meta.htable_base);
  htcap = meta.htcap;

  postings = (posting_reader *) (buf + meta.posting_meta_base);
  posting_count = meta.posting_count;

  key_meta_backing.setup(buf + meta.key_meta_base);
  key_data_backing.setup(buf + meta.key_data_base);
  posting_backing.setup(buf + meta.posting_data_base);
}

std::vector<post> index_reader::find(const std::string &s)
{
  uint32_t hash_key = hash(s, htcap);

  auto &key_b = keys[hash_key];
  auto posting_id = key_b.find(key_meta_backing, key_data_backing, s);
  if (posting_id) {
    auto &posting = postings[*posting_id];
    return posting.decompress(posting_backing);
  } else {
    return {};
  }
}

void index_writer::write_buf(const std::string &path, uint8_t *buf, size_t len)
{
  std::ofstream file;

  file.open(path, std::ios::out | std::ios::binary | std::ios::trunc);

  if (!file.is_open()) {
    throw std::runtime_error(fmt::format("error opening file {}", path));
  }

  spdlog::info("writing {:4} kb to {}", len / 1024, path);

  file.write((const char *) buf, len);

  file.close();
}

void index_writer::save(const std::string &path, uint8_t *buf, size_t max_len)
{
  size_t key_meta_size = 0;
  size_t key_count = 0;

  for (size_t i = 0; i < htcap; i++) {
    auto &key = keys[i];
    key_count += key.items;
  }

  key_meta_size = key_count * (sizeof(uint8_t) + sizeof(uint32_t) * 2);

  spdlog::info("saving {}, keys, {} postings", key_count, postings.size());

  size_t htable_size = htcap * sizeof(uint32_t) * 2;
  size_t posting_meta_size = postings.size() * sizeof(uint32_t) * 2;

  size_t htable_base = 128;
  size_t key_meta_base = htable_base + htable_size;
  size_t posting_meta_base = key_meta_base + key_meta_size;

  size_t key_data_base = posting_meta_base + posting_meta_size;
  size_t posting_data_base = 0; // after key data

  if (key_data_base >= max_len) {
    throw std::runtime_error(fmt::format("too much data key data > max len. {} keys, {} postings",
          key_meta_size, postings.size()));
  }

  uint32_t *htable_data = (uint32_t *) (buf + htable_base);
  uint8_t *key_meta = buf + key_meta_base;
  uint8_t *key_data = buf + key_data_base;

  size_t key_meta_offset = 0;
  size_t key_data_offset = 0;

  for (size_t i = 0; i < htcap; i++) {
    auto &key = keys[i];
    auto entries = key.load(key_meta_backing, key_data_backing);

    htable_data[i*2+0] = (uint32_t) key.items;
    htable_data[i*2+1] = (uint32_t) key_meta_offset;

    uint8_t *lens = (key_meta + key_meta_offset + 2 * key.items * sizeof(uint32_t));
    uint32_t *offsets = (uint32_t *) (key_meta + key_meta_offset + 0 * key.items * sizeof(uint32_t));
    uint32_t *ids = (uint32_t *) (key_meta + key_meta_offset + 1 * key.items * sizeof(uint32_t));

    for (size_t j = 0; j < entries.size(); j++) {
      if (key_data_base + key_data_offset  >= max_len) {
        throw std::runtime_error(fmt::format("too much data key data > max len"));
      }

      memcpy(key_data + key_data_offset, entries[j].key.data(), entries[j].key.size());

      offsets[j] = key_data_offset;
      lens[j] = entries[j].key.size();
      ids[j] = entries[j].posting_id;

      key_data_offset += entries[j].key.size();
    }

    key_meta_offset += key.items * (sizeof(uint8_t) + sizeof(uint32_t) * 2);
  }

  posting_data_base = key_data_base + key_data_offset;

  uint32_t *posting_meta_data = (uint32_t *) (buf + posting_meta_base);
  uint8_t *posting_data = buf + posting_data_base;

  uint32_t posting_data_offset = 0;

  for (size_t i = 0; i < postings.size(); i++) {
    auto &posting = postings[i];

    posting_meta_data[i*2+0] = posting.len;
    posting_meta_data[i*2+1] = posting_data_offset;

    uint8_t *data = posting_backing.get_data(posting.offset);

    if (posting_data_base + posting_data_offset >= max_len) {
      throw std::runtime_error(fmt::format("too much data posting data > max len"));
    }

    memcpy(posting_data + posting_data_offset, data, posting.len);

    posting_data_offset += posting.len;
  }

  index_meta *m = (index_meta *) buf;
  m->htcap = htcap;
  m->htable_base = htable_base;
  m->htable_size = htable_size;
  m->key_meta_base = key_meta_base;
  m->key_meta_size = key_meta_size;
  m->key_data_base = key_data_base;
  m->key_data_size = key_data_offset;
  m->posting_count = postings.size();
  m->posting_meta_base = posting_meta_base;
  m->posting_meta_size = posting_meta_size;
  m->posting_data_base = posting_data_base;
  m->posting_data_size = posting_data_offset;

  spdlog::info("writing {} with k meta: {:4} kb k data: {:4} key, p meta: {:4} kb, p data: {:4}",
    path,
    m->key_meta_size / 1024,
    m->key_data_size / 1024,
    m->posting_meta_size / 1024,
    m->posting_data_size / 1024);

  write_buf(path, buf, posting_data_base + posting_data_offset);
}

void index_writer::merge(index_reader &other, uint32_t page_id_offset)
{
  assert(htcap == other.htcap);

  if (other.posting_count == 0) {
    return;
  }

  size_t added = 0;
  size_t skipped = 0;
  size_t total_keys = 0;

  for (size_t h = 0; h < other.htcap; h++) {
    auto &key_m = keys[h]; // Make it more flexible?
    auto &key_o = other.keys[h];

    auto entries = key_o.load(other.key_meta_backing, other.key_data_backing);

    for (auto &entry: entries) {
      // TODO: check entry.key for different start / end of part.

      assert(entry.posting_id < other.posting_count);
      auto &posting_other = other.postings[entry.posting_id];

      auto posting_id = key_m.find(key_meta_backing, key_data_backing, entry.key);
      if (posting_id) {
        auto &posting = postings.at(*posting_id);
        posting.merge(posting_backing,
              posting_other, other.posting_backing,
              page_id_offset);

      } else {
        uint32_t new_id = postings.size();
        auto &posting = postings.emplace_back();

        posting.merge(posting_backing,
              posting_other, other.posting_backing,
              page_id_offset);

        key_m.add(key_meta_backing, key_data_backing, entry.key, new_id, key_base_items);
      }
    }
  }
}

void index_writer::insert(const std::string &s, uint32_t page_id)
{
  if (s.size() > key_max_len) {
    return;
  }

  uint32_t hash_key = hash(s, htcap);

  auto &key_b = keys[hash_key];
  auto posting_id = key_b.find(key_meta_backing, key_data_backing, s);

  if (posting_id) {
    auto &posting = postings[*posting_id];
    posting.append(posting_backing, page_id);

  } else {
    uint32_t new_id = postings.size();
    auto &posting = postings.emplace_back();

    posting.append(posting_backing, page_id);

    key_b.add(key_meta_backing, key_data_backing, s, new_id, key_base_items);
  }
}

}

