#ifndef INDEX_H
#define INDEX_H

#include <stdint.h>
#include <optional>
#include <chrono>
#include <assert.h>

#include <nlohmann/json.hpp>

using namespace std::chrono_literals;

#include "util.h"
#include "site.h"

#include "hash.h"
#include "vbyte.h"

namespace search {

#define key_max_len 250

enum index_type{words, pairs, trines};

index_type from_str(const std::string &s);
std::string to_str(index_type type);

struct backing_piece {
  uint8_t *buf;
  size_t offset;
  size_t size;
};

struct backing_block {
  size_t base_offset;
  size_t block_size;
  size_t used{0};

  uint8_t *buf;

  backing_block(size_t o, size_t s)
  : base_offset(o), block_size(s)
  {
    buf = (uint8_t *) malloc(block_size);
    if (buf == nullptr) {
      throw std::bad_alloc();
    }
  }

  ~backing_block() noexcept {
    if (buf != nullptr) {
      free(buf);
    }
  }

  backing_block(const backing_block &b) = delete;
  backing_block(backing_block &b) noexcept
    : base_offset(b.base_offset),
      block_size(b.block_size),
      used(b.used),
      buf(b.buf)
  {
    b.buf = nullptr;
  }

  backing_block(const backing_block &&b) = delete;
  backing_block(backing_block &&b) noexcept
    : base_offset(b.base_offset),
      block_size(b.block_size),
      used(b.used),
      buf(b.buf)
  {
    b.buf = nullptr;
  }

  bool can_alloc(uint32_t size) {
    return block_size - used > size;
  }

  backing_piece alloc(uint32_t size);

  uint8_t *get_data(uint32_t offset) {
    assert(offset < block_size);

    return &buf[offset];
  }
};

struct write_backing {
  std::string name;

  size_t block_size;
  std::vector<backing_block> blocks;

  write_backing(const std::string &n, size_t block_size)
    : name(n), block_size(block_size)
  {}

  backing_piece alloc(uint32_t size);
  backing_piece realloc(uint32_t offset, uint32_t size);

  uint8_t *get_data(uint32_t offset) {
    assert(!blocks.empty());

    uint32_t n = offset / block_size;

    assert(n < blocks.size());

    return blocks[n].get_data(offset - n * block_size);
  }

  size_t usage() {
    return blocks.size() * block_size;
  }

  void clear() {
    blocks.clear();
  }
};

struct read_backing {
  uint8_t *buf{nullptr};
  size_t size{0};

  read_backing() {}

  read_backing(uint8_t *buf, size_t size)
    : buf(buf), size(size) {}

  uint8_t *get_data(uint32_t offset) {
    assert(offset < size);

    return &buf[offset];
  }
};

struct posting_reader {
  uint32_t len;
  uint32_t offset;

  struct post {
    uint32_t id;
    uint8_t count;

    post(uint32_t i, uint8_t c)
      : id(i), count(c) {}
  };

  std::vector<post> decompress(read_backing &p)
  {
    uint8_t *b = p.get_data(offset);

    std::vector<post> posts;
    posts.reserve(len);

    uint32_t id, prev_id = 0;
    uint32_t o = 0;

    for (uint32_t i = 0; i < len; i++) {
      o += vbyte_read(&b[o], &id);
      id += prev_id;
      prev_id = id;

      uint8_t count = b[o];
      o++;

      posts.emplace_back(id, count);
    }

    return posts;
  }
};

struct posting_writer {
  uint32_t last_id{0};
  uint32_t len{0}, max_len{0};
  uint32_t offset{0};

  posting_writer() {}

  uint8_t * ensure_size(write_backing &p, uint32_t need)
  {
    if (len > 0 && need < max_len) {
      return p.get_data(offset);
    }

    uint8_t *old;
    if (len > 0) {
      old = p.get_data(offset);
    }

    while (max_len < need) {
      if (max_len < 32) {
        max_len = 32;
      } else {
        max_len = max_len * 8;
      }
    }

    // TODO: realloc

    backing_piece b = p.alloc(max_len);

    offset = b.offset;
    max_len = b.size;

    if (len > 0) {
      memcpy(b.buf, old, len);
    }

    return b.buf;
  }

  void append(write_backing &p, uint32_t id, uint8_t count = 1)
  {
    if (id == last_id && len > 0) {
      uint8_t *b = p.get_data(offset);
      if (b[len-1] < 255) {
        b[len-1]++;
      }

      return;
    }

    uint8_t *b = ensure_size(p, len + 5 + 1);

    uint32_t value = id - last_id;

    len += vbyte_store(b + len, value);
    b[len++] = count;
    last_id = id;
  }

  // Can only read from readers
  void merge(write_backing &p, posting_reader &other, read_backing &op, uint32_t id_offset)
  {
    auto posts = other.decompress(op);

    ensure_size(p, len + other.len + 5);

    for (auto &post: posts) {
      append(p, post.id + id_offset, post.count);
    }
  }
};

static const uint32_t key_list_lens     = 0;
static const uint32_t key_list_offsets  = 1;
static const uint32_t key_list_ids      = 2;

struct key_entry {
  std::string key;
  uint32_t posting_id;

  entry(uint8_t *d, uint32_t l, uint32_t id)
    : key((const char *) d, l), posting_id(id)
  {}
};


struct key_block_reader {
  uint32_t items{0};
  uint32_t offset{0};

  key_block_reader() {}

  uint32_t *get_list(read_backing &m, size_t r) {
    uint8_t *b = m.get_data(offset) + (items * r);

    return (uint32_t *) b;
  }

  uint32_t *get_lens(read_backing &m) { return get_list(m, key_list_lens); }
  uint32_t *get_offsets(read_backing &m) { return get_list(m, key_list_offsets); }
  uint32_t *get_ids(read_backing &m) { return get_list(m, key_list_ids); }

  std::vector<key_entry> load(read_backing &m, read_backing &d) {
    if (items == 0) {
      return {};
    }

    uint32_t *lens = get_lens(m);
    uint32_t *offsets = get_offsets(m);
    uint32_t *ids = get_ids(m);

    std::vector<key_entry> entries;
    entries.reserve(items);

    for (size_t i = 0; i < items; i++) {
      uint8_t *data = d.get_data(offsets[i]);
      entries.emplace_back(data, lens[i], ids[i]);
    }

    return entries;
  }

  std::optional<uint32_t> find(read_backing &m, read_backing &d, const std::string &s)
  {
    if (items == 0) {
      return {};
    }

    uint32_t *lens = get_lens(m);
    uint32_t *offsets = get_offsets(m);
    uint32_t *ids = get_ids(m);

    size_t len = s.size();

    for (size_t i = 0; i < items; i++) {
      if (lens[i] == len) {
        uint8_t *data = d.get_data(offsets[i]);

        std::string is((const char *) data, len);
        if (is == s) {
          return ids[i];
        }
      }
    }

    return {};
  }
};

struct key_block_writer {
  uint16_t max_items{0};
  uint16_t items{0};
  uint32_t offset{0};

  key_block_writer() {}

  void reset() {
    max_items = 0;
    items = 0;
    offset = 0;
  }

  uint8_t *get_lens(uint8_t *b) {
    return b;
  }

  uint32_t *get_offsets(uint8_t *b) {
    return (uint32_t *) (b + max_items * (sizeof(uint8_t)));
  }

  uint32_t *get_ids(uint8_t *b) {
    return (uint32_t *) (b + max_items * (sizeof(uint8_t) + sizeof(uint32_t)));
  }

  std::vector<key_entry> load(write_backing &m, write_backing &d) {
    if (items == 0) {
      return {};
    }

    auto lens = get_lens(m);
    auto offsets = get_offsets(m);
    auto ids = get_ids(m);

    std::vector<key_entry> entries;
    entries.reserve(items);

    for (size_t i = 0; i < items; i++) {
      uint8_t *data = d.get_data(offsets[i]);
      entries.emplace_back(data, lens[i], ids[i]);
    }

    return entries;
  }

  std::optional<uint32_t> find(write_backing &m, write_backing &d, const std::string &s)
  {
    if (items == 0) {
      return {};
    }

    uint8_t *b = m.get_data(offset);

    auto lens = get_lens(b);
    auto offsets = get_offsets(b);
    auto ids = get_ids(b);

    size_t len = s.size();
    const uint8_t *s_data = (const uint8_t *) s.data();

    for (size_t i = 0; i < items; i++) {
      if (lens[i] == len) {
        const uint8_t *data = d.get_data(offsets[i]);

        if (memcmp(s_data, data, len) == 0) {
          return ids[i];
        }
      } else if (lens[i] > len) {
        break;
      }
    }

    return {};
  }

  void add(write_backing &m, write_backing &d, const std::string &s, uint32_t posting_id)
  {
    assert(s.size() < key_max_len);

    size_t loc = 0;
    uint8_t *lens;
    uint32_t *offsets;
    uint32_t *ids;

    size_t s_len = s.size();

    if (items > 0) {
      uint8_t *b = m.get_data(offset);

      uint8_t *old_lens = get_lens(b);
      uint32_t *old_offsets = get_offsets(b);
      uint32_t *old_ids = get_ids(b);

      while (loc < items && old_lens[loc] <= s_len)
        loc++;

      if (items + 1 >= max_items) {
        max_items *= 2;

        backing_piece back = m.alloc(max_items * (sizeof(uint8_t) + sizeof(uint32_t) * 2));
        offset = back.offset;

        uint8_t *n = back.buf;

        lens = get_lens(n);
        offsets = get_offsets(n);
        ids = get_ids(n);

        if (loc > 0) {
          memcpy(lens, old_lens, loc * sizeof(uint8_t));
          memcpy(offsets, old_offsets, loc * sizeof(uint32_t));
          memcpy(ids, old_ids, loc * sizeof(uint32_t));
        }

        if (loc < items) {
          memcpy(lens + loc + 1, old_lens + loc, (items - loc) * sizeof(uint8_t));
          memcpy(offsets + loc + 1, old_offsets + loc, (items - loc) * sizeof(uint32_t));
          memcpy(ids + loc + 1, old_ids + loc, (items - loc) * sizeof(uint32_t));
        }

        // TODO: free

      } else {
        lens = old_lens;
        offsets = old_offsets;
        ids = old_ids;

        memcpy(lens + loc + 1, old_lens + loc, (items - loc) * sizeof(uint8_t));
        memcpy(offsets + loc + 1, old_offsets + loc, (items - loc) * sizeof(uint32_t));
        memcpy(ids + loc + 1, old_ids + loc, (items - loc) * sizeof(uint32_t));
      }

    } else {
      max_items = 8;
      backing_piece back = m.alloc(max_items * (sizeof(uint8_t) + sizeof(uint32_t) * 2));
      offset = back.offset;

      uint8_t *n = back.buf;

      lens = get_lens(n);
      offsets = get_offsets(n);
      ids = get_ids(n);
    }

    backing_piece b = d.alloc(s_len);

    memcpy(b.buf, s.data(), s_len);

    lens[loc] = s_len;
    offsets[loc] = b.offset;
    ids[loc] = posting_id;

    items++;
  }
};

struct index_reader {
  std::string path;

  size_t htcap;

  // doc id 0 is invalid

  // hash
  key_block_reader *keys{nullptr};

  // array
  posting_reader *postings{nullptr};

  std::vector<std::string> page_urls;

  // backings
  read_backing keys_meta_backing;
  read_backing keys_data_backing;
  read_backing postings_backing;

  index_reader(const std::string &path)
    : path(path)
  {}

  void load()
  {

  }

  posting_reader * find(const std::string &s)
  {
    uint32_t hash_key = hash(s, htcap);

    auto &key_b = keys[hash_key];
    auto posting_id = key_b.find(keys_meta_backing, keys_data_backing, s);
    if (posting_id) {
      return &postings[*posting_id];
    } else {
      return nullptr;
    }
  }
};

struct index_writer {
  size_t htcap;

  // hash
  //std::vector<key_block_writer> keys;
  key_block_writer *keys{nullptr};

  // array
  std::vector<posting_writer> postings;

  // doc id 0 is invalid
  //std::vector<std::string> page_urls;

  write_backing keys_meta_backing;
  write_backing keys_data_backing;
  write_backing postings_backing;

  index_writer(size_t htcap)
    : htcap(htcap),
      keys_meta_backing("keys_meta", 1024 * 512),
      keys_data_backing("keys_data", 1024 * 128),
      postings_backing("postings", 1024 * 256)
  {
    //page_urls.push_back("INVALID");

    keys = (key_block_writer *) malloc(sizeof(key_block_writer) * htcap);
    if (keys == nullptr) {
      throw std::bad_alloc();
    }

    memset((void *) keys, 0, sizeof(key_block_writer) * htcap);
  }

  index_writer(const index_writer &o) = delete;
  index_writer(const index_writer &&o) = delete;
  index_writer(index_writer &o) = delete;

  index_writer(index_writer &&o)
    : htcap(o.htcap), keys(o.keys),
      postings(std::move(o.postings)),
      keys_meta_backing(std::move(o.keys_meta_backing)),
      keys_data_backing(std::move(o.keys_data_backing)),
      postings_backing(std::move(o.postings_backing))
  {
    o.keys = nullptr;
  }

  ~index_writer() {
    if (keys) {
      free(keys);
    }
  }

  size_t usage() {
    return keys_meta_backing.usage() +
           keys_data_backing.usage() +
           postings_backing.usage() +
           postings.size() * sizeof(posting_writer);
           //keys.size() * sizeof(key_block_writer) +
           //page_urls.size() * 64;
  }

  void clear() {
    memset((void *) keys, 0, sizeof(key_block_writer) * htcap);

    postings.clear();

    //page_urls.clear();
    //page_urls.push_back("INVALID");

    keys_meta_backing.clear();
    keys_data_backing.clear();
    postings_backing.clear();
  }

  void save(const std::string &p)
  {
    uint8_t *buf = 0;

    size_t key_meta_size = 0;

    for (size_t i = 0; i < htcap; i++) {
      auto &key = keys[i];
      key_meta_size += key.items * (sizeof(uint8_t) + sizeof(uint32_t) * 2);
    }

    size_t htable_base = 0;
    size_t key_meta_base = htable_base + sizeof(uint32_t) * 2 * htcap;
    size_t key_data_base = keys_meta_base + keys_meta_size;

    uint32_t *htable_data = (uint32_t *) (buf + htable_base);
    uint8_t *keys_meta = buf + keys_meta_base;
    uint8_t *keys_data = buf + keys_data_base;

    size_t key_meta_offset = 0;
    size_t key_data_offset = 0;

    for (size_t i = 0; i < htcap; i++) {
      auto &key = keys[i];
      auto entries = key.load(keys_meta_backing, keys_data_backing);

      htable_base[i*2+0] = key.items;
      htable_base[i*2+1] = keys_meta_offset;

      uint8_t *lens = keys_meta + keys_meta_offset;
      uint32_t *offsets = keys_meta + keys_meta_offset + key.items * sizeof(uint8_t);
      uint32_t *ids = keys_meta + keys_meta_offset + key.items * (sizeof(uint8_t) + sizeof(uint32_t));

      for (size_t j = 0; j < entries.size(); j++) {
        memcpy(keys_data + keys_data_offset, entries[j].key.data(), entries[j].key.size());

        offsets[j] = keys_data_offset;
        lens[j] = entries[j].key.size();
        ids[j] = entries[j].ids;

        keys_data_offset += entries[j].key.size();
      }

      keys_meta_offset += key.items * (sizeof(uint8_t) + sizeof(uint32_t) * 2);
    }

    size_t posting_base = keys_data_base + keys_data_offset;

    uint8_t *posting_data = buf + posting_base;

    for (auto &posting: postings) {

    }

    // write htcap
    // write keys_meta_offset
    // write keys_data_offset
    // write posting_offset
  }

  /*
  size_t add_page(const std::string &page) {
    size_t id = page_urls.size();
    page_urls.emplace_back(page);
    return id;
  }
*/

  void merge(index_reader &other)
  {
    //assert(keys.size() == other.htcap);

    size_t added = 0;
    size_t skipped = 0;
    size_t total_keys = 0;

    uint32_t page_id_offset = 0;/*page_urls.size();

    page_urls.reserve(page_urls.size() + other.page_urls.size());

    for (auto p: other.page_urls) {
      page_urls.emplace_back(p);
    }
    */

    for (size_t h = 0; h < other.htcap; h++) {
      auto &key_m = keys[h]; // Make it more flexible?
      auto &key_o = other.keys[h];

      auto entries = key_o.load(other.keys_meta_backing, other.keys_data_backing);

      for (auto &entry: entries) {
        // TODO: check entry.key for different start / end of part.

        auto &posting_other = other.postings[entry.posting_id];

        auto posting_id = key_m.find(keys_meta_backing, keys_data_backing, entry.key);
        if (posting_id) {
          auto &posting = postings[*posting_id];
          posting.merge(postings_backing,
                posting_other, other.postings_backing,
                page_id_offset);

        } else {
          uint32_t new_id = postings.size();
          auto &posting = postings.emplace_back();

          posting.merge(postings_backing,
                posting_other, other.postings_backing,
                page_id_offset);

          key_m.add(keys_meta_backing, keys_data_backing, entry.key, new_id);
        }
      }
    }
  }

  void insert(const std::string &s, uint32_t page_id)
  {
    if (s.size() > key_max_len) {
      return;
    }

    uint32_t hash_key = hash(s, htcap);

    auto &key_b = keys[hash_key];
    auto posting_id = key_b.find(keys_meta_backing, keys_data_backing, s);

    if (posting_id) {
      auto &posting = postings[*posting_id];
      posting.append(postings_backing, page_id);

    } else {
      uint32_t new_id = postings.size();
      auto &posting = postings.emplace_back();

      posting.append(postings_backing, page_id);

      key_b.add(keys_meta_backing, keys_data_backing, s, new_id);
    }
  }
};

struct index_part_info {
  std::string path;

  std::string start;
  std::optional<std::string> end;

  index_part_info() {}

  index_part_info(const std::string &p) : path(p) {}

  index_part_info(const std::string &p, const std::string &s, std::optional<std::string> e)
    : path(p), start(s), end(e) {}
};

void to_json(nlohmann::json &j, const index_part_info &i);
void from_json(const nlohmann::json &j, index_part_info &i);

struct index_info {
  std::string path;

  size_t htcap;

  uint32_t average_page_length;
  std::map<std::string, uint32_t> page_lengths;

  std::vector<index_part_info> word_parts;
  std::vector<index_part_info> pair_parts;
  std::vector<index_part_info> trine_parts;

  index_info(std::string p) : path(p) {}

  void load();
  void save();
};

struct indexer {
  size_t splits, htcap;

  std::vector<std::pair<std::string, uint32_t>> pages;

  std::vector<index_part_info> word_i, pair_i, trine_i;
  std::vector<index_writer> word_t, pair_t, trine_t;

  uint8_t *file_buf{nullptr};
  size_t file_buf_size{0};

  indexer(
      size_t splits,
      size_t htcap,
      size_t max_p)
    : splits(splits), htcap(htcap), file_buf_size(max_p)
  {
    spdlog::info("setting up");

    file_buf = (uint8_t *) malloc(file_buf_size);
    if (file_buf == nullptr) {
      throw std::bad_alloc();
    }

    for (size_t i = 0; i < splits; i++) {
      std::string start = fmt::format("{}", i);
      std::optional<std::string> end;

      spdlog::info("setting up split {}", start);

      word_i.emplace_back("", start, end);
      pair_i.emplace_back("", start, end);
      trine_i.emplace_back("", start, end);

      word_t.emplace_back(htcap);
      pair_t.emplace_back(htcap);
      trine_t.emplace_back(htcap);
    }
  }

  ~indexer() {
    if (file_buf) {
      free(file_buf);
    }
  }

  void clear() {
    pages.clear();
    for (auto &p: word_t) p.clear();
    for (auto &p: pair_t) p.clear();
    for (auto &p: trine_t) p.clear();
  }

  std::string save(const std::string &path);

  std::vector<index_part_info> save_parts(
    std::vector<index_part_info> &i,
    std::vector<index_writer> &t,
    const std::string &base_path);

  std::string flush(const std::string &base_path) {
    spdlog::info("flushing {}", base_path);

    auto meta_path = save(base_path);

    clear();

    return meta_path;
  }

  size_t usage() {
    size_t u = 0;

    for (auto &p: word_t) u += p.usage();
    for (auto &p: pair_t) u += p.usage();
    for (auto &p: trine_t) u += p.usage();

    u += pages.size() * 64;

    return u;
  }

  void index_site(site_map &site);

  void insert(std::vector<index_writer> &t,
      const std::string &s, uint32_t page_id);

  void insert(index_type t, const std::string &s, uint32_t page_id);

  uint32_t add_page(const std::string &page) {
    pages.emplace_back(page, 0);
    return pages.size() - 1;
  }

  void set_page_size(const std::string &page, uint32_t size) {
    auto &p = pages.back();
    assert (p.first == page);
    p.second = size;
  }
};

struct searcher {
  index_info info;

  searcher(std::string p) : info(p) {}

  void load() {
    info.load();
  }

  void find_part_matches(index_reader &p,
    std::string &term,
    std::vector<std::vector<std::pair<std::string, double>>> &postings);

  void find_matches(
    std::vector<index_part_info> &part_info,
    std::list<std::string> &terms,
    std::vector<std::vector<std::pair<std::string, double>>> &postings);

  std::vector<std::vector<std::pair<std::string, double>>>
    find_matches(char *line);
};

std::list<std::pair<std::string, double>>
intersect_postings(std::vector<std::vector<std::pair<std::string, double>>> &postings);

}

#endif

