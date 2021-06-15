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

uint32_t part_split(const std::string &s, size_t parts)
{
  const char *data = s.data();
  size_t m = s.size();

  uint32_t h = 0;

  for (size_t l = 0; l < 3 && l < m; l++) {
    h += (uint32_t) (data[l] - '0');
  }

  h = h % parts;

//  spdlog::debug("{:4} <- {}", h, s);
  return h;
}

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

void index_info::save()
{
  json j = json{
      {"average_page_length", average_page_length},
      {"page_lengths", page_lengths},
      {"htcap", htcap},
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
  j.at("htcap").get_to(htcap);
  j.at("word_parts").get_to(word_parts);
  j.at("pair_parts").get_to(pair_parts);
  j.at("trine_parts").get_to(trine_parts);
}

}
