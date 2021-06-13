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
