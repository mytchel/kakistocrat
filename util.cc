#include <string.h>
#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include <sys/stat.h>
#include <sys/types.h>

#include <vector>
#include <set>
#include <map>
#include <string>
#include <iostream>
#include <fstream>

#include "util.h"

namespace util {

std::string get_host(std::string url) {
  int slashes = 0;
  std::vector<char> s;

  for (auto c: url) {
    if (c == '/') {
      slashes++;
      continue;
    }

    if (slashes == 2) {
      s.push_back(c);
    } else if (slashes == 3) {
      break;
    }
  }

  return std::string(s.begin(), s.end());
}

std::string get_path(std::string url) {
  int slashes = 0;
  std::vector<char> s;

  for (auto c: url) {
    if (slashes >= 3) {
      s.push_back(c);

    } else if (c == '/') {
      slashes++;
      continue;
    }
  }

  return std::string(s.begin(), s.end());
}

std::pair<std::string, std::string> split_dir(std::string path) {
  std::string dir = "", f = "";

  for (auto &c : path) {
    if (c == '/') {
      dir += f + "/";
      f = "";
    } else {
      f += c;
    }
  }

  return std::pair<std::string, std::string>(dir, f);
}

std::string normalize_path(std::string s) {
  std::string n = "";

  for (auto &c : s) {
    if (c == '?') {
      n += "_bad";
      break;

    } else if (c == '\t' || c == '\n') {
      n += "_bad";
      break;

    } else if (c == '&' || c == '*' || c == '!' 
        || c == '@' || c == '#' || c == '$' 
        || c == '%' || c == '^') 
    {
      n += "_junk";
      break;
    
    } else if (c == '.' && n.length() > 0 && n.back() == '.') {
      n += "_dots";
      break;

    } else {
      n += c;
    }
  }

  return n;
}

std::string make_path(std::string host, std::string url) {
  auto path = get_path(url);

  path = normalize_path(path);

  auto p = split_dir(path);

  auto dir = p.first;
  auto file = p.second;

  if (file == "") {
    file = "index";
  }

  return host + "/" + dir + "/" + file;
}

bool bare_minimum_valid_url(std::string url) {
  if (url.length() >= max_url_len) {
    return false;
  }

  for (auto &c : url) {
    if (c == '\t' || c == '\n') {
      return false;
    }
  }

  return true;
}

std::string simplify_url(std::string url) {
  std::string n;

  for (auto &c: url) {
    if (c == '?') break;
    else if (c == '&') break;
    else if (c == '#') break;
    else n += c;
  }

  return n;
}

void load_index(std::string host,
      std::map<std::string, std::string> &urls)
{
  std::ifstream file;

  auto path = host + "_index";

  printf("load index %s\n", path.c_str());

  file.open(path, std::ios::in);

  if (!file.is_open()) {
    fprintf(stderr, "error opening file %s\n", path.c_str());
    return;
  }

  std::string line;
  while (getline(file, line)) {
    std::string url, path;
    bool haveTab = false;
    for (auto &c: line) {
      if (c == '\t') haveTab = true;
      else if (haveTab) path += c;
      else url += c;
    }

    printf("load index -- %s -> %s\n", url.c_str(), path.c_str());

    urls.insert(std::pair<std::string, std::string>(url, path));
  }

  file.close();
}

void save_index(std::string host,
      std::map<std::string, std::string> &urls)
{
  std::ofstream file;
  
  auto path = host + "_index";

  printf("save index %lu -> %s\n", urls.size(), path.c_str());

  file.open(path, std::ios::out | std::ios::trunc);
  
  if (!file.is_open()) {
    fprintf(stderr, "error opening file %s\n", path.c_str());
    return;
  }

  for (auto &u : urls) {
    file << u.first << "\t" << u.second << "\n";
  }

  file.close();
}

void load_other(std::string host,
      std::set<std::string> &urls)
{
  std::ifstream file;

  auto path = host + "_other";

  printf("load other %s\n", path.c_str());

  file.open(path, std::ios::in);

  if (!file.is_open()) {
    fprintf(stderr, "error opening file %s\n", path.c_str());
    return;
  }

  std::string line;
  while (getline(file, line)) {
    printf("load other -- %s\n", line.c_str());
    urls.insert(line);
  }

  file.close();
}

void save_other(std::string host,
      std::set<std::string> &urls)
{
  std::ofstream file;
  
  auto path = host + "_other";

  printf("save other %lu -> %s\n", urls.size(), path.c_str());

  file.open(path, std::ios::out | std::ios::trunc);
  
  if (!file.is_open()) {
    fprintf(stderr, "error opening file %s\n", path.c_str());
    return;
  }

  for (auto &u : urls) {
    file << u << "\n";
  }

  file.close();
}

}

