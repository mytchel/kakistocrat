#include <string.h>
#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>

#include <sys/stat.h>
#include <sys/types.h>

#include "spdlog/spdlog.h"

#include <vector>
#include <set>
#include <map>
#include <string>
#include <iostream>
#include <fstream>
#include <cassert>

#include "util.h"

namespace util {

bool has_suffix(const std::string &s, const std::string &suffix) {
  if (s.length() >= suffix.length()) {
    return (0 == s.compare(s.length() - suffix.length(), suffix.length(), suffix));
  } else {
    return false;
  }
}

bool has_prefix(const std::string &s, const std::string &prefix) {
  if (s.length() >= prefix.length()) {
    return (0 == s.compare(0, prefix.length(), prefix));
  } else {
    return false;
  }
}

std::string get_proto(const std::string &url) {
  int slashes = 0;
  std::vector<char> s;

  for (auto c: url) {
    if (c == ':') {
      return std::string(s.begin(), s.end());

    } else if (
        !('a' <= c && c <= 'z') &&
        !('A' <= c && c <= 'Z') &&
        !('0' <= c && c <= '9')) {
      break;

    } else {
      s.push_back(tolower(c));
    }
  }

  return "";
}

std::string get_host_from_meta_path(const std::string &path) {
  std::string host;
  std::string part;
  
  for (auto c: path) {
    if (c == '/') {
      host = "";
      part = "";

    } else if (c == '.') {
      host += part;
      part = ".";
    } else {
      part += c;
    }
  }

  spdlog::info("got host from path {} -> {}", path, host);

  return host;
}

std::string get_host(const std::string &url) {
  int chars = 0, slashes = 0;
  bool need_host = false;
  std::vector<char> s;

  for (auto c: url) {
    chars++;

    if (c != '/' && c != ':' && c != '@'
            && !('a' <= c && c <= 'z')
            && !('A' <= c && c <= 'Z')
            && !('0' <= c && c <= '9')
            && c != '-' && c != '_'
            && c != '.')
    {
      return "";

    } else if (slashes == 0 && c == ':') {
      need_host = true;

    } else if (c == '/') {
      slashes++;
      if (chars == 2 && slashes == 2) {
        need_host = true;
      } else if (slashes == 3) {
        break;
      }

    } else if (need_host) {
      if (slashes == 2) {
        if (c == '@') {
          s.clear();
        } else {
          s.push_back(tolower(c));
        }
      }
    }
  }

  bool have_dot = false;
  for (auto c: s) {
    if (c == '.') {
      have_dot = true;
      break;
    }
  }

  if (have_dot && s.size() > 3) {
    return std::string(s.begin(), s.end());
  } else {
    return "";
  }
}

std::vector<std::string> split_path(std::string s) {
  std::vector<std::string> path;
  std::string cur;

  for (auto &c: s) {
    if (c == '/') {
      if (cur == ".") {
        cur = "";

      } else if (cur == "..") {
        if (!path.empty()) {
          cur = path.back();
          path.pop_back();

        } else {
          cur = "";
        }

      } else if (!cur.empty()) {
        path.push_back(cur);
        cur = "";
      }
    } else {
      cur += c;
    }
  }

  if (cur == ".") {
    cur = "";

  } else if (cur == "..") {
    if (!path.empty()) {
      cur = path.back();
      path.pop_back();

    } else {
      cur = "";
    }

  } else if (!cur.empty()) {
    path.push_back(cur);
  }

  return path;
}

std::string get_path(const std::string &url) {
  int slashes = 0;
  bool need_host = false;
  std::vector<char> s;

  for (auto c: url) {
    if (slashes == 0 && c == ':') {
      need_host = true;
      s.clear();

    } else if (need_host) {
      if (c == '/') {
        if (++slashes == 3) {
          need_host = false;
          s.push_back(c);
        }
      }

    } else if (c == '#') {
      break;

    } else {
      s.push_back(c);
    }
  }

  auto parts = split_path(std::string(s.begin(), s.end()));
  std::string path = "";
  for (auto part: parts) {
    path += "/" + part;
  }

  if (path.empty()) {
    path = "/";
  }

  return path;
}

std::string get_dir(const std::string &path) {
  auto parts = split_path(path);
  std::string dir = "";

  auto p = parts.begin();
  while (p < parts.end()) {
    dir += "/" + *p;
    p++;
  }

  if (dir.empty()) {
    dir = "/";
  }

  return dir;
}

void make_path(const std::string &path) {
  auto path_parts = split_path(path);

  if (path_parts.empty()) return;

  std::string file_path = "";
  for (int i = 0; i < path_parts.size(); i++) {
    auto &part = path_parts[i];

    file_path += part + "/";

    struct stat s;
    if (stat(file_path.c_str(), &s) != -1) {

      bool is_dir = (s.st_mode & S_IFMT) == S_IFDIR;

      if (!is_dir) {
        spdlog::error("make path failed for {}", path);
        return;
      }

    } else {
      mkdir(file_path.c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
    }
  }
}

bool bare_minimum_valid_url(const std::string &url) {
  if (url.length() + 1 >= max_url_len) {
    return false;
  }

  auto cc = url.c_str();

  for (size_t i = 0; i < url.length(); i++) {
    char c = cc[i];

    // covers all control characters, new lines, tabs, etc
    if ((int) c < ' ') {
      return false;

    // del, & would it returns non ascii?
    } else if ((int) c >= 127) {
      return false;
    }
  }

  return true;
}

std::vector<std::string> load_list(const std::string &path) {
  std::ifstream file;
  std::vector<std::string> values;

  spdlog::info("load {}", path);

  file.open(path, std::ios::in);

  if (!file.is_open()) {
    spdlog::error("error opening file {}", path);
    return values;
  }

  std::string line;
  while (getline(file, line)) {
    if (line.empty() || line.front() == '#')
      continue;
    values.push_back(line);
  }

  file.close();

  return values;
}

std::string url_decode(const std::string &in) {
  std::string out;
  int i, ii;
  char ch;

  for (i = 0; i < in.length(); i++) {
    if (in[i] == '%') {
      sscanf(in.substr(i+1, 2).c_str(), "%x", &ii);
      ch = static_cast<char>(ii);
      out += ch;
      i = i + 2;
    } else {
      out += in[i];
    }
  }

  return out;
}

float get_rand() {
  return static_cast <float> (rand()) / static_cast <float> (RAND_MAX);
}

}
