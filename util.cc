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

#include <vector>
#include <set>
#include <map>
#include <string>
#include <iostream>
#include <fstream>

#include "util.h"

namespace util {

bool has_suffix(std::string const &s, std::string const &suffix) {
  if (s.length() >= suffix.length()) {
    return (0 == s.compare(s.length() - suffix.length(), suffix.length(), suffix));
  } else {
    return false;
  }
}

bool has_prefix(std::string const &s, std::string const &prefix) {
  if (s.length() >= prefix.length()) {
    return (0 == s.compare(0, prefix.length(), prefix));
  } else {
    return false;
  }
}

std::string get_proto(std::string url) {
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

std::string get_host(std::string url) {
  int chars = 0, slashes = 0;
  bool need_host = false;
  std::vector<char> s;

  for (auto c: url) {
    chars++;

    if (c != '/' && c != ':' && c != '@'
            && !('a' <= c && c <= 'z')
            && !('A' <= c && c <= 'Z')
            && !('0' <= c && c <= '9')
            && c != '.' && c != '-')
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
          s.push_back(c);
        }
      }
    }
  }

  return std::string(s.begin(), s.end());
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

  if (!cur.empty()) {
    path.push_back(cur);
  }

  return path;
}

std::string get_path(std::string url) {
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

std::string get_dir(std::string path) {
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

std::string make_path(std::string url) {
  auto host = get_host(url);
  if (host.empty()) {
    printf("make path bad input '%s'\n", url.c_str());
    return "junk_path";
  }

  auto path = get_path(url);
  auto path_parts = split_path(path);

  auto file_path = host;

  mkdir(host.c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);

  if (path_parts.empty()) {
    return file_path + "/index";
  }

  for (int i = 0; i < path_parts.size(); i++) {
    auto &part = path_parts[i];

    bool need_dir = i + 1 < path_parts.size();
    bool exists = false;

    auto p = file_path + "/" + part;

    struct stat s;
    if (stat(p.c_str(), &s) != -1) {

      bool is_dir = (s.st_mode & S_IFMT) == S_IFDIR;
      bool is_file = (s.st_mode & S_IFMT) == S_IFDIR;

      if (need_dir && !is_dir) {
        p = file_path + "/" + part + "_dir";
      } else if (!need_dir && is_dir) {
        p = file_path + "/" + part + "/index";
      }

      exists = stat(p.c_str(), &s) != -1;

    } else {
      exists = false;
    }

    file_path = p;

    if (!exists) {
      if (need_dir) {
        mkdir(file_path.c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);

      } else {
        int fd = creat(file_path.c_str(), S_IRUSR | S_IWUSR);
        if (fd > 0) {
          close(fd);
        }
      }
    }
  }

  return file_path;
}

bool bare_minimum_valid_url(std::string url) {
  if (url.length() >= max_url_len) {
    return false;
  }

  for (auto &c : url) {
    if (c == ' ' || c == '\t' || c == '\n' || c == '\r') {
      return false;
    }
  }

  return true;
}

std::vector<std::string> load_list(std::string path) {
  std::ifstream file;
  std::vector<std::string> values;

  printf("load %s\n", path.c_str());

  file.open(path, std::ios::in);

  if (!file.is_open()) {
    fprintf(stderr, "error opening file %s\n", path.c_str());
    return values;
  }

  std::string line;
  while (getline(file, line)) {
    if (line.empty() || line.front() == '#')
      continue;
    printf("     %s\n", line.c_str());
    values.push_back(line);
  }

  printf("\n");

  file.close();

  return values;
}

}
