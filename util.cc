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

std::string get_host(std::string url) {
  int slashes = 0;
  std::vector<char> s;

  for (auto c: url) {
    if (c == '/') {
      slashes++;
      continue;
    
    } else if (c == '@') {
      s.clear();
      continue;
    }

    if (slashes == 2) {
      s.push_back(tolower(c));
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
    if (c == '?' || c == '#') {
      break;

    } else if (c == '\t' || c == '\n') {
      n += "_bad";
      break;

    } else if (c == '&' || c == '*' || c == '!' 
        || c == '@' || c == '$' || c == '^') 
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

std::string make_path(std::string host, std::string url) {
  auto path = normalize_path(get_path(url));

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
    printf("     %s\n", line.c_str());
    values.push_back(line);
  }
    
  printf("\n");

  file.close();

  return values;
}

void save_index(std::vector<struct site> &index, std::string path)
{
  std::ofstream file;
  
  printf("save index %lu -> %s\n", index.size(), path.c_str());

  file.open(path, std::ios::out | std::ios::trunc);

  if (!file.is_open()) {
    fprintf(stderr, "error opening file %s\n", path.c_str());
    return;
  }

  for (auto &site: index) {
    bool has_pages = false;
    for (auto &p: site.pages) {
      if (p.second.path.empty()) continue;

      has_pages = true;
      break;
    }

    if (!has_pages) continue;

    file << site.host << "\t";
    file << site.level << "\n";

    for (auto &p: site.pages) {
      if (p.second.path.empty()) continue;

      file << "\t";
      file << p.first << "\t";
      file << p.second.path << "\t";
      file << p.second.refs << "\n";
    }
  }
  file.close();
}

std::vector<struct site> load_index(std::string path)
{
  std::ifstream file;
  std::vector<struct site> index;

  printf("load %s\n", path.c_str());

  file.open(path, std::ios::in);

  if (!file.is_open()) {
    fprintf(stderr, "error opening file %s\n", path.c_str());
    return index;
  }

  std::string line;
  while (getline(file, line)) {
  }

  file.close();

  return index;
}

struct site * index_find_host(
        std::vector<struct site> &index,
        std::string host
) {
  for (auto &i: index) {
    if (i.host == host) {
      return &i;
    }
  }

  return NULL;
}

}
