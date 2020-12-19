#include <list>
#include <vector>
#include <set>
#include <map>
#include <string>
#include <iostream>
#include <fstream>

namespace util {

const size_t max_url_len = 512;

std::string get_proto(std::string url);
std::string get_host(std::string url);
std::string get_path(std::string url);
std::string get_dir(std::string path);

std::string make_path(std::string url);

bool bare_minimum_valid_url(std::string url);

void load_index(std::string host,
      std::map<std::string, std::string> &urls);

void save_index(std::string host,
      std::map<std::string, std::string> &urls);

void load_other(std::string host,
      std::set<std::string> &urls);

void save_other(std::string host,
      std::set<std::string> &urls);

std::vector<std::string> load_list(std::string path);

struct page {
  std::string url;
  std::string path;
  int refs;
};

struct site {
  std::string host;
  bool scraped;
  size_t level;
  size_t refs;
  std::list<struct page> pages;
};

void save_index(std::list<struct site> &index, std::string path);
void load_index(std::list<struct site> &index, std::string path);

struct site * index_find_host(
        std::list<struct site> &index,
        std::string host);

};


