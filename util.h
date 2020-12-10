#include <vector>
#include <set>
#include <map>
#include <string>
#include <iostream>
#include <fstream>

namespace util {

#define max_url_len 512

std::string get_host(std::string url);
std::string get_path(std::string url);
std::pair<std::string, std::string> split_dir(std::string path);

std::string normalize_path(std::string s);

std::string make_path(std::string host, std::string url);

bool bare_minimum_valid_url(std::string url);

std::string simplify_url(std::string url);

void load_index(std::string host,
      std::map<std::string, std::string> &urls);

void save_index(std::string host,
      std::map<std::string, std::string> &urls);

void load_other(std::string host,
      std::set<std::string> &urls);

void save_other(std::string host,
      std::set<std::string> &urls);
};
