#ifndef UTIL_H
#define UTIL_H

namespace util {

const size_t max_url_len = 256;

bool has_suffix(const std::string &s, const std::string &suffix);

bool has_prefix(const std::string &s, const std::string &prefix);

std::string get_proto(const std::string &url);
std::string get_host(const std::string &url);
std::string get_path(const std::string &url);
std::string get_dir(const std::string &path);

std::vector<std::string> split_path(std::string s);

void make_path(const std::string &path);

bool bare_minimum_valid_url(const std::string &url);

std::vector<std::string> load_list(const std::string &path);

std::string url_decode(const std::string &in);
};

#endif
