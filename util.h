namespace util {

const size_t max_url_len = 256;

bool has_suffix(std::string const &s, std::string const &suffix);

bool has_prefix(std::string const &s, std::string const &prefix);

std::string get_proto(const std::string &url);
std::string get_host(const std::string &url);
std::string get_path(const std::string &url);
std::string get_dir(const std::string &path);

std::string make_path(const std::string &url);

bool bare_minimum_valid_url(const std::string &url);

std::vector<std::string> load_list(std::string path);

};

