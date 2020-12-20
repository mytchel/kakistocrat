namespace util {

const size_t max_url_len = 512;

std::string get_proto(std::string url);
std::string get_host(std::string url);
std::string get_path(std::string url);
std::string get_dir(std::string path);

std::string make_path(std::string url);

bool bare_minimum_valid_url(std::string url);

std::vector<std::string> load_list(std::string path);

};

