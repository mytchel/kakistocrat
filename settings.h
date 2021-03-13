#ifndef SETTINGS_H
#define SETTINGS_H

#include <nlohmann/json.hpp>

namespace settings {

struct settings {
  size_t max_files;
  size_t files_per_dir;

  std::string data_dir;
  std::string meta_dir;

  std::string seed;
  std::string blacklist;
};

void from_json(const nlohmann::json &j, settings &s);

}
#endif

