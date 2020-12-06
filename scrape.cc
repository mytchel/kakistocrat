#include <libxml/HTMLparser.h>
#include <libxml/xpath.h>
#include <libxml/uri.h>
#include <curl/curl.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>

#include <sys/stat.h>
#include <sys/types.h>

#include <vector>
#include <set>
#include <map>
#include <string>
#include <iostream>
#include <fstream>

#define max_url_len 512

/* resizable buffer */ 
typedef struct {
  char *buf;
  size_t size;
} memory;

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
      n += "_args";
      break;

    } else if (c == '&' || c == '*' || c == '!' || c == '@' || c == '#' || c == '$' || c == '%' || c == '^') {
      n += "_junk";
      break;
    
    } else if (c == '\t' || c == '\n') {
      n += "_naughty";
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


size_t grow_buffer(void *contents, size_t sz, size_t nmemb, void *ctx)
{
  size_t realsize = sz * nmemb;
  memory *mem = (memory*) ctx;
  char *ptr = (char *) realloc(mem->buf, mem->size + realsize);
  if(!ptr) {
    /* out of memory */ 
    printf("not enough memory (realloc returned NULL)\n");
    return 0;
  }
  mem->buf = ptr;
  memcpy(&(mem->buf[mem->size]), contents, realsize);
  mem->size += realsize;
  return realsize;
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

std::vector<std::string> find_links(memory *mem, std::string url)
{
  std::vector<std::string> urls;

  char url_w[max_url_len];
  strcpy(url_w, url.c_str());

  int opts = HTML_PARSE_NOBLANKS | HTML_PARSE_NOERROR |  
             HTML_PARSE_NOWARNING | HTML_PARSE_NONET;

  htmlDocPtr doc = htmlReadMemory(mem->buf, mem->size, url.c_str(), NULL, opts);
  if (!doc) {
    printf("read mem failed\n");
    return urls;
  }

  xmlChar *xpath = (xmlChar*) "//a/@href";
  xmlXPathContextPtr context = xmlXPathNewContext(doc);
  xmlXPathObjectPtr result = xmlXPathEvalExpression(xpath, context);
  xmlXPathFreeContext(context);

  if (!result) {
    printf("xml parse failed\n");
    return urls;
  }

  xmlNodeSetPtr nodeset = result->nodesetval;

  if (xmlXPathNodeSetIsEmpty(nodeset)) {
    xmlXPathFreeObject(result);
    return urls;
  }

  int i;
  for (i = 0; i < nodeset->nodeNr; i++) {
    double r = rand();
    int x = r * nodeset->nodeNr / RAND_MAX;

    const xmlNode *node = nodeset->nodeTab[x]->xmlChildrenNode;

    xmlChar *orig = xmlNodeListGetString(doc, node, 1);

    xmlChar *href = xmlBuildURI(orig, (const xmlChar *) url_w);
    xmlFree(orig);

    char *link = (char *) href;

    if (!link || strlen(link) < 20)
      continue;

    if (!strncmp(link, "http://", 7) || !strncmp(link, "https://", 8)) {
      std::string s(link);

      if (bare_minimum_valid_url(s)) {
        urls.push_back(s);
      }
    }

    xmlFree(link);
  }

  xmlXPathFreeObject(result);

  return urls;
}

int is_html(char *ctype)
{
  return ctype != NULL && strlen(ctype) > 10 && strstr(ctype, "text/html");
}

void mkdir_tree(std::string sub, std::string dir) {
  if (sub.length() == 0) return;

  int i;

  for (i = 0; i < sub.length(); i++) {
    dir += sub[i];
    if (sub[i] == '/') break;
  }

  mkdir(dir.c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);

  if (i + 1 < sub.length()) {
    mkdir_tree(sub.substr(i+1), dir);
  }
}

void mkdir_tree(std::string path) {
  mkdir_tree(path, "");
}

void save_file(std::string path, memory *mem)
{
  std::ofstream file;
  
  auto d = split_dir(path);
  mkdir_tree(d.first);

  file.open(path, std::ios::out | std::ios::binary | std::ios::trunc);
  
  if (!file.is_open()) {
    fprintf(stderr, "error opening file %s\n", path.c_str());
    return;
  }

  file.write(mem->buf, mem->size);

  file.close();
}

void save_index(std::string path,
      std::map<std::string, std::string> &url_scanned)
{
  std::ofstream file;

  printf("save index %lu -> %s\n", url_scanned.size(), path.c_str());

  file.open(path, std::ios::out | std::ios::trunc);
  
  if (!file.is_open()) {
    fprintf(stderr, "error opening file %s\n", path.c_str());
    return;
  }

  for (auto &u : url_scanned) {
    file << u.first << "\t" << u.second << "\n";
  }

  file.close();
}

void save_other(std::string path,
      std::set<std::string> &url_other)
{
  std::ofstream file;

  printf("save other %lu -> %s\n", url_other.size(), path.c_str());

  file.open(path, std::ios::out | std::ios::trunc);
  
  if (!file.is_open()) {
    fprintf(stderr, "error opening file %s\n", path.c_str());
    return;
  }

  for (auto &u : url_other) {
    file << u << "\n";
  }

  file.close();
}

void insert_urls(std::string host,
      std::vector<std::string> urls,
      std::map<std::string, std::string> &url_scanned,
      std::set<std::string> &url_bad,
      std::set<std::string> &url_other,
      std::vector<std::string> &url_scanning)
{
  for (auto &url: urls) {
    std::string url_host = get_host(url);

    if (url_host == host) {
      auto a = url_scanned.find(url);
      if (a != url_scanned.end()) continue;

      auto b = url_bad.find(url);
      if (b != url_bad.end()) continue;

      bool found = false;

      for (auto &u: url_scanning) {
        if (url == u) {
          found = true;
          break;
        }
      }

      if (found) continue;

      url_scanning.push_back(url);

    } else {
      url_other.insert(url);
    } 
  }
}

std::string pick_next(std::vector<std::string> &url_scanning) {
  auto best = url_scanning.begin();
  for (auto u = url_scanning.begin(); u != url_scanning.end(); u++) {
    if ((*u).length() < (*best).length()) {
      best = u;
    }
  }

  std::string r(*best);
  url_scanning.erase(best);
  return r;
}

int
main(int argc, const char *argv[])
{
  if (argc < 2) {
    printf("url?");
    return EXIT_FAILURE;
  }

  std::string start_page(argv[1]);
  if (start_page.length() >= max_url_len) {
    fprintf(stderr, "url too long\n");
    return EXIT_FAILURE;
  }

  int max_pages = 100;

  if (argc == 3) {
    max_pages = strtol(argv[2], NULL, 10);
  }

  std::string host = get_host(start_page);

  printf("scraping %s for up to %i pages\n", host.c_str(), max_pages);
  printf("starting with %s\n", start_page.c_str());

  std::map<std::string, std::string> url_scanned;
  std::set<std::string> url_bad;
  std::set<std::string> url_other;
  std::vector<std::string> url_scanning;

  CURL *curl_handle;
  CURLcode res;

  url_scanning.push_back(start_page);

  curl_global_init(CURL_GLOBAL_DEFAULT);
 
  while (!url_scanning.empty() && url_scanned.size() < max_pages) {
    auto url = pick_next(url_scanning);
   
    auto path = make_path(host, url);

    bool found = false;
    for (auto &u : url_scanned) {
      if (u.second == path) {
        found = true;
        break;
      }
    }

    if (found) {
      printf("skip dup path %s\n", url.c_str());
      continue;
    }

    char *c = (char *) malloc(1);
    memory mem{c, 0};

    curl_handle = curl_easy_init();

    curl_easy_setopt(curl_handle, CURLOPT_WRITEFUNCTION, grow_buffer);
    curl_easy_setopt(curl_handle, CURLOPT_WRITEDATA, &mem);
    curl_easy_setopt(curl_handle, CURLOPT_USERAGENT, "libcurl-agent/1.0");

    char s[max_url_len];
    strcpy(s, url.c_str());
    curl_easy_setopt(curl_handle, CURLOPT_URL, s);

    res = curl_easy_perform(curl_handle);
    if (res == CURLE_OK) {
      long res_status;
      curl_easy_getinfo(curl_handle, CURLINFO_RESPONSE_CODE, &res_status);

      if (res_status == 200) {
        char *ctype;
        curl_easy_getinfo(curl_handle, CURLINFO_CONTENT_TYPE, &ctype);
        printf("HTTP 200 (%s): %s\n", ctype, url.c_str());

        if (is_html(ctype) && mem.size > 100) {
          auto urls = find_links(&mem, url);

          insert_urls(host, urls, url_scanned, url_bad, url_other, url_scanning);

          printf("save to %s\n", path.c_str());

          save_file(path, &mem);
          
          url_scanned.insert(std::pair<std::string, std::string>(url, path));
        
        } else {
          printf("skip non html %s\n", url.c_str());
          url_bad.insert(url);
        }

      } else {
        printf("miss http %d %s\n", (int) res_status, url.c_str());
        url_bad.insert(url);
      }

    } else {
      fprintf(stderr, "curl_easy_perform() failed: %s\n",
              curl_easy_strerror(res));
      url_bad.insert(url);
    }
   
    curl_easy_cleanup(curl_handle);

    free(mem.buf);
  }

  curl_global_cleanup();

  save_index(host + "_index", url_scanned);
  save_other(host + "_other", url_other);

  return EXIT_SUCCESS;
}

