#include <libxml/HTMLparser.h>
#include <libxml/xpath.h>
#include <libxml/uri.h>
#include <curl/curl.h>
#include <string.h>
#include <math.h>
#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include <sys/stat.h>
#include <sys/types.h>

#include "util.h"

#define max_url_len 512

/* resizable buffer */ 
typedef struct {
  char *buf;
  size_t size;
} memory;

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

    if (!link) continue;

    if (!strncmp(link, "http://", 7) || !strncmp(link, "https://", 8)) {
      
      std::string s(link);

      if (util::bare_minimum_valid_url(s)) {
        urls.push_back(util::simplify_url(s));
      }
    }

    xmlFree(link);
  }

  xmlXPathFreeObject(result);

  return urls;
}

int is_html(char *ctype)
{
  return ctype != NULL && strlen(ctype) >= 9 && strstr(ctype, "text/html");
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
  
  auto d = util::split_dir(path);
  mkdir_tree(d.first);

  file.open(path, std::ios::out | std::ios::binary | std::ios::trunc);
  
  if (!file.is_open()) {
    fprintf(stderr, "error opening file %s\n", path.c_str());
    return;
  }

  file.write(mem->buf, mem->size);

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
    std::string url_host = util::get_host(url);

    if (url_host == host) {
      auto a = url_scanned.find(url);
      if (a != url_scanned.end()) {
        continue;
      }

      auto b = url_bad.find(url);
      if (b != url_bad.end()) {
        continue;
      }

      bool found = false;

      for (auto &u: url_scanning) {
        if (url == u) {
          found = true;
          break;
        }
      }

      if (found) {
        continue;
      }

      url_scanning.push_back(url);

    } else {
      url_other.insert(url);
    } 
  }
}

std::string pick_next(std::vector<std::string> &urls) {
  auto best = urls.begin();

  for (auto u = urls.begin(); u != urls.end(); u++) {
    if ((*u).length() < (*best).length()) {
      best = u;
    }
  }

  std::string r(*best);
  urls.erase(best);
  return r;
}

void
scrape(int max_pages, 
    std::string host, 
    std::vector<std::string> url_scanning,
    std::map<std::string, std::string> &url_index,
    std::set<std::string> &url_other)
{
  printf("scraping %s for up to %i pages\n", host.c_str(), max_pages);

  std::set<std::string> url_bad;
  url_index.clear();
  url_other.clear();

  CURL *curl_handle;
  CURLcode res;

  curl_global_init(CURL_GLOBAL_DEFAULT);
 
  while (!url_scanning.empty() && url_index.size() < max_pages) {
    auto url = pick_next(url_scanning);

    printf("scan %s\n", url.c_str());
   
    auto path = util::make_path(host, url);

    bool found = false;
    for (auto &u : url_index) {
      if (u.second == path) {
        printf("dup path %s -> %s matches\n", u.first.c_str(), u.second.c_str());
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
    curl_easy_setopt(curl_handle, CURLOPT_FOLLOWLOCATION, 1);
    curl_easy_setopt(curl_handle, CURLOPT_TIMEOUT, 10);

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
        printf("HTTP 200 (%s) : %lu : %s\n", ctype, mem.size, url.c_str());

        if (is_html(ctype) && mem.size > 100) {
          auto urls = find_links(&mem, url);

          printf("save to %s\n", path.c_str());

          save_file(path, &mem);
          
          url_index.insert(std::pair<std::string, std::string>(url, path));
        
          insert_urls(host, urls, url_index, url_bad, url_other, url_scanning);

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
}

