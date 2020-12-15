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

#include <list>
#include <set>
#include <map>
#include <string>

#include "util.h"
#include "scrape.h"

/* resizable buffer */ 
typedef struct {
  char *buf;
  size_t size, cur;
} memory;

size_t grow_buffer(void *contents, size_t sz, size_t nmemb, void *ctx)
{
  memory *mem = (memory*) ctx;
  size_t realsize = sz * nmemb;

  if (mem->size < mem->cur + realsize) {
    printf("file too big\n");

    return 0;
  }

  memcpy(&(mem->buf[mem->cur]), contents, realsize);
  mem->cur += realsize;

  return realsize;
}

bool has_suffix(std::string const &s, std::string const &suffix) {
  if (s.length() >= suffix.length()) {
    return (0 == s.compare(s.length() - suffix.length(), suffix.length(), suffix));
  } else {
    return false;
  }
}

bool want_url(std::string url) {
  if (strncmp(url.c_str(), "http://", 7) && strncmp(url.c_str(), "https://", 8)) 
    return false;

  if (!util::bare_minimum_valid_url(url)) return false;

  if (has_suffix(url, ".txt") ||
      has_suffix(url, ".jpg") ||
      has_suffix(url, ".png") ||
      has_suffix(url, ".gif") ||
      has_suffix(url, ".svg") ||
      has_suffix(url, ".mov") ||
      has_suffix(url, ".mp3") ||
      has_suffix(url, ".flac") ||
      has_suffix(url, ".ogg") ||
      has_suffix(url, ".epub") ||
      has_suffix(url, ".tar") ||
      has_suffix(url, ".rar") ||
      has_suffix(url, ".zip") ||
      has_suffix(url, ".gz") ||
      has_suffix(url, ".xz") ||
      has_suffix(url, ".bz2") ||
      has_suffix(url, ".crate") ||
      has_suffix(url, ".xml") ||
      has_suffix(url, ".csv") ||
      has_suffix(url, ".ppt") ||
      has_suffix(url, ".sheet") ||
      has_suffix(url, ".sh") ||
      has_suffix(url, ".py") ||
      has_suffix(url, ".js") ||
      has_suffix(url, ".asc") ||
      has_suffix(url, ".pdf")) 
    return false;

  return true;
}

std::list<std::string> find_links(memory *mem, std::string url)
{
  std::list<std::string> urls;

  char url_w[util::max_url_len];
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
    // TODO: what does this actually do?
    double r = rand();
    int x = r * nodeset->nodeNr / RAND_MAX;

    const xmlNode *node = nodeset->nodeTab[x]->xmlChildrenNode;

    xmlChar *orig = xmlNodeListGetString(doc, node, 1);

    xmlChar *href = xmlBuildURI(orig, (const xmlChar *) url_w);
    xmlFree(orig);

    char *link = (char *) href;

    if (!link) continue;

    std::string url(link);

    if (want_url(url)) {
      urls.push_back(util::simplify_url(url));
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

void save_file(std::string path, std::string url, memory *mem)
{
  std::ofstream file;
 
  file.open(path, std::ios::out | std::ios::binary | std::ios::trunc);
  
  if (!file.is_open()) {
    fprintf(stderr, "error opening file %s for %s\n", path.c_str(), url.c_str());
    return;
  }

  file.write(mem->buf, mem->size);

  file.close();
}

bool index_check_mark(
    std::list<struct index_url> &url_index,
    std::string url)
{
  for (auto &i: url_index) {
    if (i.url == url) {
      i.count++;
      return true;
    }
  }

  return false;
}

bool index_check_path(
    std::list<struct index_url> &url_index,
    std::string path)
{
  for (auto &i: url_index) {
    if (i.path == path) {
      return true;
    }
  }

  return false;
}

bool other_check_mark(
    std::list<struct other_url> &url_other,
    std::string url)
{
  for (auto &i: url_other) {
    if (i.url == url) {
      i.count++;
      return true;
    }
  }

  return false;
}

void insert_urls(std::string host,
      std::list<std::string> urls,
      std::list<struct index_url> &url_index,
      std::list<struct other_url> &url_other,
      std::set<std::string> &url_bad,
      std::list<struct index_url> &url_scanning)
{
  for (auto &url: urls) {

    std::string url_host = util::get_host(url);

    if (url_host.empty()) continue;

    if (url_host == host) {
      auto b = url_bad.find(url);
      if (b != url_bad.end()) {
        continue;
      }

      if (index_check_mark(url_index, url)) {
        continue;
      }

      if (index_check_mark(url_scanning, url)) {
        continue;
      }

      auto p = util::make_path(host, url);

      if (index_check_path(url_scanning, p)) {
        continue;
      }

      if (index_check_path(url_index, p)) {
        continue;
      }
   
      struct index_url i = {1, url, p};
      url_scanning.push_back(i);

    } else {
      if (other_check_mark(url_other, url)) {
        continue;
      }

      struct other_url i = {1, url};
      url_other.push_back(i);
    } 
  }
}

struct index_url pick_next(std::list<struct index_url> &urls) {
  auto best = urls.begin();

  for (auto u = urls.begin(); u != urls.end(); u++) {
    // TODO: base on count too
    if ((*u).url.length() < (*best).url.length()) {
      best = u;
    }
  }

  struct index_url r(*best);
  urls.erase(best);
  return r;
}

void
scrape(int max_pages, 
    const std::string host, 
    const std::vector<std::string> &url_seed,
    std::list<struct index_url> &url_index,
    std::list<struct other_url> &url_other)
{
  printf("scraping %s for up to %i pages\n", host.c_str(), max_pages);

  std::list<struct index_url> url_scanning;
  std::set<std::string> url_bad;

  for (auto &u: url_seed) {
    auto p = util::make_path(host, u);

    if (index_check_path(url_scanning, p)) {
      printf("skip dup path %s\n", u.c_str());
      continue;
    }

    struct index_url i = {0, u, p};

    url_scanning.push_back(i);
  }

  CURL *curl_handle;
  CURLcode res;

  int fail_net = 0;
  int fail_web = 0;
  
  // TODO: have a memory leak somewhere
  size_t size = 1024 * 1024;
  char *c = (char *) malloc(size);
  memory mem{c, size, 0};
 
  while (!url_scanning.empty()) {
    if (url_index.size() >= max_pages) {
      printf("%s reached max pages\n", host.c_str());
      break;
    }
    
    if (fail_net > 1 + url_index.size() / 4) {
      printf("%s reached max fail net %i / %lu\n", host.c_str(),
          fail_net, url_index.size());
      break;
    }

    if (fail_web > 1 + url_index.size() / 2) {
      printf("%s reached max fail web %i / %lu\n", host.c_str(),
          fail_web, url_index.size());
      break;
    }
 
    auto u = pick_next(url_scanning);

    auto path = u.path;
    auto url = u.url;

    mem.cur = 0;

    curl_handle = curl_easy_init();

    curl_easy_setopt(curl_handle, CURLOPT_WRITEFUNCTION, grow_buffer);
    curl_easy_setopt(curl_handle, CURLOPT_WRITEDATA, &mem);
    curl_easy_setopt(curl_handle, CURLOPT_USERAGENT, "libcurl-agent/1.0");
    curl_easy_setopt(curl_handle, CURLOPT_FOLLOWLOCATION, 1);
    curl_easy_setopt(curl_handle, CURLOPT_TIMEOUT, 10);

    char s[util::max_url_len];
    strcpy(s, url.c_str());
    curl_easy_setopt(curl_handle, CURLOPT_URL, s);

    res = curl_easy_perform(curl_handle);
    if (res == CURLE_OK) {
      long res_status;
      curl_easy_getinfo(curl_handle, CURLINFO_RESPONSE_CODE, &res_status);

      if (res_status == 200) {
        char *ctype;
        curl_easy_getinfo(curl_handle, CURLINFO_CONTENT_TYPE, &ctype);

        if (is_html(ctype) && mem.size > 10) {
          auto urls = find_links(&mem, url);

          save_file(path, url, &mem);

          if (url_index.size() > 0 && url_index.size() % 10 == 0) {
            printf("%s %lu / %lu with %lu failures\n",
                host.c_str(), url_index.size(), url_scanning.size(),
                url_bad.size());
          }
          
          url_index.push_back(u);
        
          insert_urls(host, urls, url_index, url_other, url_bad, url_scanning);

        } else {
          printf("miss '%s' %lu %s\n", ctype, mem.size, url.c_str());
          url_bad.insert(url);
        }

      } else {
        printf("miss %d %s\n", (int) res_status, url.c_str());
        url_bad.insert(url);
        fail_web++;
      }

    } else {
      printf("miss %s %s\n", curl_easy_strerror(res), url.c_str());
      url_bad.insert(url);
      fail_net++;
    }
   
    curl_easy_cleanup(curl_handle);
  }
  
  free(mem.buf);
}

