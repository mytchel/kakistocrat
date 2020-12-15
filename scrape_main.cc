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

#include "scrape.h"

int
main(int argc, char *argv[])
{
  int max_pages = 100;
  int c;

  while ((c = getopt(argc, argv, "n:")) != -1) {
    switch (c) {
      case 'n':
        max_pages = strtol(optarg, NULL, 10);
        break;
      case '?':
        if (optopt == 'n') 
          fprintf(stderr, "option -n requieres an argument\n");
        else if (isprint(optopt))
          fprintf(stderr, "unknown option -%c\n", optopt);
        else
          fprintf(stderr, "unknown option\n");
        return EXIT_FAILURE;
      default:
        return EXIT_FAILURE;
    }
  }
 
  if (optind == argc) {
    fprintf(stderr, "need url\n");
    return EXIT_FAILURE;
  }

  if (strlen(argv[optind]) >= util::max_url_len) {
    fprintf(stderr, "url too long\n");
    return EXIT_FAILURE;
  }

  std::string host(argv[optind]);
 
  printf("scraping %s for up to %i pages\n", host.c_str(), max_pages);
  
  curl_global_init(CURL_GLOBAL_DEFAULT);

  std::vector<struct index_url> url_index;
  std::vector<struct other_url> url_other;

  std::vector<std::string> url_scanning;

  for (int i = optind + 1; i < argc; i++) {
    url_scanning.push_back(std::string(argv[i]));
  }

  scrape(max_pages, host, url_scanning, url_index, url_other);

  // TODO
  //util::save_index(host, url_index);
  //util::save_other(host, url_other);

  curl_global_cleanup();

  return EXIT_SUCCESS;
}

