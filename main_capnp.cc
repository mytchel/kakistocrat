#include <stdio.h>
#include <unistd.h>

#include <cstdlib>
#include <cstring>

#include <vector>
#include <list>
#include <set>
#include <map>
#include <string>
#include <algorithm>
#include <thread>
#include <future>
#include <optional>
#include <iostream>
#include <fstream>
#include <sstream>
#include <cstdint>

#include <nlohmann/json.hpp>
#include "spdlog/spdlog.h"

#include "util.h"
#include "crawler.h"
#include "index.h"
#include "index_manager.h"
#include "capnp_server.h"

#include "indexer.capnp.h"

#include <kj/debug.h>
#include <kj/async-io.h>
#include <kj/async-unix.h>
#include <kj/timer.h>
#include <kj/threadlocal.h>
#include <capnp/ez-rpc.h>
#include <capnp/rpc-twoparty.h>
#include <capnp/capability.h>
#include <capnp/rpc.h>
#include <capnp/message.h>
#include <iostream>

using nlohmann::json;

class MasterImpl final: public Master::Server,
                        public kj::TaskSet::ErrorHandler {

 public:
  MasterImpl(kj::AsyncIoContext &io_context, const config &s)
    : settings(s), tasks(*this),
      timer(io_context.provider->getTimer()),
      indexer(s.index_meta_path, s.indexer.sites_per_part, s.index_parts, s.merger.meta_path),
      crawler(s)
  {
    tasks.add(timer.afterDelay(1 * kj::SECONDS).then(
      [this] () {
        setup();
      }));
  }

  void setup() {
    crawler.load();

    std::vector<std::string> blacklist = util::load_list(settings.blacklist_path);
    std::vector<std::string> initial_seed = util::load_list(settings.seed_path);

    crawler.load_blacklist(blacklist);
    crawler.load_seed(initial_seed);

    //indexer.load();

    for (auto &site: crawler.sites) {
      indexer.mark_indexable(site.path);
    }

    flush();
    checkCrawlers();

    indexNext();
    mergeNext();
  }

  void checkCrawlers() {
    for (auto &c: crawlers) {
      auto request = c.canCrawlRequest();

      auto it = std::find(ready_crawlers.begin(), ready_crawlers.end(), &c);
      if (it != ready_crawlers.end()) {
        continue;
      }

      tasks.add(request.send().then(
        [this, &c] (auto result) {

          bool can_crawl = result.getHaveSpace();

          if (can_crawl) {
            auto it = std::find(ready_crawlers.begin(), ready_crawlers.end(), &c);
            if (it == ready_crawlers.end()) {
              ready_crawlers.push_back(&c);
              crawlNext();
            }
          }
        },
        [this, &c] (auto exception) {
          spdlog::warn("crawler {} error checking can crawl, removing", (uintptr_t) &c);

          ready_crawlers.remove(&c);
/*
          crawlers.remove_if(
              [&c] (auto cc) {
                return &cc == &c;
              });
              */
        }));
    }

    tasks.add(timer.afterDelay(1 * kj::SECONDS).then(
          [this] () {
            checkCrawlers();
          }));
  }

  void flush() {
    //search::save_parts(settings.indexer.meta_path, index_parts);

    indexer.save();

    if (have_changes) {
      spdlog::info("periodic save: started");

      spdlog::info("periodic save: flush sites");
      for (auto &s: crawler.sites) {
        s.flush();
      }

      spdlog::info("periodic save: save metadata");
      crawler.save();

      spdlog::info("periodic save: finished");

      have_changes = false;
    }

    tasks.add(timer.afterDelay(60 * kj::SECONDS).then(
          [this] () {
            flush();
          }));
  }

  void crawlNext() {
    if (ready_crawlers.empty()) {
      return;
    }

    auto site = crawler.get_next_site();
    if (site == nullptr) {
      spdlog::info("no sites to crawl");
      return;
    }

    site->flush();

    auto proc = ready_crawlers.front();
    ready_crawlers.pop_front();

    auto request = proc->crawlRequest();

    request.setSitePath(site->path);
    request.setDataPath(crawler.get_data_path(site->host));

    request.setMaxPages(site->max_pages);

    /*
    request.setMaxConnections(settings.crawler.site_max_connections);
    request.setMaxPageSize(settings.crawler.max_page_size);
    request.setMaxPartSize(settings.crawler.max_site_part_size);
    */

    site->scraping = true;

    spdlog::info("start crawling {}", site->host);

    tasks.add(request.send().then(
        [this, site, proc] (auto result) {
          spdlog::info("got response for crawl site {}", site->host);

          site->reload();

          size_t max_add_sites = 0;
          size_t next_max_pages = 0;

          if (site->level + 1 < crawler.levels.size()) {
            auto level = crawler.levels[site->level];
            auto next_level = crawler.levels[site->level + 1];

            max_add_sites = level.max_add_sites;
            next_max_pages = next_level.max_pages;
          }

          crawler.expand(site, max_add_sites, next_max_pages);

          site->last_scanned = time(NULL);

          size_t page_count = site->pages.size();
          spdlog::debug("saving scraped site with {} pages", page_count);

          // Need to write the changes for this site
          // so the indexer has something to load.
          site->flush();

          site->max_pages = 0;
          site->scraped = true;
          site->scraping = false;

          have_changes = true;

          indexer.mark_indexable(site->path);

          auto it = std::find(ready_crawlers.begin(), ready_crawlers.end(), proc);
          if (it == ready_crawlers.end()) {
            ready_crawlers.push_back(proc);
          }

          crawlNext();
        },
        [this, site, proc] (auto exception) {
          spdlog::warn("got exception for crawl site {} : {}",
              site->host, std::string(exception.getDescription()));

          site->scraping = false;

          crawlNext();
        }));

    crawlNext();
  }

  void mergeNext() {
    if (!indexer.need_merge_part()) {
      return;
    }

    if (ready_mergers.empty()) {
      return;
    }

    auto &p = indexer.get_merge_part();

    auto merger = ready_mergers.front();
    ready_mergers.pop_front();

    spdlog::info("start merging part {} {}",
            search::to_str(p.type), p.start);

    auto request = merger->mergeRequest();

    request.setType(search::to_str(p.type));

    request.setStart(p.start);

    if (p.end) {
      request.setEnd(*p.end);
    }

    auto paths = request.initIndexPartPaths(p.index_parts->size());

    size_t i = 0;
    for (auto &path: *p.index_parts) {
      paths.set(i++, path);
    }

    util::make_path(settings.merger.parts_path);

    auto out = fmt::format("{}/index.{}.{}.dat",
      settings.merger.parts_path, search::to_str(p.type), p.start);

    request.setOut(out);

    tasks.add(request.send().then(
        [this, &p, merger, out] (auto result) mutable {
          spdlog::info("finished merging part {} {}",
            search::to_str(p.type), p.start);

          indexer.merge_part_done(p, out, true);

          ready_mergers.push_back(merger);

          mergeNext();
        },
        [this, &p] (auto exception) mutable {
          spdlog::warn("got exception for merge part {} {} : {}",
              search::to_str(p.type), p.start,
              std::string(exception.getDescription()));

          indexer.merge_part_done(p, "", false);

          // Put merger on ready?

          mergeNext();
        }));

    mergeNext();
  }

  void indexNext() {
    spdlog::info("index next");

    if (!indexer.need_index() && !indexer.index_active()) {
      spdlog::info("nothing to index");

      if (indexer.need_start_merge()) {
        spdlog::info("start merge");
        indexer.start_merge();
        mergeNext();
      }

      tasks.add(timer.afterDelay(30 * kj::SECONDS).then(
          [this] () {
            indexNext();
          }));

      return;
    }

    if (ready_indexers.empty()) {
      spdlog::info("no ready indexers");
      return;
    }

    bool flush = true;//time(NULL) > last_index + settings.merger.frequency_minutes * 60;

    auto sites = indexer.get_sites_for_index(flush);
    if (sites.empty()) {
      return;
    }

    last_index = time(NULL);

    auto client = ready_indexers.front();
    ready_indexers.pop_front();

    spdlog::info("request index for {} sites", sites.size());

    auto &op = index_ops.emplace_back(sites);

    auto request = client->indexRequest();

    std::string output = fmt::format("{}/{}",
      settings.indexer.parts_path,
      indexer.get_next_part_id());

    auto paths = request.initSitePaths(op.size());
    size_t i = 0;
    for (auto &s: op) {
      paths.set(i++, s);
    }

    request.setOutputBase(output);

    tasks.add(request.send().then(
        [this, &op, client] (auto result) {
          std::list<std::string> parts;

          for (auto p: result.getOutputs()) {
            std::string path = p.getPath();

            std::vector<std::string> sites;

            for (auto s_path_reader: p.getSites()) {
              sites.emplace_back(s_path_reader);
            }

            spdlog::info("index new part {} with {} sites", path, sites.size());
            indexer.add_part(path, sites);
          }

          index_ops.remove_if(
              [&op] (auto &o) {
                return &op == &o;
              });

          ready_indexers.push_back(client);
          indexNext();
        },
        [this, &op, client] (auto exception) {
          spdlog::warn("got exception while indexing {}", std::string(exception.getDescription()));

          for (auto &s: op) {
            spdlog::info("index finished for {}", s);
            indexer.mark_indexable(s);
          }

          index_ops.remove_if(
              [&op] (auto &o) {
                return &op == &o;
              });
        }));

    indexNext();
  }

  kj::Promise<void> registerCrawler(RegisterCrawlerContext context) override {
    spdlog::debug("got register crawler");

    crawlers.push_back(context.getParams().getCrawler());

    ready_crawlers.push_back(&crawlers.back());

    crawlNext();

    return kj::READY_NOW;
  }

  kj::Promise<void> registerIndexer(RegisterIndexerContext context) override {
    spdlog::debug("got register indexer");

    indexers.push_back(context.getParams().getIndexer());

    ready_indexers.push_back(&indexers.back());

    indexNext();

    return kj::READY_NOW;
  }

  kj::Promise<void> registerMerger(RegisterMergerContext context) override {
    spdlog::debug("got register merger");

    mergers.push_back(context.getParams().getMerger());
    ready_mergers.push_back(&mergers.back());

    mergeNext();

    return kj::READY_NOW;
  }

  kj::Promise<void> registerScorer(RegisterScorerContext context) override {
    spdlog::debug("got register scorer");

    std::vector<std::string> initial_seed = util::load_list(settings.seed_path);

    scorers.clear();
    scorers.push_back(context.getParams().getScorer());
    auto scorer = scorers.back();

    auto request = scorer.scoreRequest();

    std::vector<crawl::site *> sites;

    for (auto &s: crawler.sites) {
      //if (s.merged) {
      if (s.scraped) {
        sites.push_back(&s);
      }
    }

    auto paths = request.initSitePaths(sites.size());

    for (size_t i = 0; i < sites.size(); i++) {
      paths.set(i, sites[i]->path);
    }

    auto seed = request.initSeed(initial_seed.size());

    for (size_t i = 0; i < initial_seed.size(); i++) {
      seed.set(i, initial_seed[i]);
    }

    tasks.add(request.send().then(
          [] (auto result) {
            spdlog::info("scoring finished");
          },
          [] (auto exception) {
            spdlog::warn("scoring failed");
          }));

    return kj::READY_NOW;
  }

  kj::Promise<void> registerSearcher(RegisterSearcherContext context) override {
    spdlog::debug("got register searcher");

    searchers.push_back(context.getParams().getSearcher());

    return kj::READY_NOW;
  }

  kj::Promise<float> getScore(const std::string &url) {
    if (scorers.empty()) {
      return kj::Promise<float>(0.0);
    }

    auto scorer = scorers.back();
    auto request = scorer.getScoreRequest();

    request.setUrl(url);

    return request.send().then(
        [this] (auto result)  {
          float score = result.getScore();
          return score;
        },
        [this] (auto exception) {
          spdlog::warn("get score failed: {}", std::string(exception.getDescription()));
          return 0;
        });
  }

  kj::Promise<void> getScore(GetScoreContext context) override {
    spdlog::debug("get score");

    auto url = context.getParams().getUrl();

    return getScore(url).then(
        [this, KJ_CPCAP(context)] (auto score) mutable {
          context.getResults().setScore(score);
        },
        [this] (auto exception) {
          spdlog::warn("get score failed: {}", std::string(exception.getDescription()));
        });
  }

  kj::Promise<void> getPageInfo(GetPageInfoContext context) override {
    spdlog::debug("get page info");

    std::string url = context.getParams().getUrl();

    auto host = util::get_host(url);

    auto site = crawler.find_site(host);
    if (site == nullptr) {
      spdlog::info("unknown site {}", host);
      return kj::READY_NOW;
    }

    auto page = site->find_page(url);
    if (page == nullptr) {
      spdlog::info("have site {} but not page {}", host, url);
      return kj::READY_NOW;
    }

    if (page->last_scanned == 0) {
      spdlog::info("page not scraped {} : {} {} {} {}", url, page->last_scanned, page->url, page->title, page->path);
      return kj::READY_NOW;
    }

    spdlog::info("have page {} : {} {} {} {}", url, page->last_scanned, page->url, page->title, page->path);

    auto results = context.getResults();

    results.setTitle(page->title);
    results.setPath(page->path);

    return getScore(url).then(
        [this, url, KJ_CPCAP(context), KJ_CPCAP(results)] (auto score) mutable {
          spdlog::info("got score for page {} {}", score, url);
          results.setScore(score);
        },
        [this] (auto exception) {
          spdlog::warn("get score failed: {}", std::string(exception.getDescription()));
        });
  }

  void taskFailed(kj::Exception&& exception) override {
    spdlog::warn("task failed: {}", std::string(exception.getDescription()));
    kj::throwFatalException(kj::mv(exception));
  }

  const config &settings;

  bool have_changes{false};

  kj::TaskSet tasks;

  kj::Timer &timer;

  // Crawling

  crawl::crawler crawler;

  std::list<Crawler::Client> crawlers;
  std::list<Crawler::Client *> ready_crawlers;

  // Indexing

  index_manager indexer;

  std::list<std::vector<std::string>> index_ops;

  std::list<Indexer::Client> indexers;
  std::list<Indexer::Client *> ready_indexers;

  // Merging

  std::list<Merger::Client> mergers;
  std::list<Merger::Client *> ready_mergers;

  time_t last_merge{0};
  time_t last_index{0};

  // Scoring
  std::list<Scorer::Client> scorers;

  // Searching

  std::list<Searcher::Client> searchers;
};

int main(int argc, char *argv[]) {
  spdlog::set_level(spdlog::level::debug);

  config settings = read_config();

  spdlog::info("loading");

  kj::UnixEventPort::captureSignal(SIGINT);
  kj::UnixEventPort::captureSignal(SIGPIPE);

  auto ioContext = kj::setupAsyncIo();

  Master::Client master = kj::heap<MasterImpl>(ioContext, settings);

  Server server(ioContext, "localhost:1234", master);

  ioContext.unixEventPort.onSignal(SIGINT).wait(ioContext.waitScope);

  return 0;
}

