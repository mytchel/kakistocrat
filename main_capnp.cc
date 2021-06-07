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

struct index_part {
  std::string path;
  std::vector<std::string> sites;
  bool merged;

  index_part() = default;

  index_part(const std::string &path,
             const std::vector<std::string> &sites)
    : path(path), sites(sites)
  {}
};

void to_json(nlohmann::json &j, const index_part &p)
{
  j["path"] = p.path;
  j["sites"] = p.sites;
  j["merged"] = p.merged;
}

void from_json(const nlohmann::json &j, index_part &p)
{
  p.path = j.at("path");
  j.at("sites").get_to(p.sites);
  p.merged = j.at("merged");
}

class index_manager {
  std::string path;
  size_t min_pages;

  size_t next_part_id{0};

  std::list<index_part> index_parts;

  std::set<std::string> sites_pending_index;
  std::set<std::string> sites_indexing;

  bool have_changes{false};

public:
  index_manager(const std::string &path, size_t m) 
    : path(path), min_pages(m) {}

  void load() {
    spdlog::debug("load {}", path);

    std::ifstream file;

    file.open(path, std::ios::in);

    if (!file.is_open()) {
      spdlog::warn("error opening file {}", path);
      have_changes = true;
      return;
    }

    try {
      json j = json::parse(file);

      j.at("parts").get_to(index_parts);
      j.at("next_part_id").get_to(next_part_id);

      std::vector<std::string> sites;

      sites.reserve(sites_pending_index.size() + sites_indexing.size());

      sites.insert(sites.end(),
            sites_pending_index.begin(), 
            sites_pending_index.end());

      sites.insert(sites.end(),
            sites_indexing.begin(), 
            sites_indexing.end());

      j.at("sites_pending_index").get_to(sites);

    } catch (const std::exception &e) {
      spdlog::warn("failed ot load {}", path);
    }

    file.close();
  }

  void save() {
    spdlog::debug("save {}", path);

    json j = {
      { "parts", index_parts },
      { "next_part_id", next_part_id },
      { "sites_pending_index", sites_pending_index },
    };

    std::ofstream file;

    file.open(path, std::ios::out | std::ios::trunc);

    if (!file.is_open()) {
      spdlog::warn("error opening file {}", path);
      return;
    }

    file << j;

    file.close();

    have_changes = false;
  }

  bool has_changes() {
    return have_changes;
  }

  bool need_merge() {
    for (auto &p: index_parts) {
      if (!p.merged) {
        return true;
      }
    }

    return false;
  }

  bool need_index() {
    return !sites_pending_index.empty();
  }

  bool index_active() {
    return !sites_indexing.empty();
  }

  size_t get_next_part_id() {
    return next_part_id++;
  }

  std::vector<std::string> get_parts_for_merge() {
    std::vector<std::string> parts;

    parts.reserve(index_parts.size());

    for (auto &p: index_parts) {
      parts.emplace_back(p.path);
    }

    return parts;
  }

  void mark_merged(const std::vector<std::string> &parts) {
    for (auto &p: parts) {
      auto pp = find_part(p);
      if (pp == nullptr) {
        spdlog::error("merged unknown part {}", p);
        continue;
      }

      pp->merged = true;
    }
    
    have_changes = true;
  }

  void mark_indexable(const std::string &site_path) {
    auto removed = pop_parts(site_path);

    for (auto ss: removed) {
      add_indexable(ss);
    }

    add_indexable(site_path);
    
    have_changes = true;
  }

  std::vector<std::string> get_sites_for_index(bool flush) {
    if (!flush && sites_pending_index.size() * 100 < min_pages) {
      spdlog::debug("waiting on more sites, have {} sites", sites_pending_index.size());
      return {};
    }
    
    std::vector<std::string> sites(sites_pending_index.begin(), sites_pending_index.end());

    sites_indexing.insert(sites_pending_index.begin(), sites_pending_index.end());

    sites_pending_index.clear();

    have_changes = true;

    return sites;
  }

  void add_part(const std::string &path, const std::vector<std::string> &sites) {
    for (auto &site_path: sites) {
      auto it = sites_indexing.find(site_path);
      if (it != sites_indexing.end()) {
        sites_indexing.erase(it);
      } else {
        spdlog::warn("add part with site that is not in sites indexing? {}", site_path);
      }

      if (sites_pending_index.find(site_path) != sites_pending_index.end()) {
        spdlog::error("add parts with site that is in pending index? {}", site_path);
      }

      for (auto &p: index_parts) {
        for (auto &ss: p.sites) {
          if (ss == site_path) {
            spdlog::error("add parts with site that is in index part {} : {}", p.path, site_path);
          }
        }
      }
    }

    index_parts.emplace_back(path, sites);

    have_changes = true;
  }

private:
  void add_indexable(const std::string &path) {
    for (auto s: sites_pending_index) {
      if (s == path) {
        return;
      }
    }

    sites_pending_index.emplace(path);
  }

  index_part * find_part(const std::string &path) {
    for (auto &p: index_parts) {
      if (p.path == path) {
        return &p;
      }
    }

    return nullptr;
  }

  std::vector<std::string> pop_parts(const std::string &site_path) {
    std::vector<std::string> removed_sites;

    auto part = index_parts.begin();

    while (part != index_parts.end()) {
      bool site_in_part = false;

      for (auto s: part->sites) {
        if (s == site_path) {
          site_in_part = true;
          break;
        }
      }

      if (site_in_part) {
        removed_sites.insert(removed_sites.end(), part->sites.begin(), part->sites.end());

        part = index_parts.erase(part);
      } else {
        part++;
      }
    }

    return removed_sites;
  }
};

class MasterImpl final: public Master::Server,
                        public kj::TaskSet::ErrorHandler {

 public:
  MasterImpl(kj::AsyncIoContext &io_context, const config &s)
    : settings(s), tasks(*this),
      timer(io_context.provider->getTimer()),
      indexer_manager(s.index_meta_path, s.indexer.pages_per_part),
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

    indexer_manager.load();

    flush();
    checkCrawlers();

    indexNext();
  }

  void checkCrawlers() {
    spdlog::info("checking {} crawlers", crawlers.size());

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

    if (indexer_manager.has_changes()) {
      indexer_manager.save();
    }

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

  void checkMerge() {
    if (!merge_parts_pending.empty() || !merge_parts_merging.empty()) {
      spdlog::debug("merge in progress");
      return;
    }

    if (indexer_manager.need_index()) {
      spdlog::debug("need index");
      return;
    }

    if (!indexer_manager.need_merge()) {
      spdlog::debug("do not need merge");
      return;
    }

    if (time(NULL) < last_merge + settings.merger.frequency_minutes * 60) {
      spdlog::debug("last merge too recent");
      return;
    }

    last_merge = time(NULL);

    spdlog::info("starting merge");
    startMerging();
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

          indexer_manager.mark_indexable(site->path);

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

  void finishedMerging() {
    spdlog::info("finished merging");

    search::index_info info(settings.merger.meta_path);

    info.average_page_length = 0;

    for (auto &path: index_parts_merging) {
      search::index_info index(path);
      index.load();

      for (auto &p: index.page_lengths) {
        info.average_page_length += p.second;
        info.page_lengths.emplace(p.first, p.second);
      }
    }

    if (info.page_lengths.size() > 0) {
      info.average_page_length /= info.page_lengths.size();
    } else {
      info.average_page_length = 0;
    }

    info.word_parts = merge_out_w;
    info.pair_parts = merge_out_p;
    info.trine_parts = merge_out_t;

    info.save();

    merge_out_w.clear();
    merge_out_p.clear();
    merge_out_t.clear();

    indexer_manager.mark_merged(index_parts_merging);

    index_parts_merging.clear();

    last_merge = 0;
  }

  void mergeNext() {
    if (merge_parts_pending.empty()) {
      if (merge_parts_merging.empty()) {
        finishedMerging();

      } else {
        spdlog::info("all parts merging");
      }

      return;
    }

    if (ready_mergers.empty()) {
      spdlog::info("have no ready mergers");
      return;
    }

    auto p = merge_parts_pending.front();
    merge_parts_pending.pop_front();

    merge_parts_merging.push_back(p);

    auto merger = ready_mergers.front();
    ready_mergers.pop_front();

    auto request = merger->mergeRequest();

    request.setStart(p.first);

    if (p.second) {
      request.setEnd(*p.second);
    }

    auto paths = request.initIndexPartPaths(index_parts_merging.size());

    size_t i = 0;
    for (auto &p: index_parts_merging) {
      paths.set(i++, p);
    }

    util::make_path(settings.merger.parts_path);

    auto w_p = fmt::format("{}/index.words.{}.dat", settings.merger.parts_path, p.first);
    auto p_p = fmt::format("{}/index.pairs.{}.dat", settings.merger.parts_path, p.first);
    auto t_p = fmt::format("{}/index.trines.{}.dat", settings.merger.parts_path, p.first);

    request.setWOut(w_p);
    request.setPOut(p_p);
    request.setTOut(t_p);

    tasks.add(request.send().then(
        [this, p, merger, w_p, p_p, t_p] (auto result) {
          spdlog::info("finished merging part {}", p.first);

          merge_out_w.emplace_back(w_p, p.first, p.second);
          merge_out_p.emplace_back(p_p, p.first, p.second);
          merge_out_t.emplace_back(t_p, p.first, p.second);

          merge_parts_merging.remove(p);

          ready_mergers.push_back(merger);

          mergeNext();
        },
        [this, p] (auto exception) {
          spdlog::warn("got exception for merge part {} : {}",
              p.first, std::string(exception.getDescription()));

          merge_parts_merging.remove(p);
          merge_parts_pending.push_back(p);

          // Put merger on ready?

          mergeNext();
        }));

    mergeNext();
  }

  void startMerging() {
    if (!merge_parts_pending.empty() || !merge_parts_merging.empty()) {
      spdlog::warn("got start merge while still merging");
      return;
    }

    auto s = search::get_split_at(settings.index_parts);

    auto start = s.begin();
    while (start != s.end()) {
      std::optional<std::string> end;
      if (start + 1 != s.end()) {
        end = *(start + 1);
      }

      merge_parts_pending.emplace_back(*start, end);
      start++;
    }

    index_parts_merging = indexer_manager.get_parts_for_merge();

    mergeNext();
  }

  std::list<std::vector<std::string>> index_ops;

  void indexSites(Indexer::Client *indexer, const std::vector<std::string> &sites) {
    spdlog::info("request index for {} sites", sites.size());

    auto &op = index_ops.emplace_back(sites);

    auto request = indexer->indexRequest();

    std::string output = fmt::format("{}/{}",
      settings.indexer.parts_path,
      indexer_manager.get_next_part_id());

    auto paths = request.initSitePaths(op.size());
    size_t i = 0;
    for (auto &s: op) {
      paths.set(i++, s);
      spdlog::info("indexing {} -> {}", s);
    }

    request.setOutputBase(output);

    tasks.add(request.send().then(
        [this, &op, indexer] (auto result) {
          std::list<std::string> parts;

          for (auto p: result.getOutputs()) {
            std::string path = p.getPath();

            std::vector<std::string> sites;

            for (auto s_path_reader: p.getSites()) {
              sites.emplace_back(s_path_reader);
            }
          
            spdlog::info("index new part {} with {} sites", path, sites.size());
            indexer_manager.add_part(path, sites);
          }

          index_ops.remove_if(
              [&op] (auto &o) {
                return &op == &o;
              });
 
          ready_indexers.push_back(indexer);
        },
        [this, &op, indexer] (auto exception) {
          spdlog::warn("got exception while indexing {}", std::string(exception.getDescription()));
          // TODO store sites somehow.

          for (auto &s: op) {
            spdlog::info("index finished for {}", s);
            indexer_manager.mark_indexable(s);
          }

          index_ops.remove_if(
              [&op] (auto &o) {
                return &op == &o;
              });
        }));
  }

  void indexNext() {
    tasks.add(timer.afterDelay(30 * kj::SECONDS).then(
          [this] () {
            indexNext();
          }));

    if (!indexer_manager.need_index()) {
      checkMerge();
      return;
    }

    if (ready_indexers.empty()) {
      spdlog::info("no ready indexers");
      return;
    }
    
    bool flush = time(NULL) > last_index + settings.merger.frequency_minutes * 60;

    auto sites = indexer_manager.get_sites_for_index(flush);
    if (sites.empty()) {
      return;
    }

    last_index = time(NULL);

    if (sites.size() * 100 > 4 * settings.indexer.pages_per_part) {
      std::vector<std::vector<std::string>> parts(ready_indexers.size());

      size_t i = 0;
      for (auto &site: sites) {
        parts[i++ % parts.size()].emplace_back(site);
      }

      for (auto &part: parts) {
        auto indexer = ready_indexers.front();
        ready_indexers.pop_front();

        indexSites(indexer, part);
      }

    } else {
      auto indexer = ready_indexers.front();
      ready_indexers.pop_front();

      indexSites(indexer, sites);
    }
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

    return kj::READY_NOW;
  }

  kj::Promise<void> registerMerger(RegisterMergerContext context) override {
    spdlog::debug("got register merger");

    mergers.push_back(context.getParams().getMerger());
    ready_mergers.push_back(&mergers.back());

    if (!merge_parts_pending.empty()) {
      mergeNext();
    }

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

  index_manager indexer_manager;

  std::list<Indexer::Client> indexers;
  std::list<Indexer::Client *> ready_indexers;

  // Merging

  std::list<Merger::Client> mergers;
  std::list<Merger::Client *> ready_mergers;

  std::vector<std::string> index_parts_merging;

  std::list<std::pair<std::string, std::optional<std::string>>> merge_parts_pending;
  std::list<std::pair<std::string, std::optional<std::string>>> merge_parts_merging;

  std::vector<search::index_part_info> merge_out_w, merge_out_p, merge_out_t;

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

  //kj::UnixEventPort::captureSignal(SIGINT);
  auto ioContext = kj::setupAsyncIo();

  Master::Client master = kj::heap<MasterImpl>(ioContext, settings);

  Server server(ioContext, "localhost:1234", master);

  kj::NEVER_DONE.wait(ioContext.waitScope);

  return 0;
}

