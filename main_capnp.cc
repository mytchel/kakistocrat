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

class index_manager {
  struct index_part {
    std::string path;
    std::vector<crawl::site *> sites;

    index_part(const std::string &path,
               const std::vector<crawl::site *> &sites)
      : path(path), sites(sites)
    {}
  };

  std::list<index_part> index_parts;

  std::vector<crawl::site *> sites_pending_index;
  size_t sites_pending_index_page_count{0};

  size_t min_pages;

public:
  index_manager(size_t m) : min_pages(m) {}

  std::vector<std::string> get_parts() {
    std::vector<std::string> parts;

    parts.reserve(index_parts.size());

    for (auto &p: index_parts) {
      parts.emplace_back(p.path);
    }

    return parts;
  }

  index_part * find_part(const std::string &path) {
    for (auto &p: index_parts) {
      if (p.path == path) {
        return &p;
      }
    }

    return nullptr;
  }


  void mark_merged(const std::vector<std::string> &parts) {
    for (auto &p: parts) {
      auto pp = find_part(p);
      if (pp == nullptr) {
        spdlog::error("merged unknown part {}", p);
        continue;
      }

      for (auto s: pp->sites) {
        s->merged = true;
      }
    }
  }

  std::vector<crawl::site *> next_index(bool flush) {
    std::vector<crawl::site *> sites;

    if (!flush && sites_pending_index_page_count < min_pages) {
      spdlog::debug("waiting on more pages, have {}", sites_pending_index_page_count);
      return sites;
    }

    
    sites.insert(sites.begin(), sites_pending_index.begin(), sites_pending_index.end());

    sites_pending_index.clear();
    sites_pending_index_page_count = 0;

    return sites;
  }

  std::vector<crawl::site *> pop_parts(crawl::site *site) {
    std::vector<crawl::site *> removed_sites;

    auto part = index_parts.begin();

    while (part != index_parts.end()) {
      bool site_in_part = false;

      for (auto s: part->sites) {
        if (s == site) {
          site_in_part = true;
          break;
        }
      }

      if (site_in_part) {
        for (auto s: part->sites) {
          s->indexed = false;
        }

        removed_sites.insert(removed_sites.end(), part->sites.begin(), part->sites.end());

        part = index_parts.erase(part);
      } else {
        part++;
      }
    }

    return removed_sites;
  }

  void mark_indexable(crawl::site *s) {
    auto removed = pop_parts(s);

    for (auto ss: removed) {
      add_indexable(ss);
    }

    add_indexable(s);
  }

  void add_parts(std::vector<crawl::site *> sites, std::list<std::string> parts) {
    for (auto s: sites) {
      s->indexed = true;

      for (auto ss: sites_pending_index) {
        spdlog::error("add parts with site that is in pending index? {}", s->m_site.host);
      }

      for (auto &p: index_parts) {
        for (auto ss: p.sites) {
          if (ss == s) {
            spdlog::error("add parts with site that is in index part {} : {}", p.path, s->m_site.host);
          }
        }
      }
    }

    for (auto &part: parts) {
      index_parts.emplace_back(part, sites);
    }
  }

private:
  void add_indexable(crawl::site *ss) {
    for (auto s: sites_pending_index) {
      if (s == ss) {
        return;
      }
    }

    sites_pending_index_page_count += ss->page_count;
    sites_pending_index.push_back(ss);
  }
};

class MasterImpl final: public Master::Server,
                        public kj::TaskSet::ErrorHandler {

 public:
  MasterImpl(kj::AsyncIoContext &io_context, const config &s)
    : settings(s), tasks(*this),
      timer(io_context.provider->getTimer()),
      indexer_manager(s.indexer.pages_per_part),
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

    //index_parts = search::load_parts(settings.indexer.meta_path);
    next_index_part_id = 0;//index_parts.size();

    for (auto &s: crawler.sites) {
      s.indexed = false;
      s.merged = false;

      if (s.scraped) {
        s.load();
        indexer_manager.mark_indexable(&s);
        s.flush();
      }
    }

    flush();
    checkCrawlers();
    checkIndexing();

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
          crawlers.remove_if(
              [&c] (auto cc) {
                return &cc == &c;
              });
        }));
    }

    tasks.add(timer.afterDelay(1 * kj::SECONDS).then(
          [this] () {
            checkCrawlers();
          }));
  }

  void flush() {
    //search::save_parts(settings.indexer.meta_path, index_parts);

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

    tasks.add(timer.afterDelay(10 * kj::SECONDS).then(
          [this] () {
            flush();
          }));
  }

  void checkIndexing() {
    tasks.add(timer.afterDelay(10 * kj::SECONDS).then(
          [this] () {
            checkIndexing();
          }));

    if (!merge_parts_pending.empty() || !merge_parts_merging.empty()) {
      spdlog::debug("merge in progress");
      return;
    }

    if (time(NULL) < last_merge + settings.merger.frequency_minutes * 60) {
      spdlog::debug("last merge too recent");
      return;
    }

    last_merge = time(NULL);

    bool all_indexed = true;
    bool all_merged = true;

    for (auto &site: crawler.sites) {
      if (!site.scraped) continue;

      if (!site.indexed) {
        all_indexed = false;
        break;
      }

      if (!site.merged) {
        all_merged = false;
      }
    }

    if (all_indexed && !all_merged) {
      spdlog::info("starting merge");
      startMerging();
    } else {
      spdlog::debug("not merging, all indexed = {}, all merged = {}",
        all_indexed, all_merged);
    }
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

    request.setSitePath(site->m_site.path);
    request.setDataPath(crawler.get_data_path(site->m_site.host));

    request.setMaxPages(site->max_pages);

    /*
    request.setMaxConnections(settings.crawler.site_max_connections);
    request.setMaxPageSize(settings.crawler.max_page_size);
    request.setMaxPartSize(settings.crawler.max_site_part_size);
    */

    site->scraping = true;

    spdlog::info("start crawling {}", site->m_site.host);

    tasks.add(request.send().then(
        [this, site, proc] (auto result) {
          spdlog::info("got response for crawl site {}", site->m_site.host);

          site->reload();

          crawler.expand_links(site);

          if (site->level + 1 < crawler.levels.size()) {
            auto level = crawler.levels[site->level];
            auto next_level = crawler.levels[site->level + 1];

            crawler.enable_references(site, level.max_add_sites, next_level.max_pages);
          }

          site->last_scanned = time(NULL);

          size_t page_count = site->m_site.pages.size();
          spdlog::debug("saving scraped site with {} pages", page_count);

          // Need to write the changes for this site
          // so the indexer has something to load.
          site->flush();

          site->max_pages = 0;
          site->scraped = true;
          site->scraping = false;

          site->indexed = false;
          site->merged = false;

          have_changes = true;

          indexer_manager.mark_indexable(site);

          auto it = std::find(ready_crawlers.begin(), ready_crawlers.end(), proc);
          if (it == ready_crawlers.end()) {
            ready_crawlers.push_back(proc);
          }

          crawlNext();
        },
        [this, site, proc] (auto exception) {
          spdlog::warn("got exception for crawl site {} : {}",
              site->m_site.host, std::string(exception.getDescription()));

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

    index_parts_merging = indexer_manager.get_parts();

    mergeNext();
  }

  void indexSites(Indexer::Client *indexer, std::vector<crawl::site *> sites) {
    spdlog::info("request index for {} sites", sites.size());

    auto request = indexer->indexRequest();

    std::string output = fmt::format("{}/{}", settings.indexer.parts_path, next_index_part_id++);

    auto paths = request.initSitePaths(sites.size());
    size_t i = 0;
    for (auto &s: sites) {
      paths.set(i++, s->m_site.path);
      spdlog::info("indexing {} -> {}", s->m_site.path, output);
    }

    request.setOutputBase(output);

    tasks.add(request.send().then(
        [this, sites(std::move(sites)), output, indexer] (auto result) {
          for (auto &s: sites) {
            spdlog::info("index finished for {}", s->m_site.path);
          }

          std::list<std::string> parts;

          for (auto p: result.getOutputPaths()) {
            spdlog::info("index new part {}", std::string(p));
            parts.emplace_back(p);
          }

          indexer_manager.add_parts(sites, parts);

          ready_indexers.push_back(indexer);
        },
        [this, indexer] (auto exception) {
          spdlog::warn("got exception while indexing {}", std::string(exception.getDescription()));
          // TODO store sites somehow.
        }));
  }

  void indexNext(bool flush = false) {
    tasks.add(timer.afterDelay(30 * kj::SECONDS).then(
          [this] () {
            indexNext();
          }));

    if (ready_indexers.empty()) {
      spdlog::info("no ready indexers");
      return;
    }

    auto sites = indexer_manager.next_index(flush);
    if (sites.empty()) {
      return;
    }

    std::vector<std::vector<crawl::site *>> parts(ready_indexers.size());
    
    size_t page_count = 0;
    size_t part_page_count = 0;
    size_t part_i = 0;

    for (auto site: sites) {
      part_page_count += site->page_count;
      page_count += site->page_count;

      parts[part_i].push_back(site);

      if (part_page_count >= settings.indexer.pages_per_part) {
        part_i = (part_i + 1) % parts.size();
      }
    }
    
    if (page_count < 2 * settings.indexer.pages_per_part) {
      for (part_i = 1; part_i < parts.size(); part_i++) {
        parts[0].insert(parts[0].end(),
            parts[part_i].begin(), parts[part_i].end());
      }
    }

    
    for (part_i = 0; part_i < parts.size(); part_i++) {
      if (parts[part_i].empty()) break;

      auto indexer = ready_indexers.front();
      ready_indexers.pop_front();

      indexSites(indexer, parts[part_i]);
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

    size_t i = 0;
    for (auto s: sites) {
      paths.set(i++, s->m_site.path);
    }

    tasks.add(request.send().then(
          [] (auto result) {
            spdlog::info("scoring finished");
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

  size_t next_index_part_id;

  // Merging

  std::list<Merger::Client> mergers;
  std::list<Merger::Client *> ready_mergers;

  std::vector<std::string> index_parts_merging;

  std::list<std::pair<std::string, std::optional<std::string>>> merge_parts_pending;
  std::list<std::pair<std::string, std::optional<std::string>>> merge_parts_merging;

  std::vector<search::index_part_info> merge_out_w, merge_out_p, merge_out_t;

  time_t last_merge{0};

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

