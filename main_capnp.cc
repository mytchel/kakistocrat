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

class Server final : public kj::TaskSet::ErrorHandler {

  const config &settings;
  capnp::Capability::Client mainInterface;
  kj::TaskSet tasks;

  struct ServerContext {
    kj::Own<kj::AsyncIoStream> stream;
    capnp::TwoPartyVatNetwork network;
    capnp::RpcSystem<capnp::rpc::twoparty::VatId> rpcSystem;

    ServerContext(kj::Own<kj::AsyncIoStream>&& stream, capnp::Capability::Client bootstrap,
                  capnp::ReaderOptions readerOpts)
        : stream(kj::mv(stream)),
          network(*this->stream, capnp::rpc::twoparty::Side::SERVER, readerOpts),
          rpcSystem(makeRpcServer(network, bootstrap)) {}
  };

public:
  Server(kj::AsyncIoContext &io_context, const config &s, capnp::Capability::Client mainInterface)
    : settings(s), tasks(*this), mainInterface(kj::mv(mainInterface))
  {
    std::string bindAddress = "localhost:1234";

    capnp::ReaderOptions readerOpts;

    spdlog::info("server setup for {}", bindAddress);

    tasks.add(io_context.provider->getNetwork().parseAddress(bindAddress, 1234)
        .then([this, readerOpts](kj::Own<kj::NetworkAddress>&& addr) {

      //spdlog::info("server ready: {}", std::string(addr->toString()));
      spdlog::info("server ready");
      auto listener = addr->listen();
      acceptLoop(kj::mv(listener), readerOpts);
    }));
  }

  void acceptLoop(kj::Own<kj::ConnectionReceiver>&& listener, capnp::ReaderOptions readerOpts) {
    auto ptr = listener.get();
    tasks.add(ptr->accept().then(kj::mvCapture(kj::mv(listener),
        [this, readerOpts](kj::Own<kj::ConnectionReceiver>&& listener,
                           kj::Own<kj::AsyncIoStream>&& connection) {
      acceptLoop(kj::mv(listener), readerOpts);

      spdlog::info("server got connection");

      auto server = kj::heap<ServerContext>(kj::mv(connection), mainInterface, readerOpts);

      // Arrange to destroy the server context when all references are gone, or when the
      // EzRpcServer is destroyed (which will destroy the TaskSet).
      tasks.add(server->network.onDisconnect().attach(kj::mv(server)));
    })));
  }

  void taskFailed(kj::Exception&& exception) override {
    spdlog::warn("task failed: {}", std::string(exception.getDescription()));
    kj::throwFatalException(kj::mv(exception));
  }
};

class MasterImpl final: public Master::Server,
                        public kj::TaskSet::ErrorHandler {

 public:
  MasterImpl(kj::AsyncIoContext &io_context, const config &s)
    : settings(s), tasks(*this),
      timer(io_context.provider->getTimer()),
      crawler(s)
  {
    crawler.load();

    std::vector<std::string> blacklist = util::load_list(settings.blacklist_path);
    std::vector<std::string> initial_seed = util::load_list(settings.seed_path);

    crawler.load_blacklist(blacklist);
    crawler.load_seed(initial_seed);

    index_parts = search::load_parts(settings.indexer.meta_path);

    for (auto &site: crawler.sites) {
      if (site.scraped && !site.indexed && !site.indexed_part) {
        spdlog::info("setting site {} for indexing", site.m_site.host);
        index_pending_sites.emplace_back(&site);
      }
    }

    checkCrawlers();
    flush();
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
          spdlog::info("crawler {} can crawl: {}", (uintptr_t) &c, can_crawl);

          if (can_crawl) {
            auto it = std::find(ready_crawlers.begin(), ready_crawlers.end(), &c);
            if (it == ready_crawlers.end()) {
              spdlog::info("crawler {} transition to ready", (uintptr_t) &c);
              ready_crawlers.push_back(&c);
              crawlNext();
            }
          }
        }));
    }

    tasks.add(timer.afterDelay(1 * kj::SECONDS).then(
          [this] () {
            checkCrawlers();
          }));
  }

  void flush() {
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

  void crawlNext() {
    if (ready_crawlers.empty()) {
      spdlog::info("no ready crawlers");
      return;
    }

    if (next_site == nullptr) {
      next_site = crawler.get_next_site();
    }

    if (next_site == nullptr) {
      spdlog::info("no sites to crawl");
      return;
    }

    auto site = next_site;
    next_site = nullptr;

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

    tasks.add(request.send().then(
        [this, site, proc] (auto result) mutable {
          spdlog::info("got response for crawl site {}", site->m_site.host);

          site->reload();

          crawler.expand_links(site);

          if (site->level + 1 < crawler.levels.size()) {
            auto level = crawler.levels[site->level];
            auto next_level = crawler.levels[site->level + 1];

            crawler.enable_references(site, level.max_add_sites, next_level.max_pages);
          }

          site->last_scanned = time(NULL);

          // Need to write the changes for this site
          // so the indexer has something to load.
          site->flush();

          site->max_pages = 0;
          site->scraped = true;
          site->scraping = false;

          spdlog::info("transition {} to indexing", site->m_site.host);
          site->indexing_part = true;
          site->indexed_part = false;
          site->indexed = false;

          have_changes = true;

          index_pending_sites.emplace_back(site);
          indexNext();

          auto it = std::find(ready_crawlers.begin(), ready_crawlers.end(), proc);
          if (it == ready_crawlers.end()) {
            spdlog::info("crawler {} transition to ready", (uintptr_t) proc);
            ready_crawlers.push_back(proc);
          }

          crawlNext();
        },
        [this, site, proc] (auto exception) {
          spdlog::warn("got exception for crawl site {} : {}",
              site->m_site.host, std::string(exception.getDescription()));

          site->scraping = false;
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

    info.word_parts = std::vector<search::index_part_info>(merge_out_w.begin(), merge_out_w.end());
    info.pair_parts = std::vector<search::index_part_info>(merge_out_p.begin(), merge_out_p.end());
    info.trine_parts = std::vector<search::index_part_info>(merge_out_t.begin(), merge_out_t.end());

    info.save();

    merge_out_w.clear();
    merge_out_p.clear();
    merge_out_t.clear();

    index_parts_merging.clear();
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

  void startMerging(const std::list<std::string> &index_parts) {
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

    index_parts_merging = index_parts;

    mergeNext();
  }

  void flushIndexerDone(Indexer::Client *i, const std::list<std::string> &output_paths) {
    for (auto &p: output_paths) {
      spdlog::info("adding index part {}", p);
      index_parts.emplace_back(p);
    }

    search::save_parts(settings.indexer.meta_path, index_parts);

    spdlog::info("indexer flushed {}, remove from pending", (size_t) i);
    indexers_pending_flush.erase(i);

    if (indexers_pending_flush.empty()) {
      spdlog::info("indexers all flushed");

      for (auto &p: index_parts) {
        spdlog::info("part ready for merging {}", p);
      }

      startMerging(index_parts);

      index_parts.clear();

      flushing_indexers = false;

    } else {
      spdlog::info("waiting on {} indexers to flush", indexers_pending_flush.size());
    }
  }

  void flushIndexers() {
    if (flushing_indexers) {
      spdlog::warn("already flushing");
      return;
    }

    if (indexers_pending_flush.empty()) {
      spdlog::warn("nothing to flush");
      return;
    }

    spdlog::info("flushing indexers");

    flushing_indexers = true;

    for (auto i: indexers_pending_flush) {
      auto request = i->flushRequest();

      auto p = request.send().then(
        [this, i] (auto result) {
          spdlog::info("indexer flushed");

          std::list<std::string> output_paths;

          for (auto p: result.getOutputPaths()) {
            output_paths.emplace_back(p);
          }

          flushIndexerDone(i, output_paths);
        },
        [this, i] (auto exception) {
          spdlog::warn("indexer flush failed");

          std::list<std::string> empty_output_paths;

          flushIndexerDone(i, empty_output_paths);
        });

      tasks.add(kj::mv(p));
    }
  }

  void indexNext() {

    if (ready_indexers.empty()) {
      spdlog::info("no ready indexers");
      return;
    }

    if (index_pending_sites.empty()) {
      spdlog::info("no sites to index");

      flushIndexers();

      return;
    }

    auto indexer = ready_indexers.front();
    ready_indexers.pop_front();

    spdlog::info("get pending site for indexer");

    // Should put these somewhere and remove them
    // when a flush is done.

    auto s = index_pending_sites.front();
    index_pending_sites.pop_front();

    spdlog::info("request index for {}", s->m_site.host);

    auto request = indexer->indexRequest();
    request.setSitePath(s->m_site.path);

    tasks.add(request.send().then(
        [this, s, indexer] (auto result) {
          spdlog::info("got response for index site {}", s->m_site.host);

          indexers_pending_flush.insert(indexer);

          ready_indexers.push_back(indexer);

          indexNext();
        },
        [this, s, indexer] (auto exception) {
          spdlog::warn("got exception for index site {} : {}", s->m_site.host, std::string(exception.getDescription()));
          index_pending_sites.push_back(s);
        }));

    indexNext();
  }

  kj::Promise<void> registerCrawler(RegisterCrawlerContext context) override {
    spdlog::info("got register crawler");

    crawlers.push_back(context.getParams().getCrawler());

    ready_crawlers.push_back(&crawlers.back());

    crawlNext();

    return kj::READY_NOW;
  }

  kj::Promise<void> registerIndexer(RegisterIndexerContext context) override {
    spdlog::info("got register indexer");

    indexers.push_back(context.getParams().getIndexer());
    ready_indexers.push_back(&indexers.back());

    indexNext();

    return kj::READY_NOW;
  }

  kj::Promise<void> registerMerger(RegisterMergerContext context) override {
    spdlog::info("got register merger");

    mergers.push_back(context.getParams().getMerger());
    ready_mergers.push_back(&mergers.back());

    if (!merge_parts_pending.empty()) {
      mergeNext();
    }

    return kj::READY_NOW;
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

  crawl::site *next_site = nullptr;

  std::list<Crawler::Client> crawlers;
  std::list<Crawler::Client *> ready_crawlers;

  // Indexing

  std::list<Indexer::Client> indexers;
  std::list<Indexer::Client *> ready_indexers;

  std::list<crawl::site *> index_pending_sites;

  // Index Flushing

  bool flushing_indexers{false};
  std::set<Indexer::Client *> indexers_pending_flush;

  std::list<std::string> index_parts;

  // Merging

  std::list<Merger::Client> mergers;
  std::list<Merger::Client *> ready_mergers;

  std::list<std::string> index_parts_merging;

  std::list<std::pair<std::string, std::optional<std::string>>> merge_parts_pending;
  std::list<std::pair<std::string, std::optional<std::string>>> merge_parts_merging;

  std::list<search::index_part_info> merge_out_w, merge_out_p, merge_out_t;
};

int main(int argc, char *argv[]) {
  spdlog::set_level(spdlog::level::debug);

  if (argc != 2) {
    std::cerr << "usage: " << argv[0] << " ADDRESS[:PORT]\n"
        "Runs the server bound to the given address/port.\n"
        "ADDRESS may be '*' to bind to all local addresses.\n"
        ":PORT may be omitted to choose a port automatically." << std::endl;
    return 1;
  }

  config settings = read_config();

  spdlog::info("loading");

  //kj::UnixEventPort::captureSignal(SIGINT);
  auto ioContext = kj::setupAsyncIo();

  Master::Client master = kj::heap<MasterImpl>(ioContext, settings);

  Server server(ioContext, settings, master);

  kj::NEVER_DONE.wait(ioContext.waitScope);

/*

  auto io_context = kj::EzRpcContext::getThreadLocal();

  // Set up a server.
  capnp::EzRpcServer server(kj::heap<MasterImpl>(io_context, settings), argv[1]);

  auto& waitScope = server.getWaitScope();

  kj::NEVER_DONE.wait(waitScope);
*/

  return 0;
}

