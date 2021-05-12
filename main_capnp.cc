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

    index_parts_pending_merge = search::load_parts(settings.indexer.meta_path);
    index_part_id = index_parts_pending_merge.size();

    for (auto &site: crawler.sites) {
      if (site.scraped && !site.merged && !site.indexed) {
        spdlog::info("setting site {} for indexing", site.m_site.host);

        site.load();

        sites_pending_index.emplace_back(&site);
        sites_pending_index_page_count += site.m_site.pages.size();
      }
    }

    flush();
    checkCrawlers();
    checkStartMerge();

    indexNext();
  }

  void checkCrawlers() {
    // TODO
    return;

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
          spdlog::info("crawler {} error checking can crawl", (uintptr_t) &c);
          // TODO: remove crawler
        }));
    }

    tasks.add(timer.afterDelay(1 * kj::SECONDS).then(
          [this] () {
            checkCrawlers();
          }));
  }

  void flush() {
    search::save_parts(settings.indexer.meta_path, index_parts_pending_merge);

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

  void checkStartMerge() {
    tasks.add(timer.afterDelay(10 * kj::SECONDS).then(
          [this] () {
            checkStartMerge();
          }));

    if (!merge_parts_pending.empty() || !merge_parts_merging.empty()) {
      return;
    }

    if (index_parts_pending_merge.empty()) {
      return;
    }

    spdlog::debug("check merge ready");

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
      spdlog::debug("starting merge");
      startMerging();
    }
  }

  void crawlNext() {
    if (ready_crawlers.empty()) {
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

    spdlog::info("start crawling {}", site->m_site.host);

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

          size_t page_count = site->m_site.pages.size();

          // Need to write the changes for this site
          // so the indexer has something to load.
          site->flush();

          site->max_pages = 0;
          site->scraped = true;
          site->scraping = false;

          site->indexed = false;
          site->merged = false;

          have_changes = true;

          sites_pending_index.emplace_back(site);
          sites_pending_index_page_count += page_count;

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

    info.word_parts = std::vector<search::index_part_info>(merge_out_w.begin(), merge_out_w.end());
    info.pair_parts = std::vector<search::index_part_info>(merge_out_p.begin(), merge_out_p.end());
    info.trine_parts = std::vector<search::index_part_info>(merge_out_t.begin(), merge_out_t.end());

    info.save();

    merge_out_w.clear();
    merge_out_p.clear();
    merge_out_t.clear();

    for (auto s: sites_merging) {
      s->merged = true;
    }

    index_parts_merging.clear();
    sites_merging.clear();
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

    index_parts_merging = index_parts_pending_merge;
    sites_merging = sites_pending_merge;

    index_parts_pending_merge.clear();
    sites_pending_merge.clear();

    mergeNext();
  }

  void indexNext(bool flush = false) {

    if (ready_indexers.empty()) {
      spdlog::info("no ready indexers");
      return;
    }

    if (!flush && sites_pending_index_page_count < settings.indexer.pages_per_part) {
      spdlog::debug("waiting on more pages, have {}", sites_pending_index_page_count);
      return;
    }

    auto sites = std::move(sites_pending_index);

    sites_pending_index_page_count = 0;
    sites_pending_index.clear();

    auto indexer = ready_indexers.front();
    ready_indexers.pop_front();

    spdlog::info("request index for {} sites", sites.size());

    auto request = indexer->indexRequest();

    std::string output = fmt::format("{}/{}", settings.indexer.parts_path, index_part_id++);

    auto paths = request.initSitePaths(sites.size());
    size_t i = 0;
    for (auto &s: sites) {
      paths.set(i++, s->m_site.path);
      spdlog::info("indexing {} -> {}", s->m_site.path, output);
    }

    request.setOutputBase(output);

    tasks.add(request.send().then(
        [this, sites(std::move(sites)), indexer] (auto result) {
          for (auto &s: sites) {
            spdlog::info("index finished for {}", s->m_site.path);
            sites_pending_merge.push_back(s);
            s->indexed = true;
          }

          for (auto p: result.getOutputPaths()) {
            spdlog::info("index new part {}", std::string(p));
            index_parts_pending_merge.emplace_back(p);
          }

          ready_indexers.push_back(indexer);

          indexNext();
        },
        [this, indexer] (auto exception) {
          spdlog::warn("got exception while indexing {}", std::string(exception.getDescription()));
          // TODO store sites somehow.
        }));
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

  size_t index_part_id;

  std::list<crawl::site *> sites_pending_index;
  size_t sites_pending_index_page_count;

  // Merging

  std::list<std::string> index_parts_pending_merge;
  std::list<crawl::site *> sites_pending_merge;

  std::list<Merger::Client> mergers;
  std::list<Merger::Client *> ready_mergers;

  std::list<std::string> index_parts_merging;
  std::list<crawl::site *> sites_merging;

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

