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

#include "channel.h"
#include "util.h"
#include "scrape.h"
#include "crawl.h"
#include "crawler.h"
#include "tokenizer.h"

#include "index.h"

#include "indexer.capnp.h"

#include <kj/debug.h>
#include <kj/async-io.h>
#include <kj/async-unix.h>

#include <capnp/ez-rpc.h>
#include <capnp/message.h>
#include <iostream>

using nlohmann::json;

class MasterImpl final: public Master::Server,
                        public kj::TaskSet::ErrorHandler {
public:
  MasterImpl(const config &s)
    : settings(s), crawler(s), tasks(*this)
  {
    crawler.load();

    std::vector<std::string> blacklist = util::load_list(settings.blacklist_path);
    std::vector<std::string> initial_seed = util::load_list(settings.seed_path);

    crawler.load_blacklist(blacklist);
    crawler.load_seed(initial_seed);

    index_parts = search::load_parts(settings.indexer.meta_path);
  }

  void crawlNext() {
    if (ready_crawlers.empty()) {
      spdlog::warn("no ready crawlers");
      return;
    }

    if (next_site == nullptr) {
      next_site = crawler.get_next_site();
    }

    if (next_site == nullptr) {
      spdlog::warn("no sites");
      return;
    }

    auto site = next_site;
    next_site = nullptr;

    auto proc = ready_crawlers.front();
    ready_crawlers.pop_front();

    auto request = proc.crawlRequest();

    request.setSitePath(site->path);
    request.setDataPath(crawler.get_data_path(site->host));

    request.setMaxPages(site->max_pages);

    /*
    request.setMaxConnections(settings.crawler.site_max_connections);
    request.setMaxPageSize(settings.crawler.max_page_size);
    request.setMaxPartSize(settings.crawler.max_site_part_size);
    */

    site->scraping = true;

    tasks.add(request.send().then(
        [this, site, proc] (auto result) mutable {
          spdlog::info("got response for crawl site {}", site->host);

          ready_crawlers.push_back(kj::mv(proc));

          if (site->level + 1 < crawler.levels.size()) {
            auto level = crawler.levels[site->level];
            auto next_level = crawler.levels[site->level + 1];

            crawler.enable_references(site, level.max_add_sites, next_level.max_pages);
          }

          site->last_scanned = time(NULL);

          // Need to write the changes for this site
          // so the indexer has something to load.
          site->save();
          site->changed = false;

          site->max_pages = 0;
          site->scraped = true;
          site->scraping = false;

          spdlog::info("transition {} to indexing", site->host);
          site->indexing_part = true;
          site->indexed_part = false;
          site->indexed = false;

          index_pending_sites.emplace_back(site);

          have_changes = true;

          indexNext();
          crawlNext();
        },
        [this, site, proc] (auto exception) {
          spdlog::warn("got exception for crawl site {} : {}",
              site->host, std::string(exception.getDescription()));

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

    Merger::Client merger = ready_mergers.front();
    ready_mergers.pop_front();

    auto request = merger.mergeRequest();

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
        [this, p, merger, w_p, p_p, t_p] (auto result) mutable {
          spdlog::info("finished merging part {}", p.first);

          merge_out_w.emplace_back(w_p, p.first, p.second);
          merge_out_p.emplace_back(p_p, p.first, p.second);
          merge_out_t.emplace_back(t_p, p.first, p.second);

          merge_parts_merging.remove(p);

          ready_mergers.push_back(kj::mv(merger));

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
      spdlog::info("no more sites");

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

    spdlog::info("request index for {}", s->host);

    auto request = indexer.indexRequest();
    request.setSitePath(s->path);

    tasks.add(request.send().then(
        [this, s, indexer] (auto result) mutable {
          spdlog::info("got response for index site {}", s->host);

          indexers_pending_flush.insert(&indexer);

          ready_indexers.push_back(kj::mv(indexer));

          indexNext();
        },
        [this, s, &indexer] (auto exception) {
          spdlog::warn("got exception for index site {} : {}", s->host, std::string(exception.getDescription()));
          index_pending_sites.push_back(s);
        }));

    indexNext();
  }

  kj::Promise<void> registerCrawler(RegisterCrawlerContext context) override {
    spdlog::info("got register crawler");

    ready_crawlers.push_back(context.getParams().getCrawler());

    crawlNext();

    return kj::READY_NOW;
  }

  kj::Promise<void> registerIndexer(RegisterIndexerContext context) override {
    spdlog::info("got register indexer");

    ready_indexers.push_back(context.getParams().getIndexer());

    indexNext();

    return kj::READY_NOW;
  }

  kj::Promise<void> registerMerger(RegisterMergerContext context) override {
    spdlog::info("got register merger");

    ready_mergers.push_back(context.getParams().getMerger());

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

  // Crawling

  crawl::crawler crawler;

  crawl::site *next_site = nullptr;

  std::list<Crawler::Client> ready_crawlers;

  // Indexing

  std::list<crawl::site *> index_pending_sites;

  std::list<Indexer::Client> ready_indexers;

  // Index Flushing

  bool flushing_indexers{false};
  std::set<Indexer::Client *> indexers_pending_flush;

  std::list<std::string> index_parts;

  // Merging

  std::list<std::string> index_parts_merging;

  std::list<std::pair<std::string, std::optional<std::string>>> merge_parts_pending;
  std::list<std::pair<std::string, std::optional<std::string>>> merge_parts_merging;

  std::list<search::index_part_info> merge_out_w, merge_out_p, merge_out_t;

  std::list<Merger::Client> ready_mergers;

  kj::TaskSet tasks;
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

  /*
  kj::UnixEventPort::captureSignal(SIGINT);
  auto ioContext = kj::setupAsyncIo();

  auto addrPromise = ioContext.provider->getNetwork().parseAddress(argv[1], 2572)
  .then([](kj::Own<kj::NetworkAddress> addr) {
      spdlog::info("using addr {}", addr.toString());
      return addr->connect().attach(kj::mv(addr));
  });

  auto stream = addrPromise.wait(ioContext.waitScope);

  capnp::TwoPartyVatNetwork network(*stream, rpc::twoparty::Side::SERVER);

  MyEventLoop eventLoop;
  kj::WaitScope waitScope(eventLoop);

  TwoPartyVatNetwork network;
  // kj::Own<kj::AsyncIoStream>&& stream, ReaderOptions readerOpts)

  ReaderOptions readerOpts;

    tasks.add(context->getIoProvider().getNetwork().parseAddress(bindAddress, defaultPort)
        .then(kj::mvCapture(paf.fulfiller,
          [this, readerOpts](kj::Own<kj::PromiseFulfiller<uint>>&& portFulfiller,
                             kj::Own<kj::NetworkAddress>&& addr) {
      auto listener = addr->listen();
      portFulfiller->fulfill(listener->getPort());
      acceptLoop(kj::mv(listener), readerOpts);
    })));


    void acceptLoop(kj::Own<kj::ConnectionReceiver>&& listener, ReaderOptions readerOpts) {
      auto ptr = listener.get();
      tasks.add(ptr->accept().then(kj::mvCapture(kj::mv(listener),
          [this, readerOpts](kj::Own<kj::ConnectionReceiver>&& listener,
                             kj::Own<kj::AsyncIoStream>&& connection) {
        acceptLoop(kj::mv(listener), readerOpts);

        auto server = kj::heap<ServerContext>(kj::mv(connection), *this, readerOpts);

        // Arrange to destroy the server context when all references are gone, or when the
        // EzRpcServer is destroyed (which will destroy the TaskSet).
        tasks.add(server->network.onDisconnect().attach(kj::mv(server)));
      })));
    }

  auto listener = context->getIoProvider().getNetwork()
        .getSockaddr(bindAddress, addrSize)->listen();
  acceptLoop(kj::mv(listener), readerOpts);

  Master::Client master = kj::heap<MasterImpl>(index_pending_sites);

  auto server = makeRpcServer(network, master);

  kj::NEVER_DONE.wait(waitScope);

  //network.onDisconnect().wait(waitScope);
*/

 // auto client = makeRpcClient(network);

  // Set up a server.
  capnp::EzRpcServer server(kj::heap<MasterImpl>(settings), argv[1]);

  auto& waitScope = server.getWaitScope();

  kj::NEVER_DONE.wait(waitScope);

  return 0;
}

