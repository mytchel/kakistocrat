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
#include "scrape.h"
#include "crawler.h"

#include "curl_kj.h"

#include "indexer.capnp.h"

#include <kj/debug.h>
#include <kj/string.h>
#include <kj/async-io.h>
#include <kj/async-unix.h>
#include <kj/compat/http.h>

#include <capnp/rpc.h>
#include <capnp/rpc-twoparty.h>
#include <capnp/ez-rpc.h>
#include <capnp/message.h>
#include <iostream>

using nlohmann::json;

class CrawlerImpl final: public Crawler::Server,
                         public kj::TaskSet::ErrorHandler {

  struct adaptor {
  public:
    adaptor(kj::PromiseFulfiller<void> &fulfiller,
            CrawlerImpl *crawler,
            CrawlContext context,
            scrape::site site)
        : fulfiller(fulfiller),
          crawler(crawler),
          context(kj::mv(context)),
          site(std::move(site))
    {
      crawler->add_adaptor(this);
    }

    void finish() {
      fulfiller.fulfill();
    }

    kj::PromiseFulfiller<void> &fulfiller;
    CrawlerImpl *crawler;
    CrawlContext context;
    scrape::site site;
  };

public:
  CrawlerImpl(const config &settings, kj::AsyncIoContext &io_context)
    : settings(settings), tasks(*this),
      curl(io_context, settings.crawler.thread_max_connections),
      max_ops(settings.crawler.thread_max_connections)
  {

  }

  kj::Promise<void> crawl(CrawlContext context) override {
    spdlog::info("got crawl request");

    auto params = context.getParams();

    std::string site_path = params.getSitePath();
    std::string data_path = params.getDataPath();
    size_t max_pages = params.getMaxPages();

    spdlog::info("make scrape site  {}", site_path);
    scrape::site scrape_site(
        site_path, data_path, max_pages,
        settings.crawler.site_max_connections,
        settings.crawler.max_site_part_size,
        settings.crawler.max_page_size);

    spdlog::info("made scrape site {} {}", scrape_site.m_site.host, site_path);
    return kj::newAdaptedPromise<void, adaptor>(this, kj::mv(context), std::move(scrape_site));
  }

  void add_adaptor(adaptor *a) {
    spdlog::debug("add adaptor");
    adaptors.push_back(a);
    process();
  }

  void process() {
    spdlog::debug("processing");

    auto a = adaptors.begin();

    bool all_blocked = true;

    while (a != adaptors.end()) {
      spdlog::debug("check adaptor");

      auto &s = (*a)->site;

      if (s.finished()) {
        spdlog::info("finished {}", s.m_site.host);

        s.m_site.save();

        (*a)->finish();
        a = adaptors.erase(a);

        continue;
      }

      if (ops.size() + 1 >= max_ops) {
        spdlog::info("at max ops {}", ops.size());
        break;
      }

      spdlog::info("checking {} for op", s.m_site.host);

      auto m_op = s.get_next();
      if (m_op) {
        spdlog::info("got op for {}", s.m_site.host);

        auto op = *m_op;

        ops.emplace_back(op);

        tasks.add(curl.add(op->url, op->buf, op->buf_max).then(
          [this, op] (curl_response response) {
            spdlog::info("one op done");

            op->size = response.size;

            if (response.success) {
              op->finish(response.done_url);
            } else {
              op->finish_bad(true);
            }

            ops.remove(op);
            delete op;

            process();
          }));

        all_blocked = false;
      }

      a++;
    }

    if (!all_blocked) {
      spdlog::debug("not all blocked, call again");
      process();
    } else {
      spdlog::info("crawler all blocked with {} sites", adaptors.size());

      for (auto a: adaptors) {
        spdlog::info("crawling site {} with {} ops", a->site.m_site.host, a->site.using_bufs.size());
      }
    }
  }

  void taskFailed(kj::Exception&& exception) override {
    spdlog::warn("crawler task failed: {}", std::string(exception.getDescription()));
    //kj::throwFatalException(kj::mv(exception));
  }

  const config &settings;
  kj::TaskSet tasks;

  curl_kj curl;

  std::list<adaptor*> adaptors;
  std::list<scrape::site_op*> ops;

  size_t max_ops;
};

int main(int argc, char *argv[]) {
  spdlog::set_level(spdlog::level::debug);

  if (argc != 2) {
    spdlog::error("bad args");
    return 1;
  }

  spdlog::info("read config");
  config settings = read_config();

  // two way vat

  kj::UnixEventPort::captureSignal(SIGINT);
  auto ioContext = kj::setupAsyncIo();

  auto addrPromise = ioContext.provider->getNetwork().parseAddress(argv[1], 2572)
  .then([](kj::Own<kj::NetworkAddress> addr) {
      spdlog::info("using addr {}", std::string(addr->toString().cStr()));
      return addr->connect().attach(kj::mv(addr));
  });

  auto stream = addrPromise.wait(ioContext.waitScope);

  capnp::TwoPartyVatNetwork network(*stream, capnp::rpc::twoparty::Side::CLIENT);

  auto rpcSystem = capnp::makeRpcClient(network);

  {
    capnp::MallocMessageBuilder message;
    auto hostId = message.getRoot<capnp::rpc::twoparty::VatId>();
    hostId.setSide(capnp::rpc::twoparty::Side::SERVER);

    Master::Client master = rpcSystem.bootstrap(hostId).castAs<Master>();

    spdlog::info("creating client");

    Crawler::Client crawler = kj::heap<CrawlerImpl>(settings, ioContext);

    spdlog::info("create request");
    auto request = master.registerCrawlerRequest();
    request.setCrawler(crawler);

    spdlog::info("send crawler register");

    auto r = request.send().then(
        [] (auto result) {
          spdlog::info("crawler registered");
        },
        [] (auto exception) {
          spdlog::warn("exception registering crawler : {}", std::string(exception.getDescription()));
        });

    spdlog::info("waiting for register");
    r.wait(ioContext.waitScope);

    spdlog::info("waiting for sigint");
    ioContext.unixEventPort.onSignal(SIGINT).wait(ioContext.waitScope);
  }

  return 0;
}

