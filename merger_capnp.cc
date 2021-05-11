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
#include "config.h"
#include "index.h"

#include "indexer.capnp.h"

#include <kj/debug.h>
#include <kj/string.h>
#include <kj/async-io.h>
#include <kj/async-unix.h>

#include <capnp/rpc.h>
#include <capnp/rpc-twoparty.h>
#include <capnp/ez-rpc.h>
#include <capnp/message.h>
#include <iostream>

using nlohmann::json;

class MergerImpl final: public Merger::Server {
public:
  MergerImpl(const config &s)
    : settings(s)
  {
    buf_len = settings.merger.max_index_part_size;
    buf = (uint8_t *) malloc(buf_len);
    if (buf == NULL) {
      throw std::bad_alloc();
    }
  }

  ~MergerImpl() {
    if (buf) {
      free(buf);
    }
  }

  kj::Promise<void> merge(MergeContext context) override {
    spdlog::info("got merge request");

    auto params = context.getParams();

    std::string start = params.getStart();
    std::optional<std::string> end;
    if (params.hasEnd()) {
      end = params.getEnd();
    }

    std::list<std::string> part_paths;

    for (auto p: params.getIndexPartPaths()) {
      part_paths.emplace_back(p);
    }

    std::string w_p = params.getWOut();
    std::string p_p = params.getPOut();
    std::string t_p = params.getTOut();

    size_t htcap = settings.merger.htcap;

    search::index_part out_word(w_p, htcap, start, end);
    search::index_part out_pair(p_p, htcap, start, end);
    search::index_part out_trine(t_p, htcap, start, end);

    for (auto &index_path: part_paths) {
      spdlog::info("load {} for merging", index_path);

      search::index_info index(index_path);
      index.load();

      spdlog::info("index {} word part usage {} kb", start, out_word.usage() / 1024);
      spdlog::info("index {} pair part usage {} kb", start, out_pair.usage() / 1024);
      spdlog::info("index {} trine part usage {} kb", start, out_trine.usage() / 1024);

      for (auto &p: index.word_parts) {
        if ((!end || p.start < *end) && (!p.end || start < *p.end)) {
          search::index_part in(p.path, htcap, p.start, p.end);
          in.load();

          out_word.merge(in);
        }
      }

      for (auto &p: index.pair_parts) {
        if ((!end || p.start < *end) && (!p.end || start < *p.end)) {
          search::index_part in(p.path, htcap,  p.start, p.end);
          in.load();

          out_pair.merge(in);
        }
      }

      for (auto &p: index.trine_parts) {
        if ((!end || p.start < *end) && (!p.end || start < *p.end)) {
          search::index_part in(p.path, htcap,  p.start, p.end);
          in.load();

          out_trine.merge(in);
        }
      }
    }

    out_word.save(buf, buf_len);
    out_pair.save(buf, buf_len);
    out_trine.save(buf, buf_len);

    return kj::READY_NOW;
  }

  const config &settings;

  size_t buf_len;
  uint8_t *buf;
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

    Merger::Client merger = kj::heap<MergerImpl>(settings);

    spdlog::info("create request");
    auto request = master.registerMergerRequest();
    request.setMerger(merger);

    spdlog::info("send merger register");

    auto r = request.send().then(
        [] (auto result) {
          spdlog::info("merger registered");
        },
        [] (auto exception) {
          spdlog::warn("exception registering merger : {}", std::string(exception.getDescription()));
        });

    spdlog::info("waiting for register");
    r.wait(ioContext.waitScope);

    spdlog::info("waiting for sigint");
    ioContext.unixEventPort.onSignal(SIGINT).wait(ioContext.waitScope);
  }

  return 0;
}

