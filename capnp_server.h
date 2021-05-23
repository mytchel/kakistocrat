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

#include "spdlog/spdlog.h"

#include <kj/debug.h>
#include <kj/async-io.h>
#include <kj/async-unix.h>
#include <kj/timer.h>
#include <kj/threadlocal.h>
#include <capnp/rpc-twoparty.h>
#include <capnp/capability.h>
#include <capnp/rpc.h>
#include <capnp/message.h>

class Server final : public kj::TaskSet::ErrorHandler {

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
  Server(kj::AsyncIoContext &io_context, const std::string &bind, capnp::Capability::Client mainInterface);

private:
  void acceptLoop(kj::Own<kj::ConnectionReceiver>&& listener, capnp::ReaderOptions readerOpts);

  void taskFailed(kj::Exception&& exception) override {
    spdlog::warn("task failed: {}", std::string(exception.getDescription()));
    kj::throwFatalException(kj::mv(exception));
  }
};

