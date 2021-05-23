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

#include "capnp_server.h"

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

Server::Server(kj::AsyncIoContext &io_context, const std::string &bind, capnp::Capability::Client mainInterface)
    : tasks(*this), mainInterface(kj::mv(mainInterface))
{
  capnp::ReaderOptions readerOpts;

  spdlog::info("server setup for {}", bind);

  tasks.add(io_context.provider->getNetwork().parseAddress(bind)
      .then([this, readerOpts](kj::Own<kj::NetworkAddress>&& addr) {

    //spdlog::info("server ready: {}", std::string(addr->toString()));
    spdlog::info("server ready");
    auto listener = addr->listen();
    acceptLoop(kj::mv(listener), readerOpts);
  }));
}

void Server::acceptLoop(kj::Own<kj::ConnectionReceiver>&& listener, capnp::ReaderOptions readerOpts) {
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

