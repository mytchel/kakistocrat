#include "spdlog/spdlog.h"

#include "kj/time.h"

#include "util.h"

#include "curl_kj.h"

static int start_timeout(CURLM *multi, long timeout_ms, void *userp)
{
  spdlog::debug("got curl set timeout {}", timeout_ms);

  curl_kj *self = static_cast<curl_kj *>(userp);

  auto timeout = timeout_ms * kj::MILLISECONDS;

  self->tasks.add(self->timer.afterDelay(timeout).then(
        [self] () {
          self->on_timeout();
        }));

  return 0;
}

struct socket_context {
  socket_context(kj::UnixEventPort &event_port, int fd)
    : fd(fd), observer(event_port, fd, kj::UnixEventPort::FdObserver::OBSERVE_READ_WRITE)
  {}

  int fd;
  kj::UnixEventPort::FdObserver observer;

};

// observers must only be called when there is no data.
// ie: check before somehow
// This may not matter / curl might be handling it for us.

static void setup_read(curl_kj *self, socket_context *context)
{
  spdlog::info("set socket readable");
  self->tasks.add(context->observer.whenBecomesReadable().then(
        [self, context] () {
          int running_handles;
          spdlog::info("socket is readable");

          curl_multi_socket_action(self->multi_handle, context->fd, CURL_CSELECT_IN,
                     &running_handles);

          self->check_multi_info();

          setup_read(self, context);
        }));
}

static void setup_write(curl_kj *self, socket_context *context)
{
  spdlog::info("set socket writeable");
  self->tasks.add(context->observer.whenBecomesWritable().then(
        [self, context] () {
          int running_handles;
          spdlog::info("socket is writable");

          curl_multi_socket_action(self->multi_handle, context->fd, CURL_CSELECT_OUT,
                     &running_handles);

          self->check_multi_info();

          setup_write(self, context);
        }));
}

static int handle_socket(CURL *easy, curl_socket_t s, int action, void *userp, void *socketp)
{
  spdlog::debug("got curl set socket  {}", action);

  curl_kj *self = static_cast<curl_kj *>(userp);

  switch (action) {
    case CURL_POLL_IN:
    case CURL_POLL_OUT:
    case CURL_POLL_INOUT:
      spdlog::warn("should do something with socket {}", (int) s);

      socket_context *context;

      if (socketp) {
        spdlog::debug("get context from socketp");
        context = static_cast<socket_context *>(socketp);

      } else {
        spdlog::debug("make new context for socketp");

        context = new socket_context(self->io_context.unixEventPort, (int) s);

        curl_multi_assign(self->multi_handle, s, (void *) context);
      }

      if (action & CURL_POLL_IN) {
        setup_read(self, context);
      }

      if (action & CURL_POLL_OUT) {
        setup_write(self, context);
      }

      break;

    case CURL_POLL_REMOVE:
      spdlog::warn("should remove socket {}", (int) s);

      break;

    default:
      throw std::runtime_error(fmt::format("curl bad action {}", action));
      break;
  }

  return 0;
}

curl_kj::curl_kj(kj::AsyncIoContext &io_context, size_t max_op)
  : tasks(*this), io_context(io_context),
    timer(io_context.provider->getTimer())
{
  multi_handle = curl_multi_init();

  curl_multi_setopt(multi_handle, CURLMOPT_MAX_TOTAL_CONNECTIONS, max_op);
  curl_multi_setopt(multi_handle, CURLMOPT_PIPELINING, CURLPIPE_MULTIPLEX);

  curl_multi_setopt(multi_handle, CURLMOPT_SOCKETFUNCTION, handle_socket);
  curl_multi_setopt(multi_handle, CURLMOPT_SOCKETDATA, this);
  curl_multi_setopt(multi_handle, CURLMOPT_TIMERFUNCTION, start_timeout);
  curl_multi_setopt(multi_handle, CURLMOPT_TIMERDATA, this);
}

curl_kj::~curl_kj()
{
  curl_multi_cleanup(multi_handle);
}

class test_adaptor {
public:
  test_adaptor(kj::PromiseFulfiller<bool> &fulfiller, curl_kj *curl, const std::string &url)
      : fulfiller(fulfiller), curl(curl), url(url)
  {
    spdlog::info("start get {}", url);
    handle = curl->start_get(url, this);
  }

  ~test_adaptor()
  {
    if (handle) {
      spdlog::warn("cancel {}", url);
      curl->cancel(handle);
    }
  }

  void finish(bool r) {
    curl = nullptr;
    fulfiller.fulfill(kj::mv(r));
  }

  kj::PromiseFulfiller<bool> &fulfiller;
  curl_kj *curl;
  std::string url;

  CURL *handle;
};

void curl_kj::cancel(CURL *curl_handle)
{
  curl_easy_setopt(curl_handle, CURLOPT_PRIVATE, nullptr);
}

CURL* curl_kj::start_get(const std::string &url, test_adaptor *adaptor)
{
  CURL *curl_handle = curl_easy_init();

  curl_easy_setopt(curl_handle, CURLOPT_USERAGENT, "crawlycrawler");

  curl_easy_setopt(curl_handle, CURLOPT_PRIVATE, adaptor);

  /*
  curl_easy_setopt(curl_handle, CURLOPT_WRITEFUNCTION, curl_cb_buffer_write);
  curl_easy_setopt(curl_handle, CURLOPT_WRITEDATA, op);
  */

  curl_easy_setopt(curl_handle, CURLOPT_TIMEOUT, 60L);
  curl_easy_setopt(curl_handle, CURLOPT_CONNECTTIMEOUT, 30L);

  curl_easy_setopt(curl_handle, CURLOPT_FOLLOWLOCATION, 1);

  curl_easy_setopt(curl_handle, CURLOPT_HTTP_VERSION, CURL_HTTP_VERSION_2TLS);

  char url_c[util::max_url_len];
  strncpy(url_c, url.c_str(), sizeof(url_c));

  curl_easy_setopt(curl_handle, CURLOPT_URL, url_c);

  curl_multi_add_handle(multi_handle, curl_handle);

  return curl_handle;
}

kj::Promise<bool> curl_kj::add(const std::string &url)
{
  return kj::newAdaptedPromise<bool, test_adaptor>(this, url);
}

void curl_kj::check_multi_info()
{
  FILE *file;

  int pending;
  CURLMsg *m;
  while ((m = curl_multi_info_read(multi_handle, &pending))) {
    if (m->msg == CURLMSG_DONE) {
      CURL *easy_handle = m->easy_handle;
      CURLcode res = m->data.result;

      char *done_url;
      test_adaptor *adaptor;

      curl_easy_getinfo(easy_handle, CURLINFO_EFFECTIVE_URL, &done_url);
      curl_easy_getinfo(easy_handle, CURLINFO_PRIVATE, &adaptor);

      spdlog::warn("{} DONE", done_url);

      if (adaptor) {
        if (res == CURLE_OK) {
          adaptor->finish(true);
        } else {
          adaptor->finish(false);
        }
      }

      curl_multi_remove_handle(multi_handle, easy_handle);
      curl_easy_cleanup(easy_handle);
    }
  }
}

void curl_kj::on_timeout() {
  int running_handles;
  curl_multi_socket_action(multi_handle, CURL_SOCKET_TIMEOUT, 0,
                           &running_handles);

  check_multi_info();
}


