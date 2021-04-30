#include "spdlog/spdlog.h"

#include "kj/time.h"
#include "kj/exception.h"

#include "util.h"

#include "curl_kj.h"

static int socket_id = 0;

struct curl_kj::socket_context {
  socket_context(kj::UnixEventPort &event_port, int fd)
    : fd(fd), observer(event_port, fd,
                kj::UnixEventPort::FdObserver::OBSERVE_READ_WRITE),
    id(socket_id++)
  {
    spdlog::info("socket {} create", id);
  }

  ~socket_context() {
    spdlog::warn("socket {} destructor", id);
  }

  int fd;
  kj::UnixEventPort::FdObserver observer;
  int id;

  kj::Canceler read_canceler;
  kj::Canceler write_canceler;

  bool finish{false};
  bool read{false};
  bool write{false};
};

struct curl_kj::adaptor {
public:
  adaptor(kj::PromiseFulfiller<curl_response> &fulfiller,
          curl_kj *curl, CURL *easy,
          uint8_t *buf, size_t max)
      : fulfiller(fulfiller), curl(curl), easy(easy),
        buf(buf), buf_max(max)
  {
    spdlog::info("adaptor starting"); curl->start_get(this, easy);
  }

  ~adaptor()
  {
    spdlog::info("adaptor destructing");
    /*
    if (easy) {
      spdlog::warn("cancel");
      curl->cancel(easy);
    }
    */
  }

  void finish(curl_response &&r) {
    spdlog::info("adaptor finishing");
    easy = nullptr;
    buf_max = 0;
    fulfiller.fulfill(kj::mv(r));
  }

  kj::PromiseFulfiller<curl_response> &fulfiller;
  curl_kj *curl;
  std::string url;

  CURL *easy;

  uint8_t *buf;
  size_t buf_max;
  size_t size{0};

  time_t last_modified;
};

size_t handle_header_write_c(char *buffer, size_t size, size_t nitems, void *userp) {
  curl_kj::adaptor *adaptor = static_cast<curl_kj::adaptor *>(userp);
  if (adaptor == nullptr) {
    return 0;
  }

  buffer[nitems*size] = 0;

  if (strstr(buffer, "content-type:")) {
    if (strstr(buffer, "text/html") == NULL &&
        strstr(buffer, "text/plain") == NULL) {
      //return 0;
    }

  } else if (strstr(buffer, "Last-Modified: ")) {
    char *s = buffer + strlen("Last-Modified: ");

    if (strlen(s) > 25) {
      tm tm;
      strptime(s, "%a, %d %b %Y %H:%M:%S", &tm);

      adaptor->last_modified = mktime(&tm);
    }
  }

  return nitems * size;
}
size_t handle_buffer_write_c(void *contents, size_t sz, size_t nmemb, void *userp)
{
  spdlog::debug("handle write {}", nmemb);

  curl_kj::adaptor *adaptor = static_cast<curl_kj::adaptor *>(userp);
  if (adaptor == nullptr) {
    return 0;
  }

  size_t realsize = sz * nmemb;

  if (adaptor->buf_max < adaptor->size + realsize) {
    return 0;
  }

  memcpy(&(adaptor->buf[adaptor->size]), contents, realsize);
  adaptor->size += realsize;

  return realsize;
}

static int start_timeout_c(CURLM *multi, long timeout_ms, void *userp)
{
  spdlog::debug("got curl set timeout {}", timeout_ms);

  curl_kj *self = static_cast<curl_kj *>(userp);

  self->handle_timeout(timeout_ms);

  return 0;
}

static int handle_socket_c(CURL *easy, curl_socket_t s, int action, void *userp, void *socketp)
{
  spdlog::debug("got curl set socket {} {}", s, action);

  curl_kj *self = static_cast<curl_kj *>(userp);

  self->handle_socket(s, action, socketp);

  return 0;
}

curl_kj::curl_kj(kj::AsyncIoContext &io_context, size_t max_op)
  : tasks(*this), io_context(io_context),
    timer(io_context.provider->getTimer())
{
  multi_handle = curl_multi_init();

  curl_multi_setopt(multi_handle, CURLMOPT_MAX_TOTAL_CONNECTIONS, max_op);
  curl_multi_setopt(multi_handle, CURLMOPT_PIPELINING, CURLPIPE_MULTIPLEX);

  curl_multi_setopt(multi_handle, CURLMOPT_SOCKETFUNCTION, handle_socket_c);
  curl_multi_setopt(multi_handle, CURLMOPT_SOCKETDATA, this);
  curl_multi_setopt(multi_handle, CURLMOPT_TIMERFUNCTION, start_timeout_c);
  curl_multi_setopt(multi_handle, CURLMOPT_TIMERDATA, this);
}

curl_kj::~curl_kj()
{
  curl_multi_cleanup(multi_handle);
}

void curl_kj::cancel(CURL *curl_handle)
{
  curl_easy_setopt(curl_handle, CURLOPT_PRIVATE, nullptr);
  curl_easy_setopt(curl_handle, CURLOPT_WRITEDATA, nullptr);
  curl_easy_setopt(curl_handle, CURLOPT_HEADERDATA, nullptr);
}

void curl_kj::start_get(adaptor *adaptor, CURL *easy)
{
  spdlog::info("set data's");

  curl_easy_setopt(easy, CURLOPT_PRIVATE, adaptor);
  curl_easy_setopt(easy, CURLOPT_WRITEDATA, adaptor);
  curl_easy_setopt(easy, CURLOPT_HEADERDATA, adaptor);

  spdlog::info("add to multi");
  curl_multi_add_handle(multi_handle, easy);
}

kj::Promise<curl_response> curl_kj::add(const std::string &url,
      uint8_t *buf, size_t buf_max,
      time_t last_accessed)
{
  //  	Accept: text/html
  //  	If-Unmodified-Since: Sat, 29 Oct 1994 19:43:31 GMT

  CURL *easy = curl_easy_init();

  curl_easy_setopt(easy, CURLOPT_USERAGENT, "crawlycrawler");

  curl_easy_setopt(easy, CURLOPT_WRITEFUNCTION, handle_buffer_write_c);

  curl_easy_setopt(easy, CURLOPT_HEADERFUNCTION, handle_header_write_c);

  curl_easy_setopt(easy, CURLOPT_TIMEOUT, 60L);
  curl_easy_setopt(easy, CURLOPT_CONNECTTIMEOUT, 30L);

  curl_easy_setopt(easy, CURLOPT_FOLLOWLOCATION, 1);

  curl_easy_setopt(easy, CURLOPT_HTTP_VERSION, CURL_HTTP_VERSION_2TLS);

  char url_c[util::max_url_len];
  strncpy(url_c, url.c_str(), sizeof(url_c));

  curl_easy_setopt(easy, CURLOPT_URL, url_c);

  spdlog::info("create promise");

  return kj::newAdaptedPromise<curl_response, adaptor>(this, easy, buf, buf_max);
}

void curl_kj::check_multi_info()
{
  int pending;
  CURLMsg *m;
  while ((m = curl_multi_info_read(multi_handle, &pending))) {
    if (m->msg == CURLMSG_DONE) {
      CURL *easy_handle = m->easy_handle;
      CURLcode res = m->data.result;

      char *done_url;
      adaptor *adaptor;
      long res_status;

      curl_easy_getinfo(easy_handle, CURLINFO_EFFECTIVE_URL, &done_url);
      curl_easy_getinfo(easy_handle, CURLINFO_PRIVATE, &adaptor);
      curl_easy_getinfo(easy_handle, CURLINFO_RESPONSE_CODE, &res_status);

      spdlog::warn("{} DONE", done_url);

      if (adaptor) {
        bool ret = res == CURLE_OK;

        curl_response response{
              res == CURLE_OK,
              (int) res_status,
              std::string(done_url),
              adaptor->last_modified,
              adaptor->size
        };

        adaptor->finish(std::move(response));
      }

      curl_multi_remove_handle(multi_handle, easy_handle);
      curl_easy_cleanup(easy_handle);
    }
  }
}

void curl_kj::handle_timeout(long timeout_ms)
{
  timer_canceler.cancel("canceled");

  if (timeout_ms < 0) {
    return;
  }

  auto timeout = timeout_ms * kj::MILLISECONDS;

  tasks.add(timer_canceler.wrap(
        timer.afterDelay(timeout)).then(
          [this] () {
            timer_canceler.release();

            int running_handles;
            curl_multi_socket_action(multi_handle, CURL_SOCKET_TIMEOUT, 0,
                                     &running_handles);

            check_multi_info();

          },
          [] (auto e) {
            spdlog::info("timeout canceled");
          }
        ));
}

static bool context_check_finish(curl_kj::socket_context *context)
{
  if (context->finish) {
    bool done = true;

    if (!context->read_canceler.isEmpty()) {
      spdlog::info("socktet {} cancel read", context->id);

      context->read_canceler.cancel("finished");
      done = false;
    }

    if (!context->write_canceler.isEmpty()) {
      spdlog::info("socktet {} cancel write", context->id);

      context->write_canceler.cancel("finished");
      done = false;
    }

    if (done) {
      spdlog::info("socket {} finish and no read or write", context->id);
      delete context;
    }

    return true;

  } else {
    return false;
  }
}

// observers must only be called when there is no data.
// ie: check before somehow
// This may not matter / curl might be handling it for us.

void curl_kj::setup_read(socket_context *context)
{
  spdlog::info("socket {} read", context->id);

  tasks.add(context->read_canceler.wrap(
        context->observer.whenBecomesReadable()).then(
        [this, context] () {
          spdlog::info("socket {} read done", context->id);

          context->read_canceler.release();

          int running_handles;
          curl_multi_socket_action(multi_handle, context->fd, CURL_CSELECT_IN,
                     &running_handles);

          check_multi_info();

          if (context->read) {
            setup_read(context);
          }
        },
        [this, context] (auto exception) {
          spdlog::warn("socket {} read exception: {}", context->id, std::string(exception.getDescription()));
          context->read_canceler.release();
          context->read = false;
          context_check_finish(context);
        }));
}

void curl_kj::setup_write(socket_context *context)
{
  spdlog::info("socket {} write", context->id);

  tasks.add(context->write_canceler.wrap(
        context->observer.whenBecomesWritable()).then(
        [this, context] () {
          spdlog::info("socket {} write done", context->id);

          context->write_canceler.release();

          int running_handles;
          curl_multi_socket_action(multi_handle, context->fd, CURL_CSELECT_OUT,
                     &running_handles);

          check_multi_info();

          if (context->write) {
            setup_write(context);
          }
        },
        [this, context] (auto exception) {
          spdlog::warn("socket {} write exception: {}", context->id, std::string(exception.getDescription()));
          context->write_canceler.release();
          context->write = false;
          context_check_finish(context);
        }));
}

void curl_kj::handle_socket(curl_socket_t s, int action, void *socketp)
{
  socket_context *context;

  switch (action) {
    case CURL_POLL_IN:
    case CURL_POLL_OUT:
    case CURL_POLL_INOUT:

      if (socketp) {
        spdlog::debug("get context from socketp");
        context = static_cast<socket_context *>(socketp);

      } else {
        spdlog::debug("make new context for socketp");

        context = new socket_context(io_context.unixEventPort, (int) s);
      }

      curl_multi_assign(multi_handle, s, (void *) context);

      if (action & CURL_POLL_IN) {
        spdlog::debug("socket read");

        context->read = true;
        if (context->read_canceler.isEmpty()) {
          spdlog::warn("socket {} start reading", context->id);
          setup_read(context);
        }
      } else {
        context->read = false;
      }

      if (action & CURL_POLL_OUT) {
        spdlog::debug("socket write");

        context->write = true;
        if (context->write_canceler.isEmpty()) {
          spdlog::warn("socket {} start writeing", context->id);
          setup_write(context);
        }
      } else {
        context->write = false;
      }

      break;

    case CURL_POLL_REMOVE:
      spdlog::warn("remove socket {}", (int) s);

      if (socketp) {
        socket_context *context = static_cast<socket_context *>(socketp);

        spdlog::warn("socket {} set finished", context->id);
        context->finish = true;
        context->read = false;
        context->write = false;

        context_check_finish(context);

        curl_multi_assign(multi_handle, s, nullptr);
      }

      break;

    default:
      throw std::runtime_error(fmt::format("curl bad action {}", action));
      break;
  }
}

