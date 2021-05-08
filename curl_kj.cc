#include <unistd.h>

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
    spdlog::trace("socket {} created for fd {}", id, fd);
  }

  ~socket_context() {
    spdlog::warn("socket {} destructor", id);
  }

  bool check_finish() {
    if (!finish) {
      return false;
    }

    if (reading) {
      spdlog::info("socket {} finish but reading", id);
      return false;
    }

    if (writing) {
      spdlog::info("socket {} finish but writing", id);
      return false;
    }

    spdlog::info("socket {} finished", id);

    delete this;

    return true;
  }

  int fd;
  kj::UnixEventPort::FdObserver observer;
  int id;

  kj::Canceler read_canceler;
  kj::Canceler write_canceler;

  bool finish{false};

  bool read{false}, reading{false};
  bool write{false}, writing{false};
};

struct curl_kj::adaptor {
public:
  adaptor(kj::PromiseFulfiller<curl_response> &fulfiller,
          curl_kj *curl, CURL *easy,
          uint8_t *buf, size_t max)
      : fulfiller(fulfiller), curl(curl), easy(easy),
        buf(buf), buf_max(max)
  {
    spdlog::debug("adaptor starting"); curl->start_get(this, easy);
  }

  ~adaptor()
  {
    spdlog::debug("adaptor destructing");
    /*
    if (easy) {
      spdlog::warn("cancel");
      curl->cancel(easy);
    }
    */
  }

  void finish(curl_response &&r) {
    spdlog::debug("adaptor finishing");
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
  spdlog::trace("handle write {}", nmemb);

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
  spdlog::trace("got curl set timeout {}", timeout_ms);

  curl_kj *self = static_cast<curl_kj *>(userp);

  self->handle_timeout(timeout_ms);

  return 0;
}

static int handle_socket_c(CURL *easy, curl_socket_t s, int action, void *userp, void *socketp)
{
  spdlog::trace("got curl set socket {} {}", s, action);

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

            spdlog::debug("timeout");

            int running_handles;
            curl_multi_socket_action(multi_handle, CURL_SOCKET_TIMEOUT, 0,
                                     &running_handles);

            check_multi_info();

          },
          [] (auto e) {}
        ));
}

// observers must only be called when there is no data.
// ie: check before somehow
// This may not matter / curl might be handling it for us.

void curl_kj::setup_read(socket_context *context)
{
  /*
  uint8_t buf;
  int r = recv(context->fd, &buf, 1, MSG_PEEK|MSG_DONTWAIT);
  spdlog::debug("socket {} / {} read  {} - {}", context->id, context->fd, r, errno);
*/

  kj::Promise<void> p(kj::READY_NOW);
/*
  if (r == -1 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
    spdlog::debug("socket {} / {} read would block", context->id, context->fd);
*/
    p = context->observer.whenBecomesReadable();
/*
  } else {
    spdlog::debug("socket {} / {} read not blocked {} - {}", context->id, context->fd, r, errno);
  }
*/

  context->reading = true;
  tasks.add(context->read_canceler.wrap(kj::mv(p)).then(
        [this, context] () {
          spdlog::debug("socket {} / {} read done", context->id, context->fd);
          context->read_canceler.release();

          context->reading = false;

          if (context->check_finish()) {
            return;
          }

          if (context->read) {
            spdlog::debug("socket {} / {} continue reading", context->id, context->fd);
            setup_read(context);
          }

          int running_handles;
          curl_multi_socket_action(multi_handle, context->fd, CURL_CSELECT_IN,
                     &running_handles);

          check_multi_info();
        },
        [this, context] (auto exception) {
          spdlog::warn("socket {} read exception: {}", context->id, std::string(exception.getDescription()));
          context->read_canceler.release();

          context->reading = false;

          if (!context->finish) {
            int running_handles;
            curl_multi_socket_action(multi_handle, context->fd, CURL_CSELECT_ERR,
                         &running_handles);

            check_multi_info();
          }
        }));
}

void curl_kj::setup_write(socket_context *context)
{
  //uint8_t buf;
  //int r = send(context->fd, &buf, 1, MSG_PEEK|MSG_DONTWAIT);
  //spdlog::debug("socket {} / {} read  {} - {}", context->id, context->fd, r, errno);

  /*
  uint8_t buf;
  int r = write(context->fd, &buf, 0);

  spdlog::debug("socket {} / {} write state {} - {}", context->id, context->fd, r, errno);
*/

  kj::Promise<void> p(kj::READY_NOW);

  //if (r == -1 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
//    spdlog::debug("socket {} / {} write would block", context->id, context->fd);

    p = context->observer.whenBecomesWritable();
/*
 } else {
    spdlog::debug("socket {} / {} write not blocked {} - {}", context->id, context->fd, r, errno);
  }
*/

  context->writing = true;
  tasks.add(context->write_canceler.wrap(kj::mv(p)).then(
        [this, context] () {
          spdlog::debug("socket {} write done", context->id);
          context->write_canceler.release();

          spdlog::debug("socket {} / {} write done and handled", context->id, context->fd);

          context->writing = false;

          if (context->check_finish()) {
            return;
          }

          if (context->write) {
            setup_write(context);
          }

          int running_handles;
          curl_multi_socket_action(multi_handle, context->fd, CURL_CSELECT_OUT,
                     &running_handles);

          check_multi_info();
       },
        [this, context] (auto exception) {
          spdlog::warn("socket {} write exception: {}", context->id, std::string(exception.getDescription()));
          context->write_canceler.release();

          context->writing = false;

          if (!context->finish) {
            int running_handles;
            curl_multi_socket_action(multi_handle, context->fd, CURL_CSELECT_ERR,
                         &running_handles);

            check_multi_info();
          }
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
        spdlog::debug("get context from socketp {}", (int) s);
        context = static_cast<socket_context *>(socketp);

      } else {
        spdlog::debug("make new context for socketp {}", (int) s);

        try {
          context = new socket_context(io_context.unixEventPort, (int) s);
        } catch (const std::exception& e) {
          spdlog::warn("socket context create failed: {}", e.what());

          tasks.add(kj::Promise<void>(kj::READY_NOW).then(
                [this, s] () {
                  spdlog::warn("socket context create failed so give error to curl");
                  int running_handles;
                  curl_multi_socket_action(multi_handle, s, CURL_CSELECT_ERR, &running_handles);

                  check_multi_info();
                }));

          return;
        }

        spdlog::debug("socket {} / {} setup", context->id, context->fd);
      }

      spdlog::debug("socket {} / {} set {}", context->id, context->fd, action);

      curl_multi_assign(multi_handle, s, (void *) context);

      context->read = action & CURL_POLL_IN;
      context->write = action & CURL_POLL_OUT;

      if (context->read && !context->reading) {
        spdlog::debug("socket {} / {} start reading", context->id, context->fd);
        setup_read(context);
      }

      if (context->write && !context->writing) {
        spdlog::debug("socket {} / {} start writing", context->id, context->fd);
        setup_write(context);
      }

      if (action == CURL_POLL_NONE && (context->reading || context->writing)) {
        spdlog::debug("socket {} / {} cancel ops", context->id, context->fd);
        context->read_canceler.cancel("stop");
        context->write_canceler.cancel("stop");
      }

      break;

    case CURL_POLL_REMOVE:
      spdlog::warn("remove fd {}", (int) s);

      if (socketp) {
        socket_context *context = static_cast<socket_context *>(socketp);

        spdlog::debug("socket {} / {} finish", context->id, context->fd);

        context->finish = true;

        if (context->reading || context->writing) {
          spdlog::debug("socket {} / {} cancel and finish", context->id, context->fd);
          context->read_canceler.cancel("finished");
          context->write_canceler.cancel("finished");
        }

        context->check_finish();

        curl_multi_assign(multi_handle, s, nullptr);

      } else {
        spdlog::warn("bad socketp for remove fd {}", (int) s);
      }

      break;

    default:
      throw std::runtime_error(fmt::format("curl bad action {}", action));
      break;
  }
}

