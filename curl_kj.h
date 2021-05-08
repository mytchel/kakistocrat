#ifndef CURL_KJ_H
#define CURL_KJ_H

#include <chrono>

#include <curl/curl.h>

#include <kj/debug.h>
#include <kj/async.h>
#include <kj/async-io.h>
#include <kj/async-unix.h>
#include <kj/timer.h>

struct curl_response {
  bool success;
  int http_code;
  std::string done_url;
  time_t last_modified;
  size_t size;
};

class curl_kj : public kj::TaskSet::ErrorHandler {
public:
  struct adaptor;
  struct socket_context;

  curl_kj(kj::AsyncIoContext &io_context, size_t max_op = 100);

  ~curl_kj();

  kj::Promise<curl_response> add(const std::string &url,
      uint8_t *buf, size_t buf_max,
      time_t last_accessed = 0);

  void handle_socket(curl_socket_t s, int action, void *socketp);
  void handle_timeout(long timeout_ms);

private:
  void check_multi_info();

  void on_timeout();

  void cancel(CURL *curl_handle);
  void start_get(adaptor *, CURL *);

  void setup_read(socket_context *context);
  void setup_write(socket_context *context);

  void taskFailed(kj::Exception&& exception) override {
    spdlog::warn("curl task failed: {}", std::string(exception.getDescription()));
    kj::throwFatalException(kj::mv(exception));
  }

  kj::TaskSet tasks;
  kj::AsyncIoContext &io_context;

  kj::Timer &timer;
  kj::Canceler timer_canceler;

  CURLM *multi_handle;
};

#endif

