#ifndef CURL_KJ_H
#define CURL_KJ_H

#include <chrono>

#include <curl/curl.h>

#include <kj/debug.h>
#include <kj/async.h>
#include <kj/async-io.h>
#include <kj/async-unix.h>
#include <kj/timer.h>

class test_adaptor;

class curl_kj : public kj::TaskSet::ErrorHandler {
public:

  curl_kj(kj::AsyncIoContext &io_context, size_t max_op = 100);

  ~curl_kj();

  kj::Promise<bool> add(const std::string &url);

  void check_multi_info();

  void on_timeout();

  void cancel(CURL *curl_handle);
  CURL* start_get(const std::string &url, test_adaptor *);

  void taskFailed(kj::Exception&& exception) override {
    spdlog::warn("task failed: {}", std::string(exception.getDescription()));
    kj::throwFatalException(kj::mv(exception));
  }

  kj::TaskSet tasks;
  kj::AsyncIoContext &io_context;
  kj::Timer &timer;

  CURLM *multi_handle;
};

#endif

