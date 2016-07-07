#include "zp_meta_thread.h"

#include <glog/logging.h>
#include "zp_master_server.h"

#include "slash_mutex.h"

extern ZPMasterServer* zp_master_server;

ZPMetaThread::~ZPMetaThread() {
  should_exit_ = true;
  pthread_join(thread_id(), NULL);
  delete floyd_;
  DLOG(INFO) << " Meta thread " << pthread_self() << " exit!!!";
}

void* ZPMetaThread::ThreadMain() {
  int connect_retry_times = 0;
  struct timeval last_interaction;
  struct timeval now;
  gettimeofday(&now, NULL);
  last_interaction = now;

  pink::Status s;

  while (!should_exit_) {

    printf ("\n=====Test Write==========\n");

    std::string key = "test_key";
    std::string value = "test_value";

    pink::Status result = floyd_->Write(key, value);
    if (result.ok()) {
      printf ("Write ok\n");
    } else {
      printf ("Write failed, %s\n", result.ToString().c_str());
    }

    printf ("\n=====Test Read==========\n");

    result = floyd_->Read(key, &value);
    if (result.ok()) {
      printf ("read ok, value is %s\n", value.c_str());
    } else {
      printf ("Read failed, %s\n", result.ToString().c_str());
    }

    sleep(20);
  }
  return NULL;
}

