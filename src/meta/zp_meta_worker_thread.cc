#include <glog/logging.h>

#include "zp_command.h"
#include "zp_meta_worker_thread.h"

ZPMetaWorkerThread::ZPMetaWorkerThread(int cron_interval)
  : WorkerThread::WorkerThread(cron_interval),
    thread_querynum_(0),
    last_thread_querynum_(0),
    last_time_us_(slash::NowMicros()),
    last_sec_thread_querynum_(0) {
}

ZPMetaWorkerThread::~ZPMetaWorkerThread() {
  should_exit_ = true;
  pthread_join(thread_id(), NULL);
  LOG(INFO) << "A meta worker thread" << thread_id() << " exit!!!";
}

int ZPMetaWorkerThread::ThreadClientNum() {
  slash::RWLock l(&rwlock_, false);
  return conns_.size();
}

