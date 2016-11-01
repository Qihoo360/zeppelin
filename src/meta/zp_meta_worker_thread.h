#ifndef ZP_META_WORKER_THREAD_H
#define ZP_META_WORKER_THREAD_H

#include <unordered_map>

#include "env.h"
#include "slash_mutex.h"
#include "worker_thread.h"

#include "zp_util.h"
#include "zp_command.h"
#include "zp_meta_client_conn.h"

class ZPMetaClientConn;

class ZPMetaWorkerThread : public pink::WorkerThread<ZPMetaClientConn> {
 public:
  ZPMetaWorkerThread(int cron_interval = 0);
  virtual ~ZPMetaWorkerThread();

	int ThreadClientNum();

  uint64_t thread_querynum() {
    slash::RWLock(&rwlock_, false);
    return thread_querynum_;
  }

  uint64_t last_sec_thread_querynum() {
    slash::RWLock(&rwlock_, false);
    return last_sec_thread_querynum_;
  }

  void PlusThreadQuerynum() {
    slash::RWLock(&rwlock_, true);
    thread_querynum_++;
  }

  void ResetLastSecQuerynum() {
    uint64_t cur_time_us = slash::NowMicros();
    slash::RWLock l(&rwlock_, true);
    last_sec_thread_querynum_ = ((thread_querynum_ - last_thread_querynum_) * 1000000 / (cur_time_us - last_time_us_+1));
    last_thread_querynum_ = thread_querynum_;
    last_time_us_ = cur_time_us;
  }

 private:
  uint64_t thread_querynum_;
  uint64_t last_thread_querynum_;
  uint64_t last_time_us_;
  uint64_t last_sec_thread_querynum_;
};


#endif
