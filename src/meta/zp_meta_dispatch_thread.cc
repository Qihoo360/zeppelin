#include <glog/logging.h>
#include "zp_meta_dispatch_thread.h"

#include "zp_meta_server.h"
#include "zp_meta_client_conn.h"
#include "zp_meta_worker_thread.h"

extern ZPMetaServer* zp_meta_server;

ZPMetaDispatchThread::ZPMetaDispatchThread(int port, int work_num, ZPMetaWorkerThread** worker_thread, int cron_interval)
  : DispatchThread::DispatchThread(port, work_num,
                                   reinterpret_cast<pink::WorkerThread<ZPMetaClientConn>**>(worker_thread),
                                   cron_interval) {
}

ZPMetaDispatchThread::~ZPMetaDispatchThread() {
  LOG(INFO) << "Dispatch thread " << thread_id() << " exit!!!";
}

void ZPMetaDispatchThread::CronHandle() {
  // Calc ops
  uint64_t server_querynum = 0;
  uint64_t server_current_qps = 0;
  for (int i = 0; i < work_num(); i++) {
    slash::RWLock(&(((ZPMetaWorkerThread**)worker_thread())[i]->rwlock_), false);
    server_querynum += ((ZPMetaWorkerThread**)worker_thread())[i]->thread_querynum();
    server_current_qps += ((ZPMetaWorkerThread**)worker_thread())[i]->last_sec_thread_querynum();
  }
  LOG(INFO) << "ClientNum: " << ClientNum() << " ServerQueryNum: " << server_querynum << " ServerCurrentQps: " << server_current_qps;
  
  // Check alive
  zp_meta_server->CheckNodeAlive();
}

int ZPMetaDispatchThread::ClientNum() {
  int num = 0;
  for (int i = 0; i < work_num(); i++) {
    num += ((ZPMetaWorkerThread**)worker_thread())[i]->ThreadClientNum();
  }
  return num;
}
