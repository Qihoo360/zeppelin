#include <glog/logging.h>
#include "zp_meta_dispatch_thread.h"

#include "zp_meta_client_conn.h"
#include "zp_meta_server.h"

extern ZPMetaServer* g_zp_meta_server;

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
    slash::RWLock(&(((ZPDataWorkerThread**)worker_thread())[i]->rwlock_), false);
    server_querynum += ((ZPDataWorkerThread**)worker_thread())[i]->thread_querynum();
    server_current_qps += ((ZPDataWorkerThread**)worker_thread())[i]->last_sec_thread_querynum();
  }
  LOG(INFO) << "ClientNum: " << ClientNum() << " ServerQueryNum: " << server_querynum << " ServerCurrentQps: " << server_current_qps;
  
  // Check alive
  g_zp_meta_server->CheckNodeAlive();
}

int ZPDataDispatchThread::ClientNum() {
  int num = 0;
  for (int i = 0; i < work_num(); i++) {
    num += ((ZPDataWorkerThread**)worker_thread())[i]->ThreadClientNum();
  }
  return num;
}
