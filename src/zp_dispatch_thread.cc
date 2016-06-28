#include "zp_dispatch_thread.h"

#include <glog/logging.h>

#include "zp_client_conn.h"
#include "zp_server.h"

extern ZPServer* g_zp_server;

ZPDispatchThread::ZPDispatchThread(int port, int work_num, ZPWorkerThread** ZP_worker_thread, int cron_interval)
  : DispatchThread::DispatchThread(port, work_num,
                                   reinterpret_cast<pink::WorkerThread<ZPClientConn>**>(ZP_worker_thread),
                                   cron_interval) {
}

ZPDispatchThread::~ZPDispatchThread() {
  LOG(INFO) << "Dispatch thread " << thread_id() << " exit!!!";
}

void ZPDispatchThread::CronHandle() {
  uint64_t server_querynum = 0;
  uint64_t server_current_qps = 0;
  for (int i = 0; i < work_num(); i++) {
    slash::RWLock(&(((ZPWorkerThread**)worker_thread())[i]->rwlock_), false);
    server_querynum += ((ZPWorkerThread**)worker_thread())[i]->thread_querynum();
    server_current_qps += ((ZPWorkerThread**)worker_thread())[i]->last_sec_thread_querynum();
  }

  LOG(INFO) << "ClientNum: " << ClientNum() << " ServerQueryNum: " << server_querynum << " ServerCurrentQps: " << server_current_qps;
}

int ZPDispatchThread::ClientNum() {
  int num = 0;
  for (int i = 0; i < work_num(); i++) {
    num += ((ZPWorkerThread**)worker_thread())[i]->ThreadClientNum();
  }
  return num;
}
