#include "zp_data_dispatch_thread.h"

#include <glog/logging.h>

#include "zp_data_client_conn.h"
#include "zp_data_server.h"

extern ZPDataServer* zp_data_server;

ZPDataDispatchThread::ZPDataDispatchThread(int port, int work_num, ZPDataWorkerThread** worker_thread, int cron_interval)
  : DispatchThread::DispatchThread(port, work_num,
                                   reinterpret_cast<pink::WorkerThread<ZPDataClientConn>**>(worker_thread),
                                   cron_interval) {
}

ZPDataDispatchThread::~ZPDataDispatchThread() {
  LOG(INFO) << "Dispatch thread " << thread_id() << " exit!!!";
}

void ZPDataDispatchThread::CronHandle() {
  uint64_t server_querynum = 0;
  uint64_t server_current_qps = 0;
  for (int i = 0; i < work_num(); i++) {
    slash::RWLock(&(((ZPDataWorkerThread**)worker_thread())[i]->rwlock_), false);
    server_querynum += ((ZPDataWorkerThread**)worker_thread())[i]->thread_querynum();
    server_current_qps += ((ZPDataWorkerThread**)worker_thread())[i]->last_sec_thread_querynum();
  }

  //uint64_t meta_querynum = zp_data_server->zp_metacmd_worker_thread()->query_num();
  uint64_t sync_querynum = zp_data_server->zp_binlog_receiver_thread()->query_num();

  LOG(INFO) << "ClientNum: " << ClientNum() << " ClientQueryNum: " << server_querynum
      << " SyncCmdNum: " << sync_querynum
      << " ServerCurrentQps: " << server_current_qps;
  //zp_data_server->DumpPartitions();
      //<< " MetaQueryNum: " << meta_querynum;
}

int ZPDataDispatchThread::ClientNum() {
  int num = 0;
  for (int i = 0; i < work_num(); i++) {
    num += ((ZPDataWorkerThread**)worker_thread())[i]->ThreadClientNum();
  }
  return num;
}
