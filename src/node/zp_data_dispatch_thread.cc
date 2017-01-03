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
  // Note: ServerCurrentQPS is the sum of client qps and sync qps;
  uint64_t server_querys = 0;
  uint64_t server_qps = 0;
  for (int i = 0; i < work_num(); i++) {
    Statistic stat;
    ((ZPDataWorkerThread**)worker_thread())[i]->GetTotalStat(stat);
    server_querys += stat.querys;
    server_qps += stat.last_qps;
  }

  Statistic stat;
  zp_data_server->zp_binlog_receiver_thread()->GetTotalStat(stat);
  uint64_t sync_querys = stat.querys;
  server_qps += stat.last_qps;

  LOG(INFO) << "ClientNum: " << ClientNum() << " ClientQueryNum: " << server_querys
      << " SyncCmdNum: " << sync_querys
      << " ServerCurrentQps: " << server_qps;
  zp_data_server->DumpTablePartitions();
  zp_data_server->DumpBinlogSendTask();
}

int ZPDataDispatchThread::ClientNum() {
  int num = 0;
  for (int i = 0; i < work_num(); i++) {
    num += ((ZPDataWorkerThread**)worker_thread())[i]->ThreadClientNum();
  }
  return num;
}
