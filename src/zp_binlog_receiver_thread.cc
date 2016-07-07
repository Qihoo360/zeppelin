#include "zp_binlog_receiver_thread.h"

#include <glog/logging.h>
//#include "zp_data_server.h"
#include "zp_command.h"

//extern ZPDataServer* zp_data_server;

ZPBinlogReceiverThread::ZPBinlogReceiverThread(int port, int cron_interval)
  : HolyThread::HolyThread(port, cron_interval),
    thread_querynum_(0),
    last_thread_querynum_(0),
    last_time_us_(slash::NowMicros()),
    last_sec_thread_querynum_(0),
    serial_(0) {
  cmds_.reserve(300);
  InitClientCmdTable(&cmds_);

  DLOG(INFO) << "BinlogReceiver thread start at port:" << port;
}

ZPBinlogReceiverThread::~ZPBinlogReceiverThread() {
  DestoryCmdTable(cmds_);
  LOG(INFO) << "BinlogReceiver thread " << thread_id() << " exit!!!";
}

bool ZPBinlogReceiverThread::AccessHandle(std::string& ip) {
//  if (ip == "127.0.0.1") {
//    ip = zp_data_server->host();
//  }
//
//  if (ThreadClientNum() != 0 || !zp_data_server->ShouldAccessConnAsMaster(ip)) {
//    LOG(WARNING) << "BinlogReceiverThread AccessHandle failed: " << ip;
//    return false;
//  }
//
  //zp_data_server->PlusMasterConnection();
  return true;
}

void ZPBinlogReceiverThread::CronHandle() {
  DLOG(INFO) << "ZPBinlogReceiverThread, CronTask---";
  ResetLastSecQuerynum();
  {
    WorkerCronTask t;
    slash::MutexLock l(&mutex_);
    while (!cron_tasks_.empty()) {
      t = cron_tasks_.front();
      cron_tasks_.pop();
      mutex_.Unlock();
      DLOG(INFO) << "ZPBinlogReceiverThread, Got a WorkerCronTask";
      switch (t.task) {
        case TASK_KILL:
          break;
        case TASK_KILLALL:
          KillAll();
          break;
      }
      mutex_.Lock();
    }
  }
}

void ZPBinlogReceiverThread::KillBinlogSender() {
  AddCronTask(WorkerCronTask{TASK_KILLALL, ""});
}

void ZPBinlogReceiverThread::AddCronTask(WorkerCronTask task) {
  slash::MutexLock l(&mutex_);
  cron_tasks_.push(task);
}

void ZPBinlogReceiverThread::KillAll() {
  {
    slash::RWLock l(&rwlock_, true);
    std::map<int, void*>::iterator iter = conns_.begin();
    while (iter != conns_.end()) {
      LOG(INFO) << "==========Kill Sender Conn==============";
      close(iter->first);
      delete(static_cast<ZPSyncConn*>(iter->second));
      iter = conns_.erase(iter);
    }
  }
  //zp_data_server->MinusMasterConnection();
}
