#include "zp_metacmd_worker_thread.h"

#include <glog/logging.h>
#include "zp_metacmd_conn.h"
#include "zp_data_server.h"

#include "slash_mutex.h"

extern ZPDataServer* zp_data_server;

ZPMetacmdWorkerThread::ZPMetacmdWorkerThread(int port, int cron_interval) :
  HolyThread::HolyThread(port, cron_interval) {
  InitMetaCmdTable(&cmds_);
}

ZPMetacmdWorkerThread::~ZPMetacmdWorkerThread() {
  // may be redunct
  should_exit_ = true;
  pthread_join(thread_id(), NULL);
  DestoryCmdTable(cmds_);
  LOG(INFO) << "ZPMetacmdWorker thread " << thread_id() << " exit!!!";
}

void ZPMetacmdWorkerThread::CronHandle() {
  {
    WorkerCronTask t;
    slash::MutexLock l(&mutex_);
    while (!cron_tasks_.empty()) {
      t = cron_tasks_.front();
      cron_tasks_.pop();
      mutex_.Unlock();
      DLOG(INFO) << "ZPMetacmdReceiverThread, Got a WorkerCronTask";
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

void ZPMetacmdWorkerThread::KillMetacmdConn() {
  AddCronTask(WorkerCronTask{TASK_KILLALL, ""});
}

void ZPMetacmdWorkerThread::AddCronTask(WorkerCronTask task) {
  slash::MutexLock l(&mutex_);
  cron_tasks_.push(task);
}

void ZPMetacmdWorkerThread::KillAll() {

  slash::RWLock l(&rwlock_, true);
  std::map<int, void*>::iterator iter = conns_.begin();
  while (iter != conns_.end()) {
    LOG(INFO) << "==========Kill Metacmd Conn==============";
    close(iter->first);
    delete(static_cast<ZPMetacmdConn*>(iter->second));
    iter = conns_.erase(iter);
  }
  zp_data_server->MinusMetaServerConns();

}

bool ZPMetacmdWorkerThread::AccessHandle(std::string& ip) {
  if (ip == "127.0.0.1") {
    ip = zp_data_server->local_ip();
  }

  if (conns_.size() == 0) {
  //if (ip == zp_data_server->meta_ip() && conns_.size() == 0) {
    zp_data_server->PlusMetaServerConns();
    return true;
  }
  LOG(WARNING) << "Deny connection from " << ip << " current conns size: " << conns_.size();
  return false;
}

