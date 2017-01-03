#include "zp_binlog_receiver_thread.h"

#include <glog/logging.h>
#include "zp_data_server.h"

extern ZPDataServer* zp_data_server;

ZPBinlogReceiverThread::ZPBinlogReceiverThread(int port, int cron_interval)
  : HolyThread::HolyThread(port, cron_interval),
    last_time_us_(slash::NowMicros()) {
  DLOG(INFO) << "BinlogReceiver thread start at port:" << port;
}

ZPBinlogReceiverThread::~ZPBinlogReceiverThread() {
  should_exit_ = true;
  pthread_join(thread_id(), NULL);

  slash::MutexLock l(&stat_mu_);
  for (auto& item : table_stats_) {
    delete item.second;
  }

  LOG(INFO) << "BinlogReceiver thread " << thread_id() << " exit!!!";
}

void ZPBinlogReceiverThread::PlusStat(const std::string &table) {
  //DLOG(INFO) << "Plus table (" << table << ")";
  slash::MutexLock l(&stat_mu_);
  if (table.empty()) {
    other_stat_.querys++;
  } else {
    auto it = table_stats_.find(table);
    if (it == table_stats_.end()) {
      Statistic* pstat = new Statistic;
      pstat->table_name = table;
      pstat->querys++;
      table_stats_[table] = pstat;
    } else {
      (it->second)->querys++;
    }
  }
}

void ZPBinlogReceiverThread::UpdateLastStat() {
  uint64_t cur_time_us = slash::NowMicros();
  slash::MutexLock l(&stat_mu_);
  // TODO anan debug;
  //DLOG(INFO) << "Worker Cron UpdateLastStat " << table_stats_.size() << " tables: -->";
  for (auto it = table_stats_.begin(); it != table_stats_.end(); it++) {
    auto stat = it->second;
    //stat->Dump();
    //DLOG(INFO) << "cur_time_us=" << cur_time_us << ", last_time_us_=" << last_time_us_;
    stat->last_qps = ((stat->querys - stat->last_querys) * 1000000 / (cur_time_us - last_time_us_ + 1));
    stat->last_querys = stat->querys;
    //stat->Dump();
    //DLOG(INFO) << "---";
  }
  other_stat_.last_qps = ((other_stat_.querys - other_stat_.last_querys) * 1000000 / (cur_time_us - last_time_us_ + 1));
  other_stat_.last_querys = other_stat_.querys;
  last_time_us_ = cur_time_us;
  //DLOG(INFO) << "Worker Cron other_stat_ -->";
  //other_stat_.Dump();
  //DLOG(INFO) << "Worker Cron UpdateLastStat<--";
}

bool ZPBinlogReceiverThread::GetStat(const std::string &table, Statistic& stat) {
  slash::MutexLock l(&stat_mu_);
  stat.Reset();
  auto it = table_stats_.find(table);
  if (it == table_stats_.end()) {
    return false;
  }
  stat = *(it->second);
  return true;
}

bool ZPBinlogReceiverThread::GetTotalStat(Statistic& stat) {
  stat.Reset();
  slash::MutexLock l(&stat_mu_);
  for (auto it = table_stats_.begin(); it != table_stats_.end(); it++) {
    stat.Add(*(it->second));
  }
  stat.Add(other_stat_);
  return true;
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
  //DLOG(INFO) << "ZPBinlogReceiverThread, CronTask---";
  UpdateLastStat();
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
