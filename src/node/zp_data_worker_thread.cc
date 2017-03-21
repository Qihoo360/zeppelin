#include "zp_data_worker_thread.h"

#include <glog/logging.h>

ZPDataWorkerThread::ZPDataWorkerThread(int cron_interval)
  : WorkerThread::WorkerThread(cron_interval),
    last_time_us_(slash::NowMicros()) {
      set_thread_name("ZPDataWorkerThread");
    }

ZPDataWorkerThread::~ZPDataWorkerThread() {
  should_exit_ = true;
  pthread_join(thread_id(), NULL);

  slash::MutexLock l(&stat_mu_);
  for (auto& item : table_stats_) {
    delete item.second;
  }

  LOG(INFO) << "A worker thread " << thread_id() << " exit!!!";
}

void ZPDataWorkerThread::PlusStat(const std::string &table) {
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

void ZPDataWorkerThread::UpdateLastStat() {
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

bool ZPDataWorkerThread::GetStat(const std::string &table, Statistic& stat) {
  slash::MutexLock l(&stat_mu_);
  stat.Reset();
  auto it = table_stats_.find(table);
  if (it == table_stats_.end()) {
    return false;
  }
  stat = *(it->second);
  // TODO anan debug
  //DLOG(INFO) << "Worker GetStat -->";
  //stat.Dump();
  return true;
}

bool ZPDataWorkerThread::GetTotalStat(Statistic& stat) {
  stat.Reset();
  slash::MutexLock l(&stat_mu_);
  for (auto it = table_stats_.begin(); it != table_stats_.end(); it++) {
    stat.Add(*(it->second));
  }
  stat.Add(other_stat_);
  // TODO anan debug
  //DLOG(INFO) << "Worker GetTotalStat -->";
  //stat.Dump();
  return true;
}

void ZPDataWorkerThread::CronHandle() {
  UpdateLastStat();

  {
    struct timeval now;
    gettimeofday(&now, NULL);
    slash::RWLock l(&rwlock_, false); // Use ReadLock to iterate the conns_
    std::map<int, void*>::iterator iter = conns_.begin();

    while (iter != conns_.end()) {

      // TODO simple 3s
      if (now.tv_sec - static_cast<ZPDataClientConn*>(iter->second)->last_interaction().tv_sec > 60) {
        LOG(INFO) << "Find Timeout Client: " << static_cast<ZPDataClientConn*>(iter->second)->ip_port();
        AddCronTask(WorkerCronTask{TASK_KILL, static_cast<ZPDataClientConn*>(iter->second)->ip_port()});
      }
      iter++;
    }
  }

  {
    slash::MutexLock l(&mutex_);
    while (!cron_tasks_.empty()) {
      WorkerCronTask t = cron_tasks_.front();
      cron_tasks_.pop();
      mutex_.Unlock();
      DLOG(INFO) << "ZPDataWorkerThread, Got a WorkerCronTask";
      switch (t.task) {
        case TASK_KILL:
          ClientKill(t.ip_port);
          break;
        case TASK_KILLALL:
          ClientKillAll();
          break;
      }
      mutex_.Lock();
    }
  }
}

bool ZPDataWorkerThread::ThreadClientKill(std::string ip_port) {

  if (ip_port == "") {
    AddCronTask(WorkerCronTask{TASK_KILLALL, ""});
  } else {
    if (!FindClient(ip_port)) {
      return false;
    }
    AddCronTask(WorkerCronTask{TASK_KILL, ip_port});
  }
  return true;
}

int ZPDataWorkerThread::ThreadClientNum() {
  slash::RWLock l(&rwlock_, false);
  return conns_.size();
}

void ZPDataWorkerThread::AddCronTask(WorkerCronTask task) {
  slash::MutexLock l(&mutex_);
  cron_tasks_.push(task);
}

bool ZPDataWorkerThread::FindClient(std::string ip_port) {
  slash::RWLock l(&rwlock_, false);
  std::map<int, void*>::iterator iter;
  for (iter = conns_.begin(); iter != conns_.end(); iter++) {
    if (static_cast<ZPDataClientConn*>(iter->second)->ip_port() == ip_port) {
      return true;
    }
  }
  return false;
}

void ZPDataWorkerThread::ClientKill(std::string ip_port) {
  slash::RWLock l(&rwlock_, true);
  std::map<int, void*>::iterator iter;
  for (iter = conns_.begin(); iter != conns_.end(); iter++) {
    if (static_cast<ZPDataClientConn*>(iter->second)->ip_port() != ip_port) {
      continue;
    }
    LOG(INFO) << "==========Kill Client==============";
    close(iter->first);
    delete(static_cast<ZPDataClientConn*>(iter->second));
    conns_.erase(iter);
    break;
  }
}

void ZPDataWorkerThread::ClientKillAll() {
  slash::RWLock l(&rwlock_, true);
  std::map<int, void*>::iterator iter = conns_.begin();
  while (iter != conns_.end()) {
    LOG(INFO) << "==========Kill Client==============";
    close(iter->first);
    delete(static_cast<ZPDataClientConn*>(iter->second));
    iter = conns_.erase(iter);
  }
}

//int64_t ZPDataWorkerThread::ThreadClientList(std::vector<ClientInfo> *clients) {
//  slash::RWLock l(&rwlock_, false);
//  if (clients != NULL) {
//    std::map<int, void*>::const_iterator iter = conns_.begin();
//    while (iter != conns_.end()) {
//      clients->push_back(ClientInfo{iter->first, reinterpret_cast<ZPDataClientConn*>(iter->second)->ip_port(), static_cast<int>((reinterpret_cast<ZPDataClientConn*>(iter->second)->last_interaction()).tv_sec)});
//      iter++;
//    }
//  }
//  return conns_.size();
//}
