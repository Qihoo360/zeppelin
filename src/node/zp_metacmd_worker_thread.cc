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
  LOG(INFO) << "ZPMetacmdWorker thread " << thread_id() << " exit!!!";
}

void ZPMetacmdWorkerThread::CronHandle() {
  //	find out timeout slave and kill them 
//  struct timeval now;
//  gettimeofday(&now, NULL);
//  {
//    slash::RWLock l(&rwlock_, true); // Use WriteLock to iterate the conns_
//    std::map<int, void*>::iterator iter = conns_.begin();
//    while (iter != conns_.end()) {
//      DLOG(INFO) << "Slave:  now.tv_sec:" << now.tv_sec << ", last_inter:" << static_cast<ZPMetacmdConn*>(iter->second)->last_interaction().tv_sec;
//      if (now.tv_sec - static_cast<ZPMetacmdConn*>(iter->second)->last_interaction().tv_sec > 20) {
//        LOG(INFO) << "Find Timeout Slave: " << static_cast<ZPMetacmdConn*>(iter->second)->ip_port();
//        close(iter->first);
//        // erase item in slaves_
//        // TODO
//        //zp_data_server->DeleteSlave(iter->first);
//
//        delete(static_cast<ZPMetacmdConn*>(iter->second));
//        iter = conns_.erase(iter);
//        continue;
//      }
//      iter++;
//    }
//  }

  // erase it in slaves_;
//  {
//    slash::MutexLock l(&zp_data_server->slave_mutex_);
//    std::vector<SlaveItem>::iterator iter = zp_data_server->slaves_.begin();
//    while (iter != zp_data_server->slaves_.end()) {
//      DLOG(INFO) << " ip_port: " << iter->node.ip << " port " << iter->node.port << " sender_tid: " << iter->sender_tid << " sync_fd: " << iter->sync_fd << " sender: " << iter->sender << " create_time: " << iter->create_time.tv_sec;
//      if (!FindSlave(iter->sync_fd)) {
//   //   if ((iter->stage == SLAVE_ITEM_STAGE_ONE && now.tv_sec - iter->create_time.tv_sec > 30)
//   //       || (iter->stage == SLAVE_ITEM_STAGE_TWO && !FindSlave(iter->hb_fd))) {
//   //     //pthread_kill(iter->tid);
//
//        // Kill BinlogSender
//        LOG(WARNING) << "Erase slave (" << iter->node.ip << ":" << iter->node.port << ") ";
//        {
//          // TODO
//          zp_data_server->slave_mutex_.Unlock();
//          zp_data_server->DeleteSlave(iter->sync_fd);
//          zp_data_server->slave_mutex_.Lock();
//        }
//        continue;
//      }
//      iter++;
//    }
//  }
}

bool ZPMetacmdWorkerThread::AccessHandle(std::string& ip) {
  if (ip == "127.0.0.1") {
    ip = zp_data_server->local_ip();
  }

// TODO
//  slash::MutexLock l(&zp_data_server->slave_mutex_);
//  for (auto iter = zp_data_server->slaves_.begin(); iter != zp_data_server->slaves_.end(); iter++) {
//    if (iter->node == ip) {
//      LOG(INFO) << "HeartbeatThread access connection " << ip;
//      return true;
//    }
//  }
//
//  LOG(WARNING) << "HeartbeatThread deny connection: " << ip;
//  return false;
 // if (ip == zp_data_server->meta_ip() && conns_.size() == 0) {
    zp_data_server->PlusMetaServerConns();
    return true;
 // }
  LOG(WARNING) << "Deny connection from " << ip << " current conns size: " << conns_.size();
  return false;
}

