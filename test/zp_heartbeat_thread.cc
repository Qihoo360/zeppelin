#include "zp_heartbeat_thread.h"

#include "zp_heartbeat_conn.h"

#include "slash_mutex.h"

ZPHeartbeatThread::ZPHeartbeatThread(int port, int cron_interval) :
  HolyThread::HolyThread(port, cron_interval) {
  LOG_INFO("HeartbeatThread on port:%d\n", port); 
}

ZPHeartbeatThread::~ZPHeartbeatThread() {
  LOG_INFO ("ZPHeartbeat thread %u exit!!!\n", thread_id());
}

void ZPHeartbeatThread::CronHandle() {
  //	find out timeout slave and kill them 
//  struct timeval now;
//  gettimeofday(&now, NULL);
//  {
//    slash::RWLock l(&rwlock_, true); // Use WriteLock to iterate the conns_
//    std::map<int, void*>::iterator iter = conns_.begin();
//    while (iter != conns_.end()) {
//      DLOG(INFO) << "Slave:  now.tv_sec:" << now.tv_sec << ", last_inter:" << static_cast<ZPHeartbeatConn*>(iter->second)->last_interaction().tv_sec;
//      if (now.tv_sec - static_cast<ZPHeartbeatConn*>(iter->second)->last_interaction().tv_sec > 20) {
//        LOG(INFO) << "Find Timeout Slave: " << static_cast<ZPHeartbeatConn*>(iter->second)->ip_port();
//        close(iter->first);
//        // erase item in slaves_
//        // TODO
//        //zp_meta_server->DeleteSlave(iter->first);
//
//        delete(static_cast<ZPHeartbeatConn*>(iter->second));
//        iter = conns_.erase(iter);
//        continue;
//      }
//      iter++;
//    }
//  }
}

bool ZPHeartbeatThread::AccessHandle(std::string& ip) {
 // if (ip == "127.0.0.1") {
 //   ip = zp_meta_server->host();
 // }

// TODO
//  slash::MutexLock l(&zp_meta_server->slave_mutex_);
//  for (auto iter = zp_meta_server->slaves_.begin(); iter != zp_meta_server->slaves_.end(); iter++) {
//    if (iter->node == ip) {
//      LOG(INFO) << "HeartbeatThread access connection " << ip;
//      return true;
//    }
//  }
//
//  LOG(WARNING) << "HeartbeatThread deny connection: " << ip;
//  return false;
  return true;
}
