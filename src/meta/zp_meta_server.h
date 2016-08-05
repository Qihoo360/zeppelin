#ifndef ZP_MASTER_SERVER_H
#define ZP_MASTER_SERVER_H

#include <stdio.h>
#include <string>
#include "zp_options.h"
#include "zp_const.h"
#include "slash_status.h"
#include "slash_mutex.h"

#include "floyd.h"
#include "zp_meta_dispatch_thread.h"
#include "zp_meta_worker_thread.h"
#include "zp_meta_update_thread.h"

using slash::Status;


typedef std::unordered_map<std::string, struct timeval> NodeAliveMap;

class ZPMetaServer {
 public:

  explicit ZPMetaServer(const ZPOptions& option);
  virtual ~ZPMetaServer();
  Status Start();
  
  Status Set(const std::string &key, const std::string &value);
  Status Get(const std::string &key, std::string &value);
  Status Delete(const std::string &key);

  std::string seed_ip() {
    return options_.seed_ip;
  }
  int seed_port() {
    return options_.seed_port;
  }
  std::string local_ip() {
    return options_.local_ip;
  }
  int local_port() {
    return options_.local_port;
  }

  void CheckNodeAlive();
  void UpdateNodeAlive(const std::string& ip_port);
  void AddNodeAlive(const std::string& ip_port);
  
  bool GetLeader(std::string& ip, int& port) {
    int fy_port = 0;
    bool res = floyd_->GetLeader(ip, fy_port);
    if (res) {
      port = fy_port - kMetaPortShiftFY;
    }
    return res;
  }

 private:

  // Server related
  int worker_num_;
  ZPMetaWorkerThread* zp_meta_worker_thread_[kMaxMetaWorkerThread];
  ZPMetaDispatchThread* zp_meta_dispatch_thread_;

  floyd::Floyd* floyd_;
  ZPOptions options_;
  slash::Mutex alive_mutex_;
  NodeAliveMap node_alive_;

  ZPMetaUpdateThread update_thread_;

  //pthread_rwlock_t state_rw_;
  slash::Mutex server_mutex_;
};

#endif
