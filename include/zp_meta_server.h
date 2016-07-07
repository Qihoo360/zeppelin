#ifndef ZP_MASTER_SERVER_H
#define ZP_MASTER_SERVER_H

#include <string>
#include <memory>
#include "zp_options.h"
#include "zp_binlog.h"
#include "zp_meta.h"
#include "zp_define.h"
//#include "zp_heartbeat_thread.h"

#include "pb_conn.h"
#include "pb_cli.h"
#include "holy_thread.h"

#include "slash_status.h"
#include "slash_mutex.h"

#include "nemo.h"

using slash::Status;

class ZPMetaServer;
//class ZPMetaServerConn;
//class ZPMetaServerThread;

//class ZPWorkerThread;
//class ZPDispatchThread;


class ZPMetaServer {
 public:

  explicit ZPMetaServer(const ZPOptions& option);
  virtual ~ZPMetaServer();
  Status Start();
  
  Status Set(const std::string &key, const std::string &value);

  const std::shared_ptr<nemo::Nemo> db() {
    return db_;
  }

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

  Binlog* logger_;

  slash::Mutex slave_mutex_;
  std::vector<SlaveItem> slaves_;

  //slash::RecordMutex mutex_record_;

 private:

  ZPOptions options_;

  slash::Mutex server_mutex_;

  // DB
  std::shared_ptr<nemo::Nemo> db_;

  // Server related
  int worker_num_;
  //ZPWorkerThread* zp_worker_thread_[kMaxWorkerThread];
  //ZPDispatchThread* zp_dispatch_thread_;

  // State related
  pthread_rwlock_t state_rw_;
};

#endif
