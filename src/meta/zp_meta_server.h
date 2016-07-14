#ifndef ZP_MASTER_SERVER_H
#define ZP_MASTER_SERVER_H

#include <string>
#include <memory>
#include "zp_options.h"
#include "zp_binlog.h"
#include "zp_meta_utils.h"
#include "zp_const.h"
//#include "zp_heartbeat_thread.h"

#include "pb_conn.h"
#include "pb_cli.h"
#include "holy_thread.h"

#include "slash_status.h"
#include "slash_mutex.h"

#include "floyd.h"

using slash::Status;

class ZPMetaServer {
 public:

  explicit ZPMetaServer(const ZPOptions& option);
  virtual ~ZPMetaServer();
  Status Start();
  
  Status Set(const std::string &key, const std::string &value);

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

 private:

  floyd::Floyd* floyd_;
  ZPOptions options_;

  pthread_rwlock_t state_rw_;
  slash::Mutex server_mutex_;
};

#endif
