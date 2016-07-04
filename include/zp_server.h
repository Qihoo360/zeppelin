#ifndef ZP_SERVER_H
#define ZP_SERVER_H

#include <string>
#include <memory>
#include "zp_options.h"
#include "zp_binlog.h"
#include "zp_meta.h"
#include "zp_define.h"
#include "zp_ping_thread.h"
#include "zp_heartbeat_thread.h"
#include "zp_binlog_sender_thread.h"
#include "zp_binlog_receiver_thread.h"

#include "pb_conn.h"
#include "pb_cli.h"
#include "holy_thread.h"

#include "slash_status.h"
#include "slash_mutex.h"

#include "nemo.h"

using slash::Status;

class ZPServer;
class ZPServerConn;
class ZPServerThread;

class ZPWorkerThread;
class ZPDispatchThread;


class ZPServer {
 public:

  explicit ZPServer(const ZPOptions& option);
  virtual ~ZPServer();
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

  bool FindSlave(const Node& node);
  Status AddBinlogSender(SlaveItem &slave, uint32_t filenum, uint64_t con_offset);
  void DeleteSlave(int fd);
  bool ShouldJoin();

  Binlog* logger_;

  slash::Mutex slave_mutex_;
  std::vector<SlaveItem> slaves_;

  slash::RecordMutex mutex_record_;

 private:

  ZPOptions options_;

  slash::Mutex server_mutex_;

  // DB
  std::shared_ptr<nemo::Nemo> db_;

  // Server related
  int worker_num_;
  ZPWorkerThread* zp_worker_thread_[kMaxWorkerThread];
  ZPDispatchThread* zp_dispatch_thread_;
  ZPPingThread* zp_ping_thread_;
  ZPHeartbeatThread* zp_heartbeat_thread_;
  ZPBinlogReceiverThread* zp_binlog_receiver_thread_;

  // State related
  pthread_rwlock_t state_rw_;
  int repl_state_;  
  bool is_seed;

};

#endif
