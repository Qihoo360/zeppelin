#ifndef ZP_DATA_SERVER_H
#define ZP_DATA_SERVER_H

#include <string>
#include <memory>
#include "zp_options.h"
#include "zp_binlog.h"
#include "zp_meta_utils.h"
#include "zp_const.h"
#include "zp_metacmd_worker_thread.h"
#include "zp_ping_thread.h"
#include "zp_trysync_thread.h"
#include "zp_binlog_sender_thread.h"
#include "zp_binlog_receiver_thread.h"

#include "pb_conn.h"
#include "pb_cli.h"
#include "holy_thread.h"

#include "slash_status.h"
#include "slash_mutex.h"

#include "nemo.h"

using slash::Status;

class ZPDataServer;
class ZPDataServerConn;
class ZPDataServerThread;

class ZPDataWorkerThread;
class ZPDataDispatchThread;


class ZPDataServer {
 public:

  explicit ZPDataServer(const ZPOptions& option);
  virtual ~ZPDataServer();
  Status Start();
  
  Status Set(const std::string &key, const std::string &value);

  const std::shared_ptr<nemo::Nemo> db() {
    return db_;
  }

  std::string master_ip() {
    return master_ip_;
  }
  int master_port() {
    return master_port_;
  }
  std::string meta_ip() {
    return options_.seed_ip;
  }
  int meta_port() {
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
  void BecomeMaster();
  void BecomeSlave(const std::string& master_ip, int port);
  bool ShouldTrySync();
  void TrySyncDone();

  bool ShouldJoinMeta();
  void PlusMetaServerConns();
  void MinusMetaServerConns();

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
  ZPDataWorkerThread* zp_worker_thread_[kMaxWorkerThread];
  ZPDataDispatchThread* zp_dispatch_thread_;
  ZPPingThread* zp_ping_thread_;
  ZPMetacmdWorkerThread* zp_metacmd_worker_thread_;
  ZPBinlogReceiverThread* zp_binlog_receiver_thread_;
  ZPTrySyncThread* zp_trysync_thread_;

  // State related
  pthread_rwlock_t state_rw_;
  int role_;
  int repl_state_;  
  std::string master_ip_;
  int master_port_;

  // Meta State related
  pthread_rwlock_t meta_state_rw_;
  std::atomic<bool> should_rejoin_;
  int meta_state_;
  int meta_server_conns_;

};

#endif
