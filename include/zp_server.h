#ifndef ZP_SERVER_H
#define ZP_SERVER_H

#include <memory>
#include "zp_options.h"

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

const int ZP_MAX_WORKER_THREAD_NUM = 4;

class ZPServer {
 public:
  explicit ZPServer(const ZPOptions& option);
  virtual ~ZPServer();
  Status Start();
  
  Status Set(const std::string &key, const std::string &value);

  const std::shared_ptr<nemo::Nemo> db() {
    return db_;
  }

 private:
  ZPOptions options_;

  slash::Mutex server_mutex;

  // DB
  std::shared_ptr<nemo::Nemo> db_;

  // Server related
  int worker_num_;
  ZPWorkerThread* zp_worker_thread_[ZP_MAX_WORKER_THREAD_NUM];
  ZPDispatchThread* zp_dispatch_thread_;
};

#endif
