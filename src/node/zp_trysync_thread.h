#ifndef ZP_TRYSYNC_THREAD_H
#define ZP_TRYSYNC_THREAD_H
#include "pb_cli.h"
#include "status.h"
#include "slash_mutex.h"
#include "bg_thread.h"
#include "zp_const.h"
#include "zp_data_partition.h"

class ZPTrySyncThread {
 public:
  ZPTrySyncThread();
  virtual ~ZPTrySyncThread();
  void TrySyncTaskSchedule(Partition* partition);
  void TrySyncTask(Partition* partition);

 private:
  bool should_exit_;

  // BGThread related
  struct TrySyncTaskArg {
    ZPTrySyncThread* thread;
    Partition* partition;
    TrySyncTaskArg(ZPTrySyncThread* t, Partition* ptr)
        : thread(t), partition(ptr){}
  };
  slash::Mutex bg_thread_protector_;
  pink::BGThread* bg_thread_;
  static void DoTrySyncTask(void* arg);
  bool SendTrySync(Partition *partition);
  bool Send(const Partition* partition, pink::PbCli* cli);
  
  struct RecvResult {
    client::StatusCode code;
    std::string message;
    uint32_t filenum;
    uint64_t offset;
  };
  bool Recv(Partition* partition, pink::PbCli* cli, RecvResult* res);

  // Rsync related
  int rsync_flag_;
  void RsyncRef();
  void RsyncUnref();
  
  // Connection related
  std::map<std::string, pink::PbCli*> client_pool_;
  pink::PbCli* GetConnection(const Node& node);
  void DropConnection(const Node& node);
};

#endif //ZP_TRYSYNC_THREAD_H
