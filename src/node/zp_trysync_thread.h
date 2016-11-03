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
  void TrySyncTaskSchedule(int partition_id);
  void TrySyncTask(int partition_id);

 private:
  bool should_exit_;

  // BGThread related
  struct TrySyncTaskArg {
    ZPTrySyncThread* thread;
    int partition_id;
    TrySyncTaskArg(ZPTrySyncThread* t, int id) :
      thread(t), partition_id(id) {}
  };
  slash::Mutex bg_thread_protector_;
  pink::BGThread* bg_thread_;
  static void DoTrySyncTask(void* arg);
  bool SendTrySync(Partition *partition);
  bool Send(Partition* partition, pink::PbCli* cli);
  // Return value:   0 means ok; -1 means wait; -2 means error;
  int Recv(int id, pink::PbCli* cli);

  // Rsync related
  int rsync_flag_;
  void PrepareRsync(Partition *partition);
  void RsyncRef();
  void RsyncUnref();
  
  // Connection related
  std::map<std::string, pink::PbCli*> client_pool_;
  pink::PbCli* GetConnection(const Node& node);
  void DropConnection(const Node& node);
};

#endif //ZP_TRYSYNC_THREAD_H
