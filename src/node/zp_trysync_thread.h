#ifndef ZP_TRYSYNC_THREAD_H
#define ZP_TRYSYNC_THREAD_H

#include <map>
#include "zp_const.h"
#include "zp_meta_utils.h"
#include "zp_data_partition.h"

#include "pb_cli.h"
#include "status.h"
#include "pink_thread.h"
#include "slash_mutex.h"

class ZPTrySyncThread;
class ZPTrySyncFunctor {
  public:
    ZPTrySyncFunctor(ZPTrySyncThread* thread) : trysync_thread_(thread) {
    }
    void operator() (std::pair<int, Partition*> partition_pair);
  private:
    ZPTrySyncThread *trysync_thread_;

};

class ZPTrySyncThread : public pink::Thread {
 public:

  ZPTrySyncThread()
    :trysync_functor_(this),
    rsync_flag_(0) {
  }
  virtual ~ZPTrySyncThread();
  void SendTrySync(Partition *partition);

 private:

  ZPTrySyncFunctor trysync_functor_;
  bool Send(Partition* partition, pink::PbCli* cli);

  // Return value:
  //    0 means ok;
  //    -1 means wait 
  //    -2 means error;
  int Recv(pink::PbCli* cli);

  void PrepareRsync(Partition *partition);
  bool TryUpdateMasterOffset();

  // TODO maybe use uuid or serverId

  int rsync_flag_;
  void FlagRef();
  void FlagUnref();

  std::map<std::string, pink::PbCli*> client_pool_;
  pink::PbCli* GetConnection(const Node& node);
  void DropConnection(const Node& node);
  virtual void* ThreadMain();
};


#endif
