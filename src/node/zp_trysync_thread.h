#ifndef ZP_TRYSYNC_THREAD_H
#define ZP_TRYSYNC_THREAD_H

#include <map>
#include "zp_const.h"
#include "zp_meta_utils.h"

#include "pb_cli.h"
#include "status.h"
#include "pink_thread.h"
#include "slash_mutex.h"


class ZPTrySyncThread : public pink::Thread {
 public:

  ZPTrySyncThread()
    : rsync_flag_(false) {
    //cli_ = new pink::PbCli();
    //cli_->set_connect_timeout(1500);
  }
  virtual ~ZPTrySyncThread();

 private:

  bool Send(const int partition_id, pink::PbCli* cli);

  // Return value:
  //    0 means ok;
  //    -1 means wait 
  //    -2 means error;
  int Recv(pink::PbCli* cli);

  void PrepareRsync();
  bool TryUpdateMasterOffset();

  // TODO maybe use uuid or serverId

  //int sockfd_;
  //pink::PbCli *cli_;
  bool rsync_flag_;

  std::map<std::string, pink::PbCli*> client_pool_;

  pink::PbCli* GetConnection(const Node& node);
  virtual void* ThreadMain();

};

#endif
