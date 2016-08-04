#ifndef ZP_TRYSYNC_THREAD_H
#define ZP_TRYSYNC_THREAD_H

#include "zp_const.h"

#include "pb_cli.h"
#include "status.h"
#include "pink_thread.h"
#include "slash_mutex.h"


class ZPTrySyncThread : public pink::Thread {
 public:

  ZPTrySyncThread()
    : rsync_flag_(false) {
    cli_ = new pink::PbCli();
    cli_->set_connect_timeout(1500);
  }
  virtual ~ZPTrySyncThread();

 private:

  bool Send();

  // Return value:
  //    0 means ok;
  //    -1 means wait 
  //    -2 means error;
  int Recv();

  void PrepareRsync();
  bool TryUpdateMasterOffset();

  // TODO maybe use uuid or serverId

  //int sockfd_;
  pink::PbCli *cli_;
  bool rsync_flag_;

  virtual void* ThreadMain();

};

#endif
