#ifndef ZP_TRYSYNC_THREAD_H
#define ZP_TRYSYNC_THREAD_H

#include "zp_const.h"

#include "pb_cli.h"
#include "status.h"
#include "pink_thread.h"
#include "slash_mutex.h"


class ZPTrySyncThread : public pink::Thread {
 public:

  ZPTrySyncThread() {
    cli_ = new pink::PbCli();
    cli_->set_connect_timeout(1500);
  }
  virtual ~ZPTrySyncThread();

  pink::Status Send();
  pink::Status Recv();

 private:

  // TODO maybe use uuid or serverId

  int sockfd_;
  pink::PbCli *cli_;

  virtual void* ThreadMain();

};

#endif
