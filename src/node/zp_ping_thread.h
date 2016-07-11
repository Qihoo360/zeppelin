#ifndef ZP_PING_THREAD_H
#define ZP_PING_THREAD_H

#include "zp_const.h"
#include "zp_pb_cli.h"

#include "status.h"
#include "pink_thread.h"
#include "slash_mutex.h"


class ZPPingThread : public pink::Thread {
 public:

  ZPPingThread()
      : is_first_send_(true) {
        cli_ = new ZPPbCli();
        cli_->set_connect_timeout(1500);
      }
  virtual ~ZPPingThread();

  pink::Status Send();
  pink::Status RecvProc();

 private:

  // TODO maybe use uuid or serverId
  //int64_t sid_;
  bool is_first_send_;

  int sockfd_;
  ZPPbCli *cli_;

  virtual void* ThreadMain();

};

#endif
