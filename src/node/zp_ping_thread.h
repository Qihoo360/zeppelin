#ifndef ZP_PING_THREAD_H
#define ZP_PING_THREAD_H

#include "pb_cli.h"
#include "status.h"
#include "pink_thread.h"

class ZPPingThread : public pink::Thread {
 public:

  ZPPingThread() {
        cli_ = new pink::PbCli();
        cli_->set_connect_timeout(1500);
      }
  virtual ~ZPPingThread();

 private:
  pink::PbCli *cli_;
  pink::Status Send();
  pink::Status RecvProc();
  virtual void* ThreadMain();
};

#endif
