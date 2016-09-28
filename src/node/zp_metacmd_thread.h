#ifndef ZP_METACMD_THREAD_H
#define ZP_METACMD_THREAD_H

#include <queue>

#include "zp_command.h"
#include "zp_admin.h"
#include "zp_util.h"

#include "status.h"
#include "pink_thread.h"
#include "pb_cli.h"

#include "slash_mutex.h"

#include "env.h"

class ZPMetacmdThread : public pink::Thread {
 public:

  ZPMetacmdThread();
  virtual ~ZPMetacmdThread();

  uint64_t query_num() {
    return query_num_;
  }

 private:
  std::atomic<uint64_t> query_num_;

  pink::PbCli *cli_;

  pink::Status Send();
  pink::Status Recv();

  virtual void* ThreadMain();
};
#endif
