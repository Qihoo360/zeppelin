#ifndef ZP_METACMD_THREAD_H
#define ZP_METACMD_THREAD_H

#include "bg_thread.h"
#include "pb_cli.h"

class ZPMetacmdThread : public pink::BGThread {
 public:

  ZPMetacmdThread();
  virtual ~ZPMetacmdThread();
 
  static void DoMetaUpdateTask(void* arg) {
    (static_cast<ZPMetacmdThread*>(arg))->MetaUpdateTask();
  }
  void MetaUpdateTask();

 private:
  pink::PbCli *cli_;

  pink::Status Send();
  pink::Status Recv(int64_t &receive_epoch);
  bool FetchMetaInfo(int64_t &receive_epoch);

};
#endif //ZP_METACMD_THREAD_H
