#ifndef ZP_METACMD_THREAD_H
#define ZP_METACMD_THREAD_H

#include "zp_meta.pb.h"
#include "bg_thread.h"
#include "pb_cli.h"

class ZPMetacmdThread : public pink::BGThread {
 public:

  ZPMetacmdThread();
  virtual ~ZPMetacmdThread();
  
  void MetacmdTaskSchedule();
  static void DoMetaUpdateTask(void* arg) {
    (static_cast<ZPMetacmdThread*>(arg))->MetaUpdateTask();
  }
  void MetaUpdateTask();

 private:
  pink::PbCli *cli_;

  std::atomic<bool> is_working_;
  pink::Status ParsePullResponse(const ZPMeta::MetaCmdResponse &response, int64_t &epoch);
  pink::Status Send();
  pink::Status Recv(int64_t &receive_epoch);
  bool FetchMetaInfo(int64_t &receive_epoch);

};
#endif //ZP_METACMD_THREAD_H
