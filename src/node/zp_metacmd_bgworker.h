#ifndef ZP_METACMD_BGWORKER_H
#define ZP_METACMD_BGWORKER_H
#include "pink/include/bg_thread.h"
#include "pink/include/pink_cli.h"

#include "include/zp_meta.pb.h"

class ZPMetacmdBGWorker {
 public:

  ZPMetacmdBGWorker();
  virtual ~ZPMetacmdBGWorker();
  void AddTask();

 private:
  pink::PinkCli* cli_;
  pink::BGThread* bg_thread_;
  static void MetaUpdateTask(void* task);

  Status ParsePullResponse(const ZPMeta::MetaCmdResponse &response,
      int64_t &epoch);
  Status Send();
  Status Recv(int64_t &receive_epoch);
  bool FetchMetaInfo(int64_t &receive_epoch);

};
#endif //ZP_METACMD_BGWORKER_H
