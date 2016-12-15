#ifndef ZP_METACMD_BGWORKER_H
#define ZP_METACMD_BGWORKER_H

#include "zp_meta.pb.h"
#include "bg_thread.h"
#include "pb_cli.h"

class ZPMetacmdBGWorker {
 public:

  ZPMetacmdBGWorker();
  virtual ~ZPMetacmdBGWorker();
  void AddTask();

 private:
  pink::PbCli* cli_;
  pink::BGThread* bg_thread_;
  static void MetaUpdateTask(void* task);

  pink::Status ParsePullResponse(const ZPMeta::MetaCmdResponse &response, int64_t &epoch);
  pink::Status Send();
  pink::Status Recv(int64_t &receive_epoch);
  bool FetchMetaInfo(int64_t &receive_epoch);

};
#endif //ZP_METACMD_BGWORKER_H
