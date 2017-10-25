// Copyright 2017 Qihoo
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http:// www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
#ifndef SRC_NODE_ZP_METACMD_BGWORKER_H_
#define SRC_NODE_ZP_METACMD_BGWORKER_H_
#include "slash/include/slash_status.h"
#include "pink/include/bg_thread.h"
#include "pink/include/pink_cli.h"

#include "src/meta/zp_meta.pb.h"

class ZPMetacmdBGWorker  {
 public:
  ZPMetacmdBGWorker();
  virtual ~ZPMetacmdBGWorker();
  void AddTask();

 private:
  pink::PinkCli* cli_;
  pink::BGThread* bg_thread_;
  static void MetaUpdateTask(void* task);

  Status ParsePullResponse(const ZPMeta::MetaCmdResponse &response,
      int64_t* epoch);
  Status Send();
  Status Recv(int64_t* receive_epoch);
  bool FetchMetaInfo(int64_t* receive_epoch);
};
#endif  // SRC_NODE_ZP_METACMD_BGWORKER_H_
