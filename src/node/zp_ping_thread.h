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
#ifndef SRC_NODE_ZP_PING_THREAD_H_
#define SRC_NODE_ZP_PING_THREAD_H_
#include <string>
#include "slash/include/slash_status.h"
#include "pink/include/pink_cli.h"
#include "pink/include/pink_thread.h"

#include "src/node/zp_data_partition.h"

class ZPPingThread : public pink::Thread  {
 public:
  ZPPingThread() {
    cli_ = pink::NewPbCli();
    cli_->set_connect_timeout(1500);
    set_thread_name("ZPDataPing");
  }
  virtual ~ZPPingThread();

 private:
  pink::PinkCli *cli_;
  TablePartitionOffsets last_offsets_;

  bool CheckOffsetDelta(const std::string table_name,
      int partition_id, const BinlogOffset &new_offset);
  slash::Status Send(bool all);
  slash::Status RecvProc();
  virtual void* ThreadMain();
};
#endif  // SRC_NODE_ZP_PING_THREAD_H_
