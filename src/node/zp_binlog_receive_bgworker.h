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
#ifndef SRC_NODE_ZP_BINLOG_RECEIVE_BGWORKER_H_
#define SRC_NODE_ZP_BINLOG_RECEIVE_BGWORKER_H_
#include "pink/include/bg_thread.h"
#include "include/client.pb.h"
#include "include/zp_command.h"
#include "src/node/zp_data_partition.h"

struct ZPBinlogReceiveTask {
  PartitionSyncOption option;
  const Cmd* cmd;
  client::CmdRequest request;
  uint64_t i;

  ZPBinlogReceiveTask(const PartitionSyncOption &opt,
      const Cmd* c, const client::CmdRequest &req)
    : option(opt),
    cmd(c),
    request(req) {}

  ZPBinlogReceiveTask(const PartitionSyncOption &opt,
      uint64_t integer)
    : option(opt),
    i(integer) {}
};

class ZPBinlogReceiveBgWorker  {
 public:
    explicit ZPBinlogReceiveBgWorker(int full);
    ~ZPBinlogReceiveBgWorker();
    void AddTask(ZPBinlogReceiveTask *task);
 private:
    pink::BGThread* bg_thread_;
    static void DoBinlogReceiveTask(void* arg);
};

#endif  // SRC_NODE_ZP_BINLOG_RECEIVE_BGWORKER_H_
