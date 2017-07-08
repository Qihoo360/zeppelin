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
#ifndef SRC_NODE_ZP_TRYSYNC_THREAD_H_
#define SRC_NODE_ZP_TRYSYNC_THREAD_H_
#include <map>
#include <string>
#include <memory>
#include "slash/include/slash_status.h"
#include "slash/include/slash_mutex.h"
#include "pink/include/pink_cli.h"
#include "pink/include/bg_thread.h"

#include "include/zp_const.h"
#include "src/node/zp_data_partition.h"

class ZPTrySyncThread  {
 public:
  ZPTrySyncThread();
  virtual ~ZPTrySyncThread();
  void TrySyncTaskSchedule(const std::string& table,
      int partition_id, uint64_t delay = 0);
  void TrySyncTask(const std::string& table_name, int partition_id);

 private:
  // BGThread related
  struct TrySyncTaskArg {
    ZPTrySyncThread* thread;
    std::string table_name;
    int partition_id;
    TrySyncTaskArg(ZPTrySyncThread* t, const std::string& table, int id)
        : thread(t), table_name(table), partition_id(id){}
  };
  slash::Mutex bg_thread_protector_;
  pink::BGThread* bg_thread_;
  static void DoTrySyncTask(void* arg);
  bool SendTrySync(const std::string& table_name, int partition_id);
  bool Send(std::shared_ptr<Partition> partition, pink::PinkCli* cli);

  struct RecvResult {
    client::StatusCode code;
    std::string message;
    uint32_t filenum;
    uint64_t offset;
  };
  bool Recv(std::shared_ptr<Partition> partition, pink::PinkCli* cli,
      RecvResult* res);

  // Rsync related
  int rsync_flag_;
  void RsyncRef();
  void RsyncUnref();

  // Connection related
  std::map<std::string, pink::PinkCli*> client_pool_;
  pink::PinkCli* GetConnection(const Node& node);

  void DropConnection(const Node& node);
};

#endif  // SRC_NODE_ZP_TRYSYNC_THREAD_H_
