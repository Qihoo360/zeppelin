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
#ifndef SRC_META_ZP_META_UPDATE_THREAD_H_
#define SRC_META_ZP_META_UPDATE_THREAD_H_
#include <glog/logging.h>
#include <deque>
#include <string>
#include <unordered_map>
#include <functional>

#include "pink/include/bg_thread.h"
#include "slash/include/slash_string.h"
#include "slash/include/slash_mutex.h"
#include "slash/include/slash_status.h"
#include "src/meta/zp_meta.pb.h"
#include "src/meta/zp_meta_info_store.h"
#include "src/meta/zp_meta_migrate_register.h"

enum ZPMetaUpdateOP : unsigned int {
  kOpUpNode = 0,
  kOpDownNode,
  kOpRemoveNodes,
  kOpAddTable,
  kOpRemoveTable,
  kOpAddSlave,
  kOpRemoveSlave,
  kOpHandover,  // Replace one node by another with the same role
  kOpSetMaster,
  kOpSetActive,  // ACTIVE the partition
  kOpSetStuck,  // Stuck the partition
  kOpSetSlowdown,  // Slowdown the partition
  kOpAddMeta,
  kOpRemoveMeta
};

const int MAX_ARGS = 8;
struct UpdateTask {
  ZPMetaUpdateOP op;
  std::function<std::string()> print_args_text;
  std::string sargs[MAX_ARGS];
  int iargs[MAX_ARGS];
};

typedef std::deque<UpdateTask> ZPMetaUpdateTaskDeque;

class ZPMetaUpdateThread  {
 public:
  explicit ZPMetaUpdateThread(ZPMetaInfoStore* is,
      ZPMetaMigrateRegister* migrate);
  ~ZPMetaUpdateThread();

  Status PendingUpdate(const UpdateTask& task);
  void Active();
  void Abandon();

 private:
  std::atomic<bool> is_stuck_;
  std::atomic<bool> should_stop_;
  pink::BGThread* worker_;
  slash::Mutex task_mutex_;
  ZPMetaUpdateTaskDeque task_deque_;
  ZPMetaInfoStore* info_store_;
  ZPMetaMigrateRegister* migrate_;

  static void UpdateFunc(void *p);
  Status ApplyUpdates(const ZPMetaUpdateTaskDeque& task_deque);
};

#endif  // SRC_META_ZP_META_UPDATE_THREAD_H_
