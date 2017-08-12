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
#ifndef SRC_META_ZP_META_CONDITION_CRON_H_
#define SRC_META_ZP_META_CONDITION_CRON_H_
#include <string>
#include "pink/include/bg_thread.h"
#include "src/meta/zp_meta_update_thread.h"
#include "src/meta/zp_meta_offset_map.h"
#include "include/zp_meta.pb.h"

enum OffsetConditionType {
  kOffsetEq = 0,
};

struct OffsetCondition {
  OffsetConditionType type;
  std::string table;
  int partition_id;
  ZPMeta::Node left;
  ZPMeta::Node right;
};

struct OffsetConditionTask {
  OffsetCondition condition;
  UpdateTask update_task;
};

class ZPMetaConditionCron {
 public:
  ZPMetaConditionCron(NodeOffsetMap* offset_map,
      ZPMetaUpdateThread* update_thread);
  virtual ~ZPMetaConditionCron();
  void AddCronTask(OffsetConditionTask task);

 private:
  pink::BGThread* bg_thread_;
  NodeOffsetMap* offset_map_;
  ZPMetaUpdateThread* update_thread_;
  static void DoConditionTask(void *p);
};

#endif  // SRC_META_ZP_META_CONDITION_CRON_H_
