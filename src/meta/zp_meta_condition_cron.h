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

struct OffsetCondition {
  std::string table;
  int partition_id;
  ZPMeta::Node left;
  ZPMeta::Node right;
  OffsetCondition(const std::string& t, int pid,
      const ZPMeta::Node& l, const ZPMeta::Node& r)
    : table(t), partition_id(pid) {
      left.CopyFrom(l);
      right.CopyFrom(r);
    }
};

class ZPMetaConditionCron {
 public:
  ZPMetaConditionCron(NodeOffsetMap* offset_map,
      ZPMetaUpdateThread* update_thread);
  virtual ~ZPMetaConditionCron();
  void AddCronTask(const OffsetCondition& condition,
      const UpdateTask& update_task);

 private:
  pink::BGThread* bg_thread_;
  NodeOffsetMap* offset_map_;
  ZPMetaUpdateThread* update_thread_;
  static void CronFunc(void *p);
  bool ChecknProcess(const OffsetCondition& condition,
      const UpdateTask& update_task);

  struct OffsetConditionArg {
    ZPMetaConditionCron* cron;
    OffsetCondition condition;
    UpdateTask update_task;
    OffsetConditionArg(ZPMetaConditionCron* cur,
        OffsetCondition cond, UpdateTask t)
      : cron(cur), condition(cond), update_task(t) {}
  };

};

#endif  // SRC_META_ZP_META_CONDITION_CRON_H_
