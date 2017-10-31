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
#include <vector>
#include "pink/include/bg_thread.h"
#include "src/meta/zp_meta.pb.h"
#include "src/meta/zp_meta_update_thread.h"
#include "src/meta/zp_meta_info_store.h"

enum ConditionTaskType {
  kMigrate = 0,
  kSetMaster
};

struct OffsetCondition {
  ConditionTaskType type;
  std::string table;
  int partition_id;
  ZPMeta::Node left;
  ZPMeta::Node right;
  OffsetCondition(ConditionTaskType ctt, const std::string& t, int pid,
      const ZPMeta::Node& l, const ZPMeta::Node& r)
    : type(ctt), table(t), partition_id(pid) {
      left.CopyFrom(l);
      right.CopyFrom(r);
    }
};

class ZPMetaConditionCron  {
 public:
  ZPMetaConditionCron(ZPMetaInfoStore* info_store,
      ZPMetaMigrateRegister* migrate,
      ZPMetaUpdateThread* update_thread);
  virtual ~ZPMetaConditionCron();
  void AddCronTask(const OffsetCondition& condition,
      const std::vector<UpdateTask>& update_set);
  void Active();
  void Abandon();

 private:
  pink::BGThread* bg_thread_;
  ZPMetaInfoStore* info_store_;
  ZPMetaMigrateRegister* migrate_;
  ZPMetaUpdateThread* update_thread_;
  static void CronFunc(void *p);
  bool RecoverWhenError(const OffsetCondition& condition);
  bool ChecknProcess(const OffsetCondition& condition,
      const std::vector<UpdateTask>& update_set);

  struct OffsetConditionArg {
    ZPMetaConditionCron* cron;
    OffsetCondition condition;
    std::vector<UpdateTask> update_set;
    OffsetConditionArg(ZPMetaConditionCron* cur,
        OffsetCondition cond, std::vector<UpdateTask> us)
      : cron(cur), condition(cond), update_set(us) {}
  };
};

#endif  // SRC_META_ZP_META_CONDITION_CRON_H_
