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
#ifndef SRC_META_ZP_META_MIGRATE_REGISTER_H_
#define SRC_META_ZP_META_MIGRATE_REGISTER_H_
#include <string>
#include <vector>
#include <unordered_set>

#include "slash/include/slash_status.h"
#include "floyd/include/floyd.h"

#include "include/zp_meta.pb.h"

using slash::Status;

class ZPMetaMigrateRegister {
 public:
   explicit ZPMetaMigrateRegister(floyd::Floyd* floyd);

   Status Load();
   Status Init(const std::vector<ZPMeta::RelationCmdUnit>& diffs);
   Status Check(ZPMeta::MigrateStatus* status);
   Status Erase(const std::string& diff_key);
   Status GetN(uint32_t count, std::vector<ZPMeta::RelationCmdUnit>* items);
   Status Cancel();
   bool ExistWithLock();
  

 private:
   pthread_rwlock_t migrate_rw_;  // protect partition status below
   uint64_t ctime_;
   int total_size_;
   int refer_;  // refer count indicate how many task be processing now
   std::unordered_set<std::string> diff_keys_;
   floyd::Floyd* floyd_;

   bool Exist() const;

   ZPMetaMigrateRegister();
   // No copying allowed
   ZPMetaMigrateRegister (const ZPMetaMigrateRegister&);
   void operator=(const ZPMetaMigrateRegister&);
};

extern std::string DiffKey(const ZPMeta::RelationCmdUnit& diff);
extern std::string DiffKey(const std::string& table, int partition,
    const std::string& left_ip_port, const std::string& right_ip_port);

#endif
