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
#ifndef SRC_META_ZP_META_INFO_STORE_H_
#define SRC_META_ZP_META_INFO_STORE_H_
#include "slash/include/slash_status.h"
#include "floyd/include/floyd.h"
#include "include/zp_meta.pb.h"

using slash::Status;

class ZPMetaInfoStore {
 public:
   explicit ZPMetaInfoStore(floyd::Floyd* floyd);
   static Status Create(floyd::Floyd* floyd, ZPMetaInfoStore** regist);

   Status Load();

 private:
   floyd::Floyd* floyd_;

   ZPMetaInfoStore();
   // No copying allowed
   ZPMetaInfoStore (const ZPMetaInfoStore&);
   void operator=(const ZPMetaInfoStore&);
};

extern std::string DiffKey(const ZPMeta::RelationCmdUnit& diff);

#endif  // SRC_META_ZP_META_INFO_STORE_H_
