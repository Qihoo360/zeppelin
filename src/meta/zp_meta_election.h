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
#ifndef SRC_META_ZP_META_ELECTION_H_
#define SRC_META_ZP_META_ELECTION_H_
#include <string>

#include "slash/include/slash_status.h"
#include "floyd/include/floyd.h"

#include "src/meta/zp_meta.pb.h"

using slash::Status;

class ZPMetaElection {
 public:
  ZPMetaElection(floyd::Floyd* f);
  ~ZPMetaElection();
  
  bool GetLeader(std::string* ip, int* port);

 private:
  floyd::Floyd* floyd_;
  ZPMeta::MetaLeader last_leader_;
  bool Jeopardy(std::string* ip, int* port);
  Status ReadLeaderRecord(ZPMeta::MetaLeader* cleader);
  Status WriteLeaderRecord(const ZPMeta::MetaLeader& cleader);
};

#endif  // SRC_META_ZP_META_ELECTION_H_
