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
#ifndef SRC_NODE_ZP_DATA_COMMAND_H_
#define SRC_NODE_ZP_DATA_COMMAND_H_

#include <string>
#include "include/zp_command.h"

////// kv ///// /
class SetCmd : public Cmd  {
 public:
  explicit SetCmd(int flag) : Cmd(flag, kSetCmd) {}
  virtual std::string name() const {
    return "Set";
  }
  virtual void Do(const google::protobuf::Message *req,
      google::protobuf::Message *res, void* partition) const;
  virtual bool GenerateLog(const google::protobuf::Message *request,
      std::string* raw) const;
  virtual std::string ExtractTable(const google::protobuf::Message *req) const {
    const client::CmdRequest* request =
      static_cast<const client::CmdRequest*>(req);
    return request->set().table_name();
  }
  virtual std::string ExtractKey(const google::protobuf::Message *req) const {
    const client::CmdRequest* request =
      static_cast<const client::CmdRequest*>(req);
    return request->set().key();
  }
};

class GetCmd : public Cmd  {
 public:
  explicit GetCmd(int flag) : Cmd(flag, kGetCmd) {}
  virtual std::string name() const {
    return "Get";
  }
  virtual void Do(const google::protobuf::Message *req,
      google::protobuf::Message *res, void* partition) const;
  virtual std::string ExtractTable(const google::protobuf::Message *req) const {
    const client::CmdRequest* request =
      static_cast<const client::CmdRequest*>(req);
    return request->get().table_name();
  }
  virtual std::string ExtractKey(const google::protobuf::Message *req) const {
    const client::CmdRequest* request =
      static_cast<const client::CmdRequest*>(req);
    return request->get().key();
  }
};

class DelCmd : public Cmd  {
 public:
  explicit DelCmd(int flag) : Cmd(flag, kDelCmd) {}
  virtual std::string name() const {
    return "Del";
  }
  virtual void Do(const google::protobuf::Message *req,
      google::protobuf::Message *res, void* partition) const;
  virtual std::string ExtractTable(const google::protobuf::Message *req) const {
    const client::CmdRequest* request =
      static_cast<const client::CmdRequest*>(req);
    return request->del().table_name();
  }
  virtual std::string ExtractKey(const google::protobuf::Message *req) const {
    const client::CmdRequest* request =
      static_cast<const client::CmdRequest*>(req);
    return request->del().key();
  }
};

class ListbyTagCmd : public Cmd {
 public:
  explicit ListbyTagCmd(int flag) : Cmd(flag, kListbyTagCmd) {}
  virtual std::string name() const {
    return "ListbyTag";
  }
  virtual void Do(const google::protobuf::Message *req,
      google::protobuf::Message *res, void* partition) const;
  virtual std::string ExtractTable(const google::protobuf::Message *req) const {
    const client::CmdRequest* request =
      static_cast<const client::CmdRequest*>(req);
    return request->listby_tag().table_name();
  }
  // Namely hashtag
  virtual std::string ExtractKey(const google::protobuf::Message *req) const {
    const client::CmdRequest* request =
      static_cast<const client::CmdRequest*>(req);
    return request->listby_tag().hash_tag();
  }
};

class DeletebyTagCmd : public Cmd {
 public:
  explicit DeletebyTagCmd(int flag) : Cmd(flag, kDeletebyTagCmd) {}
  virtual std::string name() const {
    return "DeletebyTag";
  }
  virtual void Do(const google::protobuf::Message *req,
      google::protobuf::Message *res, void* partition) const;
  virtual std::string ExtractTable(const google::protobuf::Message *req) const {
    const client::CmdRequest* request =
      static_cast<const client::CmdRequest*>(req);
    return request->deleteby_tag().table_name();
  }
  // Namely hashtag
  virtual std::string ExtractKey(const google::protobuf::Message *req) const {
    const client::CmdRequest* request =
      static_cast<const client::CmdRequest*>(req);
    return request->deleteby_tag().hash_tag();
  }
};

class WriteBatchCmd : public Cmd {
 public:
  explicit WriteBatchCmd(int flag) : Cmd(flag, kWriteBatchCmd) {}
  virtual std::string name() const {
    return "WriteBatch";
  }
  virtual void Do(const google::protobuf::Message *req,
      google::protobuf::Message *res, void* partition) const;
  virtual std::string ExtractTable(const google::protobuf::Message *req) const {
    const client::CmdRequest* request =
      static_cast<const client::CmdRequest*>(req);
    return request->write_batch().table_name();
  }
  // Namely hashtag
  virtual std::string ExtractKey(const google::protobuf::Message *req) const {
    const client::CmdRequest* request =
      static_cast<const client::CmdRequest*>(req);
    return request->write_batch().hash_tag();
  }
};

////// Info Cmds //// /
class InfoCmd : public Cmd  {
 public:
  explicit InfoCmd(int flag) : Cmd(flag, kInfoCmd) {}
  virtual std::string name() const {
    return "Info";
  }
  virtual void Do(const google::protobuf::Message *req,
      google::protobuf::Message *res, void* parition = NULL) const;
  virtual std::string ExtractTable(const google::protobuf::Message *req) const {
    const client::CmdRequest* request =
      static_cast<const client::CmdRequest*>(req);
    if (request->has_info() && request->info().has_table_name()) {
      return request->info().table_name();
    }
    return "";
  }
};

////// Sync ///// /
class SyncCmd : public Cmd  {
 public:
  explicit SyncCmd(int flag) : Cmd(flag, kSyncCmd) {}
  virtual std::string name() const {
    return "Sync";
  }
  virtual void Do(const google::protobuf::Message *req,
      google::protobuf::Message *res, void* partition) const;
  virtual std::string ExtractTable(const google::protobuf::Message *req) const {
    const client::CmdRequest* request =
      static_cast<const client::CmdRequest*>(req);
    return request->sync().table_name();
  }
  virtual int ExtractPartition(const google::protobuf::Message *req) const {
    const client::CmdRequest* request =
      static_cast<const client::CmdRequest*>(req);
    return request->sync().sync_offset().partition();
  }
};

class MgetCmd : public Cmd  {
 public:
  explicit MgetCmd(int flag) : Cmd(flag, kMgetCmd) {}
  virtual std::string name() const {
    return "Mget";
  }
  virtual void Do(const google::protobuf::Message *req,
      google::protobuf::Message *res, void* partition) const;
  virtual std::string ExtractTable(const google::protobuf::Message *req) const {
    const client::CmdRequest* request =
      static_cast<const client::CmdRequest*>(req);
    return request->mget().table_name();
  }
};

class MsetCmd : public Cmd  {
 public:
  explicit MsetCmd(int flag) : Cmd(flag, kMsetCmd) {}
  virtual std::string name() const {
    return "Mset";
  }
  virtual void Do(const google::protobuf::Message *req,
      google::protobuf::Message *res, void* partition) const;
  virtual std::string ExtractTable(const google::protobuf::Message *req) const {
    const client::CmdRequest* request =
      static_cast<const client::CmdRequest*>(req);
    std::string table_name;
    if (request->mset_size() > 0) {
      table_name = request->mset(0).table_name();
    }
    return table_name;
  }
};

class FlushDBCmd : public Cmd  {
 public:
  explicit FlushDBCmd(int flag) : Cmd(flag, kFlushDBCmd) {}
  virtual std::string name() const {
    return "FlushDB";
  }
  virtual void Do(const google::protobuf::Message *req,
      google::protobuf::Message *res, void* partition) const;
  virtual std::string ExtractTable(const google::protobuf::Message *req) const {
    const client::CmdRequest* request =
      static_cast<const client::CmdRequest*>(req);
    return request->flushdb().table_name();
  }
  virtual int ExtractPartition(const google::protobuf::Message *req) const {
    const client::CmdRequest* request =
      static_cast<const client::CmdRequest*>(req);
    return request->flushdb().partition_id();
  }
};

#endif  // SRC_NODE_ZP_DATA_COMMAND_H_
