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
#ifndef SRC_META_ZP_META_COMMAND_H_
#define SRC_META_ZP_META_COMMAND_H_

#include <string>
#include "include/zp_command.h"

class PingCmd : public Cmd  {
 public:
  explicit PingCmd(int flag) : Cmd(flag, kPingCmd) {}
  virtual std::string name() const  {
    return "Ping";
  }
  virtual void Do(const google::protobuf::Message *req,
      google::protobuf::Message *res, void* partition = NULL) const;
};

class PullCmd : public Cmd  {
 public:
  explicit PullCmd(int flag) : Cmd(flag, kPullCmd) {}
  virtual std::string name() const  {
    return "Pull";
  }
  virtual void Do(const google::protobuf::Message *req,
      google::protobuf::Message *res, void* partition = NULL) const;
};

class InitCmd : public Cmd  {
 public:
  explicit InitCmd(int flag) : Cmd(flag, kInitCmd) {}
  virtual std::string name() const  {
    return "Init";
  }
  virtual void Do(const google::protobuf::Message *req,
      google::protobuf::Message *res, void* partition = NULL) const;
};

class SetMasterCmd : public Cmd  {
 public:
  explicit SetMasterCmd(int flag) : Cmd(flag, kSetMasterCmd) {}
  virtual std::string name() const  {
    return "SetMaster";
  }
  virtual void Do(const google::protobuf::Message *req,
      google::protobuf::Message *res, void* partition = NULL) const;
};

class AddSlaveCmd : public Cmd  {
 public:
  explicit AddSlaveCmd(int flag) : Cmd(flag, kAddSlaveCmd) {}
  virtual std::string name() const  {
    return "AddSlave";
  }
  virtual void Do(const google::protobuf::Message *req,
      google::protobuf::Message *res, void* partition = NULL) const;
};

class RemoveSlaveCmd : public Cmd  {
 public:
  explicit RemoveSlaveCmd(int flag) : Cmd(flag, kRemoveSlaveCmd) {}
  virtual std::string name() const  {
    return "RemoveSlave";
  }
  virtual void Do(const google::protobuf::Message *req,
      google::protobuf::Message *res, void* partition = NULL) const;
};

class ListTableCmd : public Cmd  {
 public:
  explicit ListTableCmd(int flag) : Cmd(flag, kListTableCmd) {}
  virtual std::string name() const  {
    return "ListTable";
  }
  virtual void Do(const google::protobuf::Message *req,
      google::protobuf::Message *res, void* partition = NULL) const;
};

class ListNodeCmd : public Cmd  {
 public:
  explicit ListNodeCmd(int flag) : Cmd(flag, kListNodeCmd) {}
  virtual std::string name() const  {
    return "ListNode";
  }
  virtual void Do(const google::protobuf::Message *req,
      google::protobuf::Message *res, void* partition = NULL) const;
};

class ListMetaCmd : public Cmd  {
 public:
  explicit ListMetaCmd(int flag) : Cmd(flag, kListMetaCmd) {}
  virtual std::string name() const  {
    return "ListMeta";
  }
  virtual void Do(const google::protobuf::Message *req,
      google::protobuf::Message *res, void* partition = NULL) const;
};

class MetaStatusCmd : public Cmd  {
 public:
  explicit MetaStatusCmd(int flag) : Cmd(flag, kMetaStatusCmd) {}
  virtual std::string name() const  {
    return "MetaStatus";
  }
  virtual void Do(const google::protobuf::Message *req,
      google::protobuf::Message *res, void* partition = NULL) const;
};

class DropTableCmd : public Cmd  {
 public:
  explicit DropTableCmd(int flag) : Cmd(flag, kDropTableCmd) {}
  virtual std::string name() const  {
    return "DropTable";
  }
  virtual void Do(const google::protobuf::Message *req,
      google::protobuf::Message *res, void* partition = NULL) const;
};

class MigrateCmd : public Cmd  {
 public:
  explicit MigrateCmd(int flag) : Cmd(flag, kMigrateCmd) {}
  virtual std::string name() const  {
    return "Migrate";
  }
  virtual void Do(const google::protobuf::Message *req,
      google::protobuf::Message *res, void* partition = NULL) const;
};

class CancelMigrateCmd : public Cmd  {
 public:
  explicit CancelMigrateCmd(int flag) : Cmd(flag, kCancelMigrateCmd) {}
  virtual std::string name() const  {
    return "CancelMigrateTable";
  }
  virtual void Do(const google::protobuf::Message *req,
      google::protobuf::Message *res, void* partition = NULL) const;
};

class RemoveNodesCmd : public Cmd  {
 public:
  explicit RemoveNodesCmd(int flag) : Cmd(flag, kRemoveNodesCmd) {}
  virtual std::string name() const  {
    return "RemoveNodes";
  }
  virtual void Do(const google::protobuf::Message *req,
      google::protobuf::Message *res, void* partition = NULL) const;
};

class AddMetaNodeCmd : public Cmd  {
 public:
  explicit AddMetaNodeCmd(int flag) : Cmd(flag, kAddMetaNodeCmd) {}
  virtual std::string name() const  {
    return "AddMetaNode";
  }
  virtual void Do(const google::protobuf::Message *req,
      google::protobuf::Message *res, void* partition = NULL) const;
};

class RemoveMetaNodeCmd : public Cmd  {
 public:
  explicit RemoveMetaNodeCmd(int flag) : Cmd(flag, kRemoveMetaNodeCmd) {}
  virtual std::string name() const  {
    return "RemoveMetaNode";
  }
  virtual void Do(const google::protobuf::Message *req,
      google::protobuf::Message *res, void* partition = NULL) const;
};

#endif  // SRC_META_ZP_META_COMMAND_H_
