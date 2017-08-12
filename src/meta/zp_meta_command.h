#ifndef ZP_META_COMMAND_H
#define ZP_META_COMMAND_H

#include "include/zp_command.h"

class PingCmd : public Cmd {
 public:
  PingCmd(int flag) : Cmd(flag) {}
  virtual std::string name() const override {
    return "Ping"; 
  }
  virtual void Do(const google::protobuf::Message *req,
      google::protobuf::Message *res, void* partition = NULL) const;
};

class PullCmd : public Cmd {
 public:
  PullCmd(int flag) : Cmd(flag) {}
  virtual std::string name() const override {
    return "Pull"; 
  }
  virtual void Do(const google::protobuf::Message *req,
      google::protobuf::Message *res, void* partition = NULL) const;
};

class InitCmd : public Cmd {
 public:
  InitCmd(int flag) : Cmd(flag) {}
  virtual std::string name() const override {
    return "Init"; 
  }
  virtual void Do(const google::protobuf::Message *req,
      google::protobuf::Message *res, void* partition = NULL) const;
};

class SetMasterCmd : public Cmd {
 public:
  SetMasterCmd(int flag) : Cmd(flag) {}
  virtual std::string name() const override {
    return "SetMaster"; 
  }
  virtual void Do(const google::protobuf::Message *req,
      google::protobuf::Message *res, void* partition = NULL) const;
};

class AddSlaveCmd : public Cmd {
 public:
  AddSlaveCmd(int flag) : Cmd(flag) {}
  virtual std::string name() const override {
    return "AddSlave"; 
  }
  virtual void Do(const google::protobuf::Message *req,
      google::protobuf::Message *res, void* partition = NULL) const;
};

class RemoveSlaveCmd : public Cmd {
 public:
  RemoveSlaveCmd(int flag) : Cmd(flag) {}
  virtual std::string name() const override {
    return "RemoveSlave"; 
  }
  virtual void Do(const google::protobuf::Message *req,
      google::protobuf::Message *res, void* partition = NULL) const;
};

class ListTableCmd : public Cmd {
 public:
  ListTableCmd(int flag) : Cmd(flag) {}
  virtual std::string name() const override {
    return "ListTable"; 
  }
  virtual void Do(const google::protobuf::Message *req,
      google::protobuf::Message *res, void* partition = NULL) const;
};

class ListNodeCmd : public Cmd {
 public:
  ListNodeCmd(int flag) : Cmd(flag) {}
  virtual std::string name() const override {
    return "ListNode"; 
  }
  virtual void Do(const google::protobuf::Message *req,
      google::protobuf::Message *res, void* partition = NULL) const;
};

class ListMetaCmd : public Cmd {
 public:
  ListMetaCmd(int flag) : Cmd(flag) {}
  virtual std::string name() const override {
    return "ListMeta"; 
  }
  virtual void Do(const google::protobuf::Message *req,
      google::protobuf::Message *res, void* partition = NULL) const;
};

class MetaStatusCmd : public Cmd {
 public:
  MetaStatusCmd(int flag) : Cmd(flag) {}
  virtual std::string name() const override {
    return "MetaStatus"; 
  }
  virtual void Do(const google::protobuf::Message *req,
      google::protobuf::Message *res, void* partition = NULL) const;
};

class DropTableCmd : public Cmd {
 public:
  DropTableCmd(int flag) : Cmd(flag) {}
  virtual std::string name() const override {
    return "DropTable"; 
  }
  virtual void Do(const google::protobuf::Message *req,
      google::protobuf::Message *res, void* partition = NULL) const;
};

class MigrateCmd : public Cmd {
 public:
  MigrateCmd(int flag) : Cmd(flag) {}
  virtual std::string name() const override {
    return "Migrate"; 
  }
  virtual void Do(const google::protobuf::Message *req,
      google::protobuf::Message *res, void* partition = NULL) const;
};

class CancelMigrateCmd : public Cmd {
 public:
  CancelMigrateCmd(int flag) : Cmd(flag) {}
  virtual std::string name() const override {
    return "CancelMigrateTable"; 
  }
  virtual void Do(const google::protobuf::Message *req,
      google::protobuf::Message *res, void* partition = NULL) const;
};

// TODO (wk)
//class CheckMigrateCmd : public Cmd {
// public:
//  CheckMigrateCmd(int flag) : Cmd(flag) {}
//  virtual std::string name() const override {
//    return "CheckMigrateTable"; 
//  }
//  virtual void Do(const google::protobuf::Message *req,
//      google::protobuf::Message *res, void* partition = NULL) const;
//};

#endif
