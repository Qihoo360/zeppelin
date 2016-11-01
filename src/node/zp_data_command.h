#ifndef ZP_KV_H
#define ZP_KV_H

#include "zp_command.h"

////// kv //////
class SetCmd : public Cmd {
 public:
  SetCmd(int flag) : Cmd(flag) {}
  virtual void Do(const google::protobuf::Message *req,
      google::protobuf::Message *res, void* partition) const;
  virtual std::string ExtractKey(const google::protobuf::Message *req) const {
    const client::CmdRequest* request = static_cast<const client::CmdRequest*>(req);
    return request->set().key();
  }
};

class GetCmd : public Cmd {
 public:
  GetCmd(int flag) : Cmd(flag) {}
  virtual void Do(const google::protobuf::Message *req,
      google::protobuf::Message *res, void* partition) const;
  virtual std::string ExtractKey(const google::protobuf::Message *req) const {
    const client::CmdRequest* request = static_cast<const client::CmdRequest*>(req);
    return request->get().key();
  }
};

////// Sync //////
class SyncCmd : public Cmd {
 public:
  SyncCmd(int flag) : Cmd(flag) {}
  virtual void Do(const google::protobuf::Message *req,
      google::protobuf::Message *res, void* partition) const;
};

#endif
