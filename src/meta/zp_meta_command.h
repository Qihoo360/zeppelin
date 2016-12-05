#ifndef ZP_META_COMMAND_H
#define ZP_META_COMMAND_H

#include "zp_command.h"

class PingCmd : public Cmd {
 public:
  PingCmd(int flag) : Cmd(flag) {}
  virtual void Do(const google::protobuf::Message *req,
      google::protobuf::Message *res, void* partition = NULL) const;
};

class PullCmd : public Cmd {
 public:
  PullCmd(int flag) : Cmd(flag) {}
  virtual void Do(const google::protobuf::Message *req,
      google::protobuf::Message *res, void* partition = NULL) const;
};

class InitCmd : public Cmd {
 public:
  InitCmd(int flag) : Cmd(flag) {}
  virtual void Do(const google::protobuf::Message *req,
      google::protobuf::Message *res, void* partition = NULL) const;
};

#endif
