#ifndef ZP_META_NODE_H
#define ZP_META_NODE_H

#include "zp_command.h"

class JoinCmd : public Cmd {
 public:
  JoinCmd(int flag) : Cmd(flag) {}
  virtual void Do(google::protobuf::Message *req, google::protobuf::Message *res, void* partition = NULL, bool readonly = false);
};

class PingCmd : public Cmd {
 public:
  PingCmd(int flag) : Cmd(flag) {}
  virtual void Do(google::protobuf::Message *req, google::protobuf::Message *res, void* partition = NULL, bool readonly = false);
};

class PullCmd : public Cmd {
 public:
  PullCmd(int flag) : Cmd(flag) {}
  virtual void Do(google::protobuf::Message *req, google::protobuf::Message *res, void* partition = NULL, bool readonly = false);
};

class InitCmd : public Cmd {
 public:
  InitCmd(int flag) : Cmd(flag) {}
  virtual void Do(google::protobuf::Message *req, google::protobuf::Message *res, void* partition = NULL, bool readonly = false);
};

#endif
