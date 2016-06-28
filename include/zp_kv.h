#ifndef ZP_KV_H
#define ZP_KV_H

#include "zp_command.h"

/*
 * kv
 */
class SetCmd : public Cmd {
 public:
  SetCmd(int flag) : Cmd(flag) {}
  virtual Status Init(const void *buf, size_t count);
  virtual void Do();
};

class GetCmd : public Cmd {
 public:
  GetCmd(int flag) : Cmd(flag) {}
  virtual Status Init(const void *buf, size_t count);
  virtual void Do();
};

#endif
