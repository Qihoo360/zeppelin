#ifndef ZP_ADMIN_H
#define ZP_ADMIN_H

#include "zp_command.h"

class PingCmd : public Cmd {
 public:
  PingCmd(int flag) : Cmd(flag) {}
  virtual Status Init(const void *buf, size_t count);
  virtual void Do();
};

class JoinCmd : public Cmd {
 public:
  JoinCmd(int flag) : Cmd(flag) {}
  virtual Status Init(const void *buf, size_t count);
  virtual void Do();

  void set_fd(int fd) {
    fd_ = fd;
  }

 private:
  int fd_;
};


void InitClientCmdTable(std::unordered_map<int, Cmd*> *cmd_table);

#endif
