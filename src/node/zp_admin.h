#ifndef ZP_ADMIN_H
#define ZP_ADMIN_H

#include "zp_command.h"

class UpdateCmd : public Cmd {
 public:
  UpdateCmd(int flag) : Cmd(flag) {}
  //virtual Status Init(const void *buf, size_t count);
  virtual void Do(google::protobuf::Message *request, google::protobuf::Message *response);
};

class SyncCmd : public Cmd {
 public:
  SyncCmd(int flag) : Cmd(flag) {}
  //virtual Status Init(const void *buf, size_t count);
  virtual void Do(google::protobuf::Message *request, google::protobuf::Message *response);

  void set_fd(int fd) {
    fd_ = fd;
  }

 private:
  int fd_;
};

void InitMetaCmdTable(std::unordered_map<int, Cmd*> *cmd_table);

#endif
