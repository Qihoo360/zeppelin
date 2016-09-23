#ifndef ZP_ADMIN_H
#define ZP_ADMIN_H

#include "zp_command.h"

class UpdateCmd : public Cmd {
 public:
  UpdateCmd(int flag) : Cmd(flag) {}
  //virtual Status Init(const void *buf, size_t count);
  virtual void Do(google::protobuf::Message *req, google::protobuf::Message *res, void* partition = NULL, bool readonly = false);
};

//void InitMetaCmdTable(std::unordered_map<int, Cmd*> *cmd_table);

#endif
