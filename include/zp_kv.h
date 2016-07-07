#ifndef ZP_KV_H
#define ZP_KV_H

#include "zp_command.h"

////// kv //////
class SetCmd : public Cmd {
 public:
  SetCmd(int flag) : Cmd(flag) {}
  virtual Status Init(const void *buf, size_t count);
  virtual void Do();
  virtual std::string key() { return key_; }

 private:
  std::string key_;
};

class GetCmd : public Cmd {
 public:
  GetCmd(int flag) : Cmd(flag) {}
  virtual Status Init(const void *buf, size_t count);
  virtual void Do();
};

void InitClientCmdTable(std::unordered_map<int, Cmd*> *cmd_table) {
  //Kv
  ////SetCmd
  Cmd* setptr = new SetCmd(kCmdFlagsWrite);
  cmd_table->insert(std::pair<int, Cmd*>(static_cast<int>(client::OPCODE::SET), setptr));
  ////GetCmd
  Cmd* getptr = new GetCmd(kCmdFlagsRead);
  cmd_table->insert(std::pair<int, Cmd*>(static_cast<int>(client::OPCODE::GET), getptr));
}

#endif
