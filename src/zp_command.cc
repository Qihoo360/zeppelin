#include "zp_command.h"
#include "zp_kv.h"

void InitCmdTable(std::unordered_map<int, Cmd*> *cmd_table) {
  //Kv
  ////SetCmd
  Cmd* setptr = new SetCmd(kCmdFlagsWrite);
  cmd_table->insert(std::pair<int, Cmd*>(static_cast<int>(client::OPCODE::SET), setptr));
  ////GetCmd
  Cmd* getptr = new GetCmd(kCmdFlagsRead);
  cmd_table->insert(std::pair<int, Cmd*>(static_cast<int>(client::OPCODE::GET), getptr));
}

Cmd* GetCmdFromTable(const client::OPCODE op, const std::unordered_map<int, Cmd*> &cmd_table) {
  std::unordered_map<int, Cmd*>::const_iterator it = cmd_table.find(static_cast<int>(op));
  if (it != cmd_table.end()) {
    return it->second;
  }
  return NULL;
}

void DestoryCmdTable(std::unordered_map<int, Cmd*> &cmd_table) {
  std::unordered_map<int, Cmd*>::const_iterator it = cmd_table.begin();
  for (; it != cmd_table.end(); ++it) {
    delete it->second;
  }
}

