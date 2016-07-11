#include "zp_command.h"
//#include "zp_kv.h"
//#include "zp_admin.h"

// Moved to Different server
//void InitClientCmdTable(std::unordered_map<int, Cmd*> *cmd_table) {
//  //Kv
//  ////SetCmd
//  Cmd* setptr = new SetCmd(kCmdFlagsWrite);
//  cmd_table->insert(std::pair<int, Cmd*>(static_cast<int>(client::OPCODE::SET), setptr));
//  ////GetCmd
//  Cmd* getptr = new GetCmd(kCmdFlagsRead);
//  cmd_table->insert(std::pair<int, Cmd*>(static_cast<int>(client::OPCODE::GET), getptr));
//}
//
//void InitServerControlCmdTable(std::unordered_map<int, Cmd*> *cmd_table) {
//  // Join
//  Cmd* joinptr = new JoinCmd(kCmdFlagsWrite);
//  cmd_table->insert(std::pair<int, Cmd*>(static_cast<int>(ServerControl::OPCODE::JOIN), joinptr));
//
//  // Ping
//  Cmd* pingptr = new PingCmd(kCmdFlagsRead);
//  cmd_table->insert(std::pair<int, Cmd*>(static_cast<int>(ServerControl::OPCODE::PING), pingptr));
//}

Cmd* GetCmdFromTable(const int op, const std::unordered_map<int, Cmd*> &cmd_table) {
  std::unordered_map<int, Cmd*>::const_iterator it = cmd_table.find(op);
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

