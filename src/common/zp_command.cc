#include "zp_command.h"

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

