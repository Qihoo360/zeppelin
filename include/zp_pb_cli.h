#ifndef ZP_PB_CLI_H
#define ZP_PB_CLI_H

#include "pb_cli.h"

class ZPPbCli : public pink::PbCli {
 public:
  void set_opcode(int opcode) {
    opcode_ = opcode;
  }
  int32_t opcode() {
    return opcode_;
  }

  pink::Status SendRaw(const void *msg, size_t size);

 private:
  virtual void BuildWbuf();
  int32_t opcode_;
};


#endif
