#ifndef ZP_COMMAND_H
#define ZP_COMMAND_H

#include <deque>
#include <string>
#include <memory>
#include <unordered_map>

#include "client.pb.h"
#include "zp_meta.pb.h"

#include "slash/include/slash_string.h"
#include "slash/include/slash_status.h"

//Constant for command name

using slash::Status;

enum CmdFlagsMask {
  kCmdFlagsMaskRW                  = 1,
  kCmdFlagsMaskType                = 14,
  kCmdFlagsMaskLocal               = 16,
  kCmdFlagsMaskSuspend             = 32,
  kCmdFlagsMaskPrior               = 64,
  kCmdFlagsMaskAdminRequire        = 128,
  kCmdFlagsMaskSinglePartition     = 256,
};

enum CmdFlags {
  kCmdFlagsRead              = 0, //default rw
  kCmdFlagsWrite             = 1,
  kCmdFlagsKv                = 2,
  kCmdFlagsAdmin             = 4, 
  // kCmdFlagsBit               = 12,
  kCmdFlagsNoLocal           = 0, //default nolocal
  kCmdFlagsLocal             = 16,
  kCmdFlagsNoSuspend         = 0, //default nosuspend
  kCmdFlagsSuspend           = 32,
  kCmdFlagsNoPrior           = 0, //default noprior
  kCmdFlagsPrior             = 64,
  kCmdFlagsNoAdminRequire    = 0, //default no need admin
  kCmdFlagsAdminRequire      = 128,
  kCmdFlagsSinglePartition   = 0, //default single partition
  kCmdFlagsMultiPartition    = 256,
};


class Cmd {
 public:
  Cmd(int flag) : flag_(flag) {}
  virtual ~Cmd() {}

  virtual void Do(const google::protobuf::Message *request,
      google::protobuf::Message *response, void* arg = NULL) const = 0;
  virtual bool GenerateLog(const google::protobuf::Message *request,
      std::string* log_raw) const {
    return request->SerializeToString(log_raw);
  }
  virtual std::string name() const = 0;
  virtual std::string ExtractTable(const google::protobuf::Message *request) const {
    return "";
  }
  virtual int ExtractPartition(const google::protobuf::Message *request) const {
    return -1;
  }
  virtual std::string ExtractKey(const google::protobuf::Message *request) const {
    return "";
  }

  bool is_write() const {
    return ((flag_ & kCmdFlagsMaskRW) == kCmdFlagsWrite);
  }
  uint16_t flag_type() const {
    return flag_ & kCmdFlagsMaskType;
  }
  bool is_admin() const {
    return ((flag_ & kCmdFlagsMaskType) == kCmdFlagsAdmin);
  }
  bool is_local() const {
    return ((flag_ & kCmdFlagsMaskLocal) == kCmdFlagsLocal);
  }
  // Others need to be suspended when a suspend command run
  bool is_suspend() const {
    return ((flag_ & kCmdFlagsMaskSuspend) == kCmdFlagsSuspend);
  }

  bool is_single_paritition() const {
    return ((flag_ & kCmdFlagsMaskSinglePartition) == kCmdFlagsSinglePartition);
  }

 private:
  uint16_t flag_;

  Cmd(const Cmd&);
  Cmd& operator=(const Cmd&);
};

// Method for Cmd Table
Cmd* GetCmdFromTable(const int op, const std::unordered_map<int, Cmd*> &cmd_table);
void DestoryCmdTable(std::unordered_map<int, Cmd*> &cmd_table);

#endif
