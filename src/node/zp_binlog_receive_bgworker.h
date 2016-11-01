#ifndef ZP_BINLOG_RECEIVE_BGWORKER
#define ZP_BINLOG_RECEIVE_BGWORKER
#include "bg_thread.h"
#include "zp_command.h"

class ZPBinlogReceiveBgWorker : public pink::BGThread {
  public:
    ZPBinlogReceiveBgWorker(int full)
       : pink::BGThread(full) {}
    ~ZPBinlogReceiveBgWorker();
    static void DoBinlogReceiveTask(void* arg);
};

struct ZPBinlogReceiveArg {
  uint32_t partition_id;
  const Cmd* cmd;
  client::CmdRequest request;
  std::string raw;
  ZPBinlogReceiveArg(uint32_t id, const Cmd* c, const client::CmdRequest &req, const std::string &r)
    : partition_id(id), cmd(c), request(req), raw(r) {}
};


#endif //ZP_BINLOG_REDEIVE_BGWORKER
