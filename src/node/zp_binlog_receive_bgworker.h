#ifndef ZP_BINLOG_RECEIVE_BGWORKER
#define ZP_BINLOG_RECEIVE_BGWORKER
#include "bg_thread.h"
#include "zp_command.h"

struct ZPBinlogReceiveTask {
  uint32_t partition_id;
  const Cmd* cmd;
  client::CmdRequest request;
  std::string raw;
  ZPBinlogReceiveTask(uint32_t id, const Cmd* c, const client::CmdRequest &req, const std::string &r)
    : partition_id(id), cmd(c), request(req), raw(r) {}
};

class ZPBinlogReceiveBgWorker {
  public:
    ZPBinlogReceiveBgWorker(int full);
    ~ZPBinlogReceiveBgWorker();
    void AddTask(ZPBinlogReceiveTask *task);
  private:
    pink::BGThread* bg_thread_;
    static void DoBinlogReceiveTask(void* arg);
};


#endif //ZP_BINLOG_REDEIVE_BGWORKER
