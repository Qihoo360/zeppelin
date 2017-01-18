#ifndef ZP_BINLOG_RECEIVE_BGWORKER
#define ZP_BINLOG_RECEIVE_BGWORKER
#include "bg_thread.h"
#include "client.pb.h"
#include "zp_command.h"

struct ZPBinlogReceiveTask {
  PartitionSyncOption option;
  const Cmd* cmd;
  client::CmdRequest request;
  uint64_t gap;

  ZPBinlogReceiveTask(const PartitionSyncOption &opt,
      const Cmd* c, const client::CmdRequest &req)
    : option(opt),
    cmd(c),
    request(req) {}

  ZPBinlogReceiveTask(const PartitionSyncOption &opt,
      uint64_t g)
    : option(opt),
    gap(g) {}
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
