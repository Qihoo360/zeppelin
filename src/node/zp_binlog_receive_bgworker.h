#ifndef ZP_BINLOG_RECEIVE_BGWORKER
#define ZP_BINLOG_RECEIVE_BGWORKER
#include "bg_thread.h"
#include "zp_command.h"

struct ZPBinlogReceiveTask {
  uint32_t partition_id;
  const Cmd* cmd;
  client::CmdRequest request;
  std::string from_node;
  uint32_t filenum;
  uint64_t offset;
  ZPBinlogReceiveTask(uint32_t id, const Cmd* c, const client::CmdRequest &req,
      const std::string& from, uint32_t arg_filenum, uint64_t arg_offset)
    : partition_id(id), cmd(c),
    request(req), from_node(from),
    filenum(arg_filenum), offset(arg_offset) {}
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
