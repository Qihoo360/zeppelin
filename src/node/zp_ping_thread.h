#ifndef ZP_PING_THREAD_H
#define ZP_PING_THREAD_H
#include "slash/include/slash_status.h"
#include "pink/include/pink_cli.h"
#include "pink/include/pink_thread.h"

#include "src/node/zp_data_partition.h"

class ZPPingThread : public pink::Thread {
 public:

  ZPPingThread() {
        cli_ = pink::NewPbCli();
        cli_->set_connect_timeout(1500);
        set_thread_name("ZPDataPing");
      }
  virtual ~ZPPingThread();

 private:
  pink::PinkCli *cli_;
  TablePartitionOffsets last_offsets_;

  bool TryOffsetUpdate(const std::string table_name,
      int partition_id, const BinlogOffset &new_offset);
  slash::Status Send();
  slash::Status RecvProc();
  virtual void* ThreadMain();
};

#endif
