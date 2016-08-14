#ifndef ZP_PARTITION_H
#define ZP_PARTITION_H

#include <memory>

#include "nemo.h"

class Partition {
 public:
  Binlog* logger_;
  slash::Mutex slave_mutex_;
  std::vector<SlaveItem> slaves_;

  slash::RecordMutex mutex_record_;

  ZPBinlogReceiverThread* binlog_receiver_thread_;

 private:
  // DB
  std::shared_ptr<nemo::Nemo> db_;

};
//struct ClientInfo {
//  int fd;
//  std::string ip_port;
//  int last_interaction;
//};
//

#endif
