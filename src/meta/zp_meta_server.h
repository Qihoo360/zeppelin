#ifndef ZP_MASTER_SERVER_H
#define ZP_MASTER_SERVER_H

#include <stdio.h>
#include <string>
#include "zp_options.h"
#include "zp_const.h"
#include "slash_status.h"
#include "slash_mutex.h"

#include "floyd.h"
#include "zp_meta_dispatch_thread.h"
#include "zp_meta_worker_thread.h"
#include "zp_meta_update_thread.h"

using slash::Status;


typedef std::unordered_map<std::string, struct timeval> NodeAliveMap;

class ZPMetaServer {
 public:

  explicit ZPMetaServer(const ZPOptions& option);
  virtual ~ZPMetaServer();
  Status Start();
  
  Status Set(const std::string &key, const std::string &value);
  Status Get(const std::string &key, std::string &value);
  Status Delete(const std::string &key);

  std::string seed_ip() {
    return options_.seed_ip;
  }
  int seed_port() {
    return options_.seed_port;
  }
  std::string local_ip() {
    return options_.local_ip;
  }
  int local_port() {
    return options_.local_port;
  }

  // Alive Check
  void CheckNodeAlive();
  void UpdateNodeAlive(const std::string& ip_port);
  void AddNodeAlive(const std::string& ip_port);
  
  // Floyd related
  Status GetPartition(uint32_t partition_id, ZPMeta::Partitions &partitions);
  Status SetPartition(uint32_t partition_id, const ZPMeta::Partitions &partitions);
  Status DeletePartition(uint32_t partition_id) {
    return DeleteFlat(PartitionId2Key(partition_id));
  }

  // Leader related
  bool IsLeader();
  Status RedirectToLeader(ZPMeta::MetaCmd &request, ZPMeta::MetaCmdResponse &response);

private:

  // Server related
  int worker_num_;
  ZPMetaWorkerThread* zp_meta_worker_thread_[kMaxMetaWorkerThread];
  ZPMetaDispatchThread* zp_meta_dispatch_thread_;
  ZPOptions options_;
  slash::Mutex server_mutex_;

  // Floyd related
  floyd::Floyd* floyd_;
  Status GetFlat(const std::string &key, std::string &value);
  Status SetFlat(const std::string &key, const std::string &value);
  Status DeleteFlat(const std::string &key);
  std::string PartitionId2Key(uint32_t id) {
    assert(id == 1);
    std::string key(ZP_META_KEY_PREFIX);
    key += "1"; //Only one partition now
    return key;
  }

  // Alive Check
  slash::Mutex alive_mutex_;
  NodeAliveMap node_alive_;
  ZPMetaUpdateThread update_thread_;
  void RestoreNodeAlive(const ZPMeta::Partitions &partitions);

  // Leader slave
  bool leader_first_time_;
  slash::Mutex leader_mutex_;
  pink::PbCli* leader_cli_;
  std::string leader_ip_;
  int leader_cmd_port_;
  Status BecomeLeader();
  void CleanLeader() {
    if (leader_cli_) {
      leader_cli_->Close();
      delete leader_cli_;
    }
    leader_ip_.clear();
    leader_cmd_port_ = 0;
  }
  bool GetLeader(std::string& ip, int& port) {
    int fy_port = 0;
    bool res = floyd_->GetLeader(ip, fy_port);
    if (res) {
      port = fy_port - kMetaPortShiftFY;
    }
    return res;
  }
};

#endif
