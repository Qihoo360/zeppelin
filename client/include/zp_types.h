/*
 * "Copyright [2016] <hrxwwd@163.com>"
 */
#ifndef CLIENT_INCLUDE_ZP_TYPES_H_
#define CLIENT_INCLUDE_ZP_TYPES_H_

#include <string>
#include <list>
#include <map>
#include <vector>
#include <memory>
#include <iostream>

#include "include/pb_cli.h"

#include "include/zp_meta.pb.h"


namespace libZp {

typedef pink::Status Status;

class IpPort {
 public:
  IpPort() {}
  IpPort(const std::string other_ip, int other_port) {
    ip = other_ip;
    port = other_port;
  }
  IpPort(const IpPort& other) {
    ip = other.ip;
    port = other.port;
  }

  ~IpPort() {
  }

  std::string ip;
  int port;

  void operator = (const IpPort& other) {
    ip = other.ip;
    port = other.port;
  }

  bool operator < (const IpPort& other) const {
    if (port < other.port) {
      return true;
    }
    return false;
  }

  bool operator == (const IpPort& other) const {
    if (ip == other.ip && port == other.port) {
      return true;
    }
    return false;
  }
};

class ReplicaSet {
 public:
  std::vector<IpPort> slaves;
  IpPort master;

  ReplicaSet(const ZPMeta::Partitions& partition_info) {
    master.ip = partition_info.master().ip();
    master.port = partition_info.master().port();
    for (int i = 0; i < partition_info.slaves_size(); i++) {
      slaves.emplace_back(partition_info.slaves(i).ip(),
          partition_info.slaves(i).port());
    }
  }
  ReplicaSet(const ReplicaSet& other) {
    master = other.master;
    slaves = other.slaves;
  }
};

class TableMap {
 public:
  std::string table_name;
  int partition_num;
  std::map<int, ReplicaSet> partitions;

  TableMap(const ZPMeta::Table& table_info) {
    table_name = table_info.name();
    partition_num = table_info.partitions_size();
    partitions.clear();
    ZPMeta::Partitions partition_info;
    for (int i = 0; i < table_info.partitions_size(); i++) {
      partition_info = table_info.partitions(i);
      partitions.emplace(partition_info.id(), partition_info);
    }
  }
};

class ClusterMap {
 public:
  ClusterMap() {
    epoch = 0;
  }
  ~ClusterMap() {
  }
  int32_t table_num;
  int64_t epoch;
  std::map<std::string, TableMap> table_maps;
};

class Options {
 public :
  Options() {
  }
  ~Options() {
  }
  std::vector<IpPort> meta_addr;
};


}  // namespace libZp
#endif  // CLIENT_INCLUDE_ZP_TYPES_H_
