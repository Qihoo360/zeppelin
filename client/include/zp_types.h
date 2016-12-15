/*
 * "Copyright [2016] qihoo"
 * "Author <hrxwwd@163.com>"
 */
#ifndef CLIENT_INCLUDE_ZP_TYPES_H_
#define CLIENT_INCLUDE_ZP_TYPES_H_

#include <string>
#include <list>
#include <map>
#include <vector>
#include <memory>
#include <iostream>
#include <unordered_map>
#include <utility>

#include "include/pb_cli.h"

#include "include/zp_meta.pb.h"


namespace libzp {

typedef pink::Status Status;

struct IpPort {
  IpPort() {
  }
  IpPort(const std::string& other_ip, int other_port) :
    ip(other_ip),
    port(other_port) {
  }

  IpPort(const IpPort& other) :
    ip(other.ip),
    port(other.port) {
  }

  ~IpPort() {
  }

  std::string ip;
  int port;

  IpPort& operator = (const IpPort& other) {
    ip = other.ip;
    port = other.port;
    return *this;
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


class Table {
 public:
  struct Partition {
    std::vector<IpPort> slaves;
    IpPort master;
    int id;

    explicit Partition(const ZPMeta::Partitions& partition_info) {
      master.ip = partition_info.master().ip();
      master.port = partition_info.master().port();
      for (int i = 0; i < partition_info.slaves_size(); i++) {
        slaves.push_back(IpPort(partition_info.slaves(i).ip(),
            partition_info.slaves(i).port()));
      }
      id = partition_info.id();
    }

    Partition(const Partition& other) {
      master = other.master;
      slaves = other.slaves;
      id = other.id;
    }

    Partition() {
      id = -1;
    }
  };

  explicit Table(const ZPMeta::Table& table_info) {
    table_name_ = table_info.name();
    partition_num_ = table_info.partitions_size();
    ZPMeta::Partitions partition_info;
    for (int i = 0; i < table_info.partitions_size(); i++) {
      partition_info = table_info.partitions(i);
      Partition* par = new Partition(partition_info);
      partitions_.insert(std::make_pair(partition_info.id(), par));
    }
  }

  virtual ~Table() {
    std::map<int, Partition*>::iterator iter = partitions_.begin();
    while (iter != partitions_.end()) {
      delete iter->second;
      iter++;
    }
  }

  IpPort GetKeyMaster(const std::string& key) {
    int par_num = std::hash<std::string>()(key) % partitions_.size();
    std::map<int, Partition*>::iterator iter = partitions_.find(par_num);
    if (iter != partitions_.end()) {
      return iter->second->master;
    } else {
      return IpPort();
    }
  }

  Partition* GetPartition(const std::string& key) {
    int par_num = std::hash<std::string>()(key) % partitions_.size();
    std::map<int, Partition*>::iterator iter = partitions_.find(par_num);
    if (iter != partitions_.end()) {
      return iter->second;
    } else {
      return NULL;
    }
  }

  void DebugDump() {
    std::cout << "  name: "<< table_name_ <<std::endl;
    std::cout << "  partition: "<< partition_num_ <<std::endl;
    auto par = partitions_.begin();
    while (par != partitions_.end()) {
      std::cout << "    partition: "<< par->second->id;
      std::cout << "    master: " << par->second->master.ip
        << " : " << par->second->master.port << std::endl;
      par++;
    }
  }

 private:
  std::string table_name_;
  int partition_num_;
  std::map<int, Partition*> partitions_;
};


class Options {
 public :
  Options() {
  }
  ~Options() {
  }
  std::vector<IpPort> meta_addr;
};

class ConnectionPool {
 public :
  pink::PbCli* GetConnection(const IpPort ip_port) {
    std::map<IpPort, pink::PbCli*>::iterator it;
    it = conn_pool_.find(ip_port);
    if (it != conn_pool_.end()) {
      return it->second;
    } else {
      pink::PbCli* cli = new pink::PbCli();
      Status s = cli->Connect(ip_port.ip, ip_port.port);
      if (s.ok()) {
        conn_pool_.insert(std::make_pair(ip_port, cli));
        return cli;
      } else {
        delete cli;
        return NULL;
      }
    }
  }

  void RemoveConnection(const IpPort ip_port) {
    std::map<IpPort, pink::PbCli*>::iterator it;
    it = conn_pool_.find(ip_port);
    if (it != conn_pool_.end()) {
      delete(it->second);
      conn_pool_.erase(it);
    }
  }

  pink::PbCli* GetExistConnection(IpPort* ip_port) {
    if (conn_pool_.size() != 0) {
      *ip_port = conn_pool_.begin()->first;
      return conn_pool_.begin()->second;
    } else {
      return NULL;
    }
  }

 private:
  std::map<IpPort, pink::PbCli*> conn_pool_;
};

}  // namespace libzp
#endif  // CLIENT_INCLUDE_ZP_TYPES_H_
