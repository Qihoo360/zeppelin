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
  IpPort();
  IpPort(const std::string& other_ip, int other_port);

  IpPort(const IpPort& other);

  ~IpPort();

  std::string ip;
  int port;

  IpPort& operator = (const IpPort& other);
  bool operator < (const IpPort& other) const;
  bool operator == (const IpPort& other) const;
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

    ~Partition() {
      slaves.clear();
    }
  };

  explicit Table(const ZPMeta::Table& table_info);
  virtual ~Table();

  IpPort GetKeyMaster(const std::string& key);

  Partition* GetPartition(const std::string& key);

  void DebugDump();

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

class ZpCli {
 public:
  explicit ZpCli(const IpPort& ip_port);
  ~ZpCli();
  Status Connect();
  Status Send(void *msg);
  Status Recv(void *msg_res);
  Status CheckTimeout();
 private:
  uint64_t NowMicros();
  Status ReConnect();
  pink::PbCli* cli_;
  std::string ip_;
  int port_;
  uint64_t lastchecktime_;
};

class ConnectionPool {
 public :

  ConnectionPool();

  virtual ~ConnectionPool();

  ZpCli* GetConnection(const IpPort ip_port);
  void RemoveConnection(const IpPort ip_port);
  ZpCli* GetExistConnection(IpPort* ip_port);

 private:
  std::map<IpPort, ZpCli*> conn_pool_;
};

}  // namespace libzp
#endif  // CLIENT_INCLUDE_ZP_TYPES_H_
