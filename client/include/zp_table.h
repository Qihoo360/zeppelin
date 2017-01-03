/*
 * "Copyright [2016] qihoo"
 * "Author <hrxwwd@163.com>"
 */
#ifndef CLIENT_INCLUDE_ZP_TABLE_H_
#define CLIENT_INCLUDE_ZP_TABLE_H_

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


struct Node {
  Node();
  Node(const std::string& other_ip, int other_port);

  Node(const Node& other);

  ~Node();

  std::string ip;
  int port;

  Node& operator = (const Node& other);
  bool operator < (const Node& other) const;
  bool operator == (const Node& other) const;
};


struct BinlogOffset {
  int file_num;
  int offset;
};

struct SpaceInfo {
  int64_t used;
  int64_t remain;
};

class Table {
 public:
  struct Partition {
    std::vector<Node> slaves;
    Node master;
    int id;

    explicit Partition(const ZPMeta::Partitions& partition_info) {
      master.ip = partition_info.master().ip();
      master.port = partition_info.master().port();
      for (int i = 0; i < partition_info.slaves_size(); i++) {
        slaves.push_back(Node(partition_info.slaves(i).ip(),
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

  Node GetKeyMaster(const std::string& key);

  const Partition* GetPartition(const std::string& key);

  void DebugDump();
  void GetNodes(std::vector<Node> *node);

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
  std::vector<Node> meta_addr;
};

}  // namespace libzp
#endif  // CLIENT_INCLUDE_ZP_TABLE_H_
