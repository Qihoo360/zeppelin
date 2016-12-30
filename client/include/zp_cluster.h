/*
 * "Copyright [2016] qihoo"
 * "Author <hrxwwd@163.com>"
 */
#ifndef CLIENT_INCLUDE_ZP_CLUSTER_H_
#define CLIENT_INCLUDE_ZP_CLUSTER_H_

#include <string>
#include <vector>
#include <map>
#include <memory>
#include <utility>
#include <unordered_map>


#include "include/pb_cli.h"

#include "include/zp_meta.pb.h"
#include "include/client.pb.h"

#include "include/zp_table.h"
#include "include/zp_conn.h"
#include "include/zp_const.h"

namespace libzp {


class Cluster {
 public :

  explicit Cluster(const Options& options);
  explicit Cluster(const std::string& ip, const int port);
  virtual ~Cluster();
  Status Connect();

  // data cmd
  Status Set(const std::string& table, const std::string& key,
      const std::string& value);
  Status Get(const std::string& table, const std::string& key,
      std::string* value);
  Status Delete(const std::string& table, const std::string& key);

  // meta cmd
  Status CreateTable(const std::string& table_name, int partition_num);
  Status Pull(const std::string& table);
  Status SetMaster(const std::string& table, const int partition,
      const Node& ip_port);
  Status AddSlave(const std::string& table, const int partition,
      const Node& ip_port);
  Status RemoveSlave(const std::string& table, const int partition,
      const Node& ip_port);

  // statistical cmd
  Status ListTable(std::vector<std::string>* tables);
  Status ListMeta(Node* master, std::vector<Node>* nodes);
  Status ListNode(std::vector<Node>* nodes,
      std::vector<std::string>* status);

  Status InfoQps(const std::string& table, int* qps, int* total_query);
  Status InfoOffset(const Node& node, const std::string& table,
      std::vector<std::pair<int, BinlogOffset>>* partitions);

  // local cmd
  Status DebugDumpTable(const std::string& table);
  const Table::Partition* GetPartition(const std::string& table,
      const std::string& key);

 private :

  void InitParam();
  Node GetRandomMetaAddr();
  Status GetDataMaster(const std::string& table,
      const std::string& key, Node* master);

  Status SubmitDataCmd(const std::string& table,
      const std::string& key, bool has_pull = false);
  Status TryDataRpc(const Node& node, int attempt = 0);
  Status SubmitMetaCmd(int attempt = 0);
  Status ResetClusterMap(const ZPMeta::MetaCmdResponse_Pull& pull);

  // meta info
  int64_t epoch_;
  std::vector<Node> meta_addr_;
  std::unordered_map<std::string, Table*> tables_;

  // connection pool
  ConnectionPool* meta_pool_;
  ConnectionPool* data_pool_;

  // Pb command for communication
  ZPMeta::MetaCmd meta_cmd_;
  ZPMeta::MetaCmdResponse meta_res_;
  client::CmdRequest data_cmd_;
  client::CmdResponse data_res_;
};

class Client {
 public :

  explicit Client(const std::string& ip, const int port,
      const std::string& table);
  virtual ~Client();
  Status Connect();

  // data cmd
  Status Set(const std::string& key, const std::string& value);
  Status Get(const std::string& key, std::string* value);
  Status Delete(const std::string& key);


 private :
  Cluster* cluster_;
  const std::string table_;
};


};  // namespace libzp

#endif  // CLIENT_INCLUDE_ZP_CLUSTER_H_
