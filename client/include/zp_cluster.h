/*
 * "Copyright [2016] <hrxwwd@163.com>"
 */
#ifndef CLIENT_INCLUDE_ZP_CLUSTER_H_
#define CLIENT_INCLUDE_ZP_CLUSTER_H_

#include <string>
#include <vector>
#include <list>
#include <map>
#include <memory>
#include <unordered_map>


#include "include/zp_types.h"
#include "include/zp_const.h"
#include "include/zp_meta_cli.h"
#include "include/zp_data_cli.h"

namespace libzp {


class Cluster {
 public :

  explicit Cluster(const Options& options);
  virtual ~Cluster();
  Status Connect();
  Status CreateTable(const std::string& table_name, int partition_num);

  Status Set(const std::string& table, const std::string& key,
      const std::string& value);
  Status Get(const std::string& table, const std::string& key,
      std::string* value);
  Status Pull(const std::string& table);
  void DebugDumpTable(const std::string& table);
  // TODO(wenduo): 这里接口定义的不好, GetPartition 返回一个空
  Table::Partition GetPartition(const std::string& table,
      const std::string& key);

 private :

  IpPort GetRandomMetaAddr();
  Status GetDataMaster(const std::string& table, const std::string& key,
      IpPort* master);

  int64_t epoch;
  std::unordered_map<std::string, Table*> tables_;
  ConnectionPool<ZpMetaCli> meta_pool_;
  ConnectionPool<ZpDataCli> data_pool_;
  std::vector<IpPort> meta_addr_;
};

};  // namespace libzp

#endif  // CLIENT_INCLUDE_ZP_CLUSTER_H_
