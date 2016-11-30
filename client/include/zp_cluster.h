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


#include "include/zp_types.h"
#include "include/zp_meta_cli.h"
#include "include/zp_data_cli.h"

namespace libZp {

class Cluster {
 public :

  explicit Cluster(const Options& options);
  virtual ~Cluster();
  Status Connect();
  Status CreateTable(const std::string& table_name, int partition_num);

  Status Set(const std::string& table, const std::string& key,
      const std::string& value);
  Status Get(const std::string& table, const std::string& key,
      std::string& value);
  Status Pull(const std::string& table);
  Status DumpTable(const std::string& table);

  // Status ListTable(std::vector<IpPort> &node_list);
  // Status ListMetaNode(std::vector<IpPort> &node_list);

 private :

  IpPort GetRandomMetaAddr();
  std::shared_ptr<ZpMetaCli> GetMetaCli();
  std::shared_ptr<ZpDataCli> GetDataCli(const IpPort& ip_port);
  std::shared_ptr<ZpMetaCli> CreateMetaCli(const IpPort& ip_port);
  std::shared_ptr<ZpDataCli> CreateDataCli(const IpPort& ip_port);

  Status GetDataMaster(IpPort& master, const std::string& table,
    const std::string& key);

  ClusterMap cluster_map_;
  std::map<IpPort, std::shared_ptr<ZpMetaCli>> meta_cli_;
  std::map<IpPort, std::shared_ptr<ZpDataCli>> data_cli_;
  std::vector<IpPort> meta_addr_;
  std::vector<IpPort> data_addr_;
};

};  // namespace libZp

#endif  // CLIENT_INCLUDE_ZP_CLUSTER_H_
