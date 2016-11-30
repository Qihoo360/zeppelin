/*
 * "Copyright [2016] <hrxwwd@163.com>"
 */
#ifndef CLIENT_INCLUDE_ZP_META_CLI_H_
#define CLIENT_INCLUDE_ZP_META_CLI_H_

#include <string>
#include "include/pb_cli.h"

#include "include/zp_meta.pb.h"
#include "include/zp_types.h"

namespace libZp {
class ZpMetaCli: public pink::PbCli {
 public:
  ZpMetaCli(const std::string& ip, const int port);
  virtual ~ZpMetaCli();
  Status Pull(ClusterMap& info, const std::string& table);
  Status CreateTable(const std::string& table_name, const int partition_num);
 private:
  Status ResetClusterMap(const ZPMeta::MetaCmdResponse_Pull& pull,
      ClusterMap& cluster_map, const std::string& table);
  std::string meta_ip_;
  int meta_port_;
};
}  // namespace libZp

#endif  // CLIENT_INCLUDE_ZP_META_CLI_H_
