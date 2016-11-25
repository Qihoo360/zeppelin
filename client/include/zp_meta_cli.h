#ifndef ZP_META_CLI_H
#define ZP_META_CLI_H

#include "pb_cli.h"

#include "zp_meta.pb.h"
#include "zp_types.h"
namespace libZp {
class ZpMetaCli: public pink::PbCli {
  public:
    ZpMetaCli(const std::string& ip, const int port);
    virtual ~ZpMetaCli();
    Status Pull(ClusterMap& info);
    Status CreateTable(std::string& table_name, int partition_num);
  private:
    Status ResetClusterMap(ZPMeta::MetaCmdResponse_Pull& pull, ClusterMap& cluster_map);
    std::string meta_ip_;
    int meta_port_;
};
}
#endif
