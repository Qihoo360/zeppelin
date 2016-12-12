/*
 * "Copyright [2016] <hrxwwd@163.com>"
 */
#ifndef CLIENT_INCLUDE_ZP_META_CLI_H_
#define CLIENT_INCLUDE_ZP_META_CLI_H_

#include <string>
#include <unordered_map>

#include "include/pb_cli.h"

#include "include/zp_meta.pb.h"
#include "include/zp_types.h"
#include "include/zp_const.h"

namespace libzp {
class ZpMetaCli: public pink::PbCli {
 public:
  ZpMetaCli();
  virtual ~ZpMetaCli();
  Status Pull(const std::string& table,
      std::unordered_map<std::string, Table*>* tables_,
      int64_t* epoch);
  Status CreateTable(const std::string& table_name, const int partition_num);
 private:
  Status ResetClusterMap(const ZPMeta::MetaCmdResponse_Pull& pull,
      std::unordered_map<std::string, Table*>* tables_,
      int64_t* epoch);
  ZPMeta::MetaCmd meta_cmd_;
  ZPMeta::MetaCmdResponse meta_res_;
};
}  // namespace libzp

#endif  // CLIENT_INCLUDE_ZP_META_CLI_H_
