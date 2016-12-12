/*
 * "Copyright [2016] <hrxwwd@163.com>"
 */
#ifndef CLIENT_INCLUDE_ZP_DATA_CLI_H_
#define CLIENT_INCLUDE_ZP_DATA_CLI_H_

#include <string>
#include "include/pb_cli.h"

#include "include/client.pb.h"
#include "include/zp_types.h"
#include "include/zp_const.h"

namespace libzp {
class ZpDataCli: public pink::PbCli {
 public:
  ZpDataCli();
  virtual ~ZpDataCli();
  Status Set(const std::string& table, const std::string& key,
      const std::string& value);
  Status Get(const std::string& table, const std::string& key,
      std::string* value);
 private:
  client::CmdRequest data_cmd_;
  client::CmdResponse data_res_;
};
}  // namespace libzp

#endif  // CLIENT_INCLUDE_ZP_DATA_CLI_H_
