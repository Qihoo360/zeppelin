/*
 * "Copyright [2016] <hrxwwd@163.com>"
 */
#ifndef CLIENT_INCLUDE_ZP_DATA_CLI_H_
#define CLIENT_INCLUDE_ZP_DATA_CLI_H_

#include <string>
#include "include/pb_cli.h"

#include "include/client.pb.h"
#include "include/zp_types.h"

namespace libZp {
class ZpDataCli: public pink::PbCli {
 public:
  ZpDataCli(const std::string& ip, const int port);
  virtual ~ZpDataCli();
  Status Set(const std::string& table, const std::string& key,
      const std::string& value);
  Status Get(const std::string& table, const std::string& key,
      std::string& value);
 private:
  std::string data_ip_;
  int data_port_;
};
}  // namespace libZp

#endif  // CLIENT_INCLUDE_ZP_DATA_CLI_H_
