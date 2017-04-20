/*
 * "Copyright [2016] qihoo"
 * "Author <hrxwwd@163.com>"
 */

#ifndef CLIENT_INCLUDE_ZP_CONN_H_
#define CLIENT_INCLUDE_ZP_CONN_H_
#include <stdint.h>
#include <string>
#include <map>

#include "include/pb_cli.h"
#include "slash_status.h"

#include "include/zp_meta.pb.h"
#include "include/zp_table.h"


namespace libzp {

typedef slash::Status Status;

struct ZpCli {
 public:
  explicit ZpCli(const Node& node);
  ~ZpCli();
  Status CheckTimeout();

  Node node;
  pink::PbCli* cli;
  uint64_t lastchecktime;
};

class ConnectionPool {
 public :

  ConnectionPool();

  virtual ~ConnectionPool();

  ZpCli* GetConnection(const Node& node);
  void RemoveConnection(ZpCli* conn);
  ZpCli* GetExistConnection();

 private:
  std::map<Node, ZpCli*> conn_pool_;
};

}  // namespace libzp
#endif  // CLIENT_INCLUDE_ZP_CONN_H_
