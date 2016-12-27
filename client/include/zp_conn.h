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

#include "include/zp_meta.pb.h"
#include "include/zp_table.h"


namespace libzp {

typedef pink::Status Status;

class ZpConn {
 public:
  explicit ZpConn(const Node& node);
  ~ZpConn();
  Status Connect();
  Status Send(void *msg);
  Status Recv(void *msg_res);
  Status CheckTimeout();

  Node node_;
 private:
  Status ReConnect();
  pink::PbCli* cli_;
  uint64_t lastchecktime_;
};

class ConnectionPool {
 public :

  ConnectionPool();

  virtual ~ConnectionPool();

  ZpConn* GetConnection(const Node& node);
  void RemoveConnection(ZpConn* conn);
  ZpConn* GetExistConnection();

 private:
  std::map<Node, ZpConn*> conn_pool_;
};

}  // namespace libzp
#endif  // CLIENT_INCLUDE_ZP_CONN_H_
