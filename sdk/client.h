#ifndef ZP_CLIENT_H
#define ZP_CLIENT_H

#include <string>
#include <vector>

#include "pb_cli.h"
#include "slash_status.h"

using slash::Status;

namespace client {

struct Option;
class Server;
class Cluster;
class ZPPbCli;

enum ClientError {
  kOk = 0,
};

class Server {
 public:
  std::string ip;
  int port;

  // colon separated ip:port
  Server(const std::string& str);
  Server(const std::string& _ip, const int& _port) : ip(_ip), port(_port) {}

  Server(const Server& server)
      : ip(server.ip),
      port(server.port) {}

  Server& operator=(const Server& server) {
    ip = server.ip;
    port = server.port;
    return *this;
  }

 private:
};

struct Option {
  // TODO session timeout
  int64_t timeout;

  std::vector<Server> servers;

  Option();

  // comma separated server list:   ip1:port1,ip2:port2
  Option(const std::string& server_str);

  Option(const std::vector<std::string>& server_list); 
  Option(const Option& option);

  void ParseFromArgs(int argc, char *argv[]);
  void Init(const std::vector<std::string>& server_list);
};

class Cluster {
 public:
  Cluster(const Option& option);

  Status Set(const std::string& key, const std::string& value);
  Status Get(const std::string& key, std::string* value);

 private:
  void Init();

  Option option_;

  //pink::PbCli pb_cli_;

  ZPPbCli *pb_cli_;
};

class ZPPbCli : public pink::PbCli {
 public:
  void set_opcode(int opcode) {
    opcode_ = opcode;
  }
 private:
  virtual void BuildWbuf();
  int32_t opcode_;
};

} // namespace client
#endif
