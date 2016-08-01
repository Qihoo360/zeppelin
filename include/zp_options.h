#ifndef ZP_OPTIONS_H
#define ZP_OPTIONS_H

#include <string>
#include <vector>

class Server;
struct ZPOptions;

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

struct ZPOptions {
  std::vector<std::string> meta_addr;

  std::string local_ip;
  int local_port;

  // TODO session timeout
  int64_t timeout;

  std::string data_path;
  std::string log_path;

  //std::vector<Server> servers;

  ZPOptions();

  ZPOptions(const ZPOptions& options);

  void Dump();
};

#endif
