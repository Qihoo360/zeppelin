#ifndef ZP_META_UTILS_H
#define ZP_META_UTILS_H

const unsigned kReplicaNum = 3;

// TODO 
class Node;
//class Leaders;

#include <stdio.h>
#include <iostream>
#include <ostream>

class Node {
 public:
  std::string ip;
  int port;

  // colon separated ip:port
  Node() {}
  Node(const std::string& str);
  Node(const std::string& _ip, const int& _port) : ip(_ip), port(_port) {}
  // TODO test
 // ~Node() {
 //   printf ("~Node dstor: ip=%s, port=%d\n", ip.c_str(), port);
 // }

  Node(const Node& node)
      : ip(node.ip),
      port(node.port) {}

  Node& operator=(const Node& node) {
    ip = node.ip;
    port = node.port;
    return *this;
  }

  bool operator==(const Node& rhs) const {
    return (ip == rhs.ip && port == rhs.port);
  }
  bool operator!=(const Node& rhs) const {
    return (ip != rhs.ip || port != rhs.port);
  }

  friend std::ostream& operator<<(std::ostream& stream, const Node& node) {
    stream << node.ip << ":" << node.port;
    return stream;
  }
};

//class Leaders {
//  Node primary;
//  Node secondary[kReplicaNum - 1];
//};


//struct ClientInfo {
//  int fd;
//  std::string ip_port;
//  int last_interaction;
//};
//

#endif
