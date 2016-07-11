#ifndef ZP_META_UTILS_H
#define ZP_META_UTILS_H

const int kReplicaNum = 3;

// TODO 
class Node;
//class Leaders;

class Node {
 public:
  std::string ip;
  int port;

  // colon separated ip:port
  Node() {}
  Node(const std::string& str);
  Node(const std::string& _ip, const int& _port) : ip(_ip), port(_port) {}

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
};

//class Leaders {
//  Node primary;
//  Node secondary[kReplicaNum - 1];
//};

// Slave item
struct SlaveItem {
  Node node;
  pthread_t sender_tid;
  int sync_fd;
  void* sender;
  struct timeval create_time;
};

//struct ClientInfo {
//  int fd;
//  std::string ip_port;
//  int last_interaction;
//};
//

#endif
