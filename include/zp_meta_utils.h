#ifndef ZP_META_UTILS_H
#define ZP_META_UTILS_H

const int kReplicaNum = 3;

// TODO 
class Node;
//class Leaders;

#include <stdio.h>

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

  SlaveItem()
    : node(),
    sender(NULL) {}
  SlaveItem(const SlaveItem& item)
    : node(item.node),
    sender_tid(item.sender_tid),
    sender(item.sender),
    create_time(item.create_time) {
      //printf ("SlaveItem Cpy cstor called\n");
    }

  //~SlaveItem() {
  //  printf ("~SlaveItem called, node address=(%x)\n", &node);
  //}
};

//struct ClientInfo {
//  int fd;
//  std::string ip_port;
//  int last_interaction;
//};
//

#endif
