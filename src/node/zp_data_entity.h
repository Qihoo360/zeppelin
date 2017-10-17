// Copyright 2017 Qihoo
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http:// www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
#ifndef SRC_NODE_ZP_DATA_ENTITY_H_
#define SRC_NODE_ZP_DATA_ENTITY_H_
#include <string>
#include <ostream>

class Node  {
 public:
  std::string ip;
  int port;

  Node()
    : port(0) {}
  Node(const std::string& _ip, const int& _port)
    : ip(_ip), port(_port) {}
  Node(const Node& node)
    : ip(node.ip),
    port(node.port) {}

  Node& operator=(const Node& node) {
    ip = node.ip;
    port = node.port;
    return *this;
  }

  bool empty() {
    return (ip.empty() || port == 0);
  }

  bool operator== (const Node& rhs) const {
    return (ip == rhs.ip && port == rhs.port);
  }
  bool operator!= (const Node& rhs) const {
    return (ip != rhs.ip || port != rhs.port);
  }
  bool operator< (const Node& rhs) const {
    return (ip < rhs.ip ||
        (ip == rhs.ip && port < rhs.port));
  }

  friend std::ostream& operator<< (std::ostream& stream, const Node& node) {
    stream << node.ip << ":" << node.port;
    return stream;
  }
};

#endif  // SRC_NODE_ZP_DATA_ENTITY_H_
