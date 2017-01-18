/*
 * "Copyright [2016] qihoo"
 * "Author <hrxwwd@163.com>"
 */
#include "include/zp_table.h"


namespace libzp {

const int kDataConnTimeout =  20000000;
Node::Node() {
}

Node::Node(const std::string& other_ip, int other_port) :
  ip(other_ip),
  port(other_port) {
}

Node::Node(const Node& other) :
  ip(other.ip),
  port(other.port) {
}

Node::~Node() {
}

Node& Node::operator = (const Node& other) {
  ip = other.ip;
  port = other.port;
  return *this;
}

bool Node::operator < (const Node& other) const {
  std::string my_val = ip + std::to_string(port);
  std::string other_val = other.ip + std::to_string(other.port);
  if (my_val < other_val) {
    return true;
  }
  return false;
}

bool Node::operator == (const Node& other) const {
  if (ip == other.ip && port == other.port) {
    return true;
  }
  return false;
}

Table::Table(const ZPMeta::Table& table_info) {
  table_name_ = table_info.name();
  partition_num_ = table_info.partitions_size();
  ZPMeta::Partitions partition_info;
  for (int i = 0; i < table_info.partitions_size(); i++) {
    partition_info = table_info.partitions(i);
    Partition* par = new Partition(partition_info);
    partitions_.insert(std::make_pair(partition_info.id(), par));
  }
}

Table::~Table() {
  std::map<int, Partition*>::iterator iter = partitions_.begin();
  while (iter != partitions_.end()) {
    delete iter->second;
    iter++;
  }
}

Node Table::GetKeyMaster(const std::string& key) {
  int par_num = std::hash<std::string>()(key) % partitions_.size();
  std::map<int, Partition*>::iterator iter = partitions_.find(par_num);
  if (iter != partitions_.end()) {
    return iter->second->master;
  } else {
    return Node();
  }
}

const Table::Partition* Table::GetPartition(const std::string& key) {
  int par_num = std::hash<std::string>()(key) % partitions_.size();
  std::map<int, Partition*>::iterator iter = partitions_.find(par_num);
  if (iter != partitions_.end()) {
    return iter->second;
  } else {
    return NULL;
  }
}

void Table::DebugDump() {
  std::cout << "  name: "<< table_name_ <<std::endl;
  std::cout << "  partition: "<< partition_num_ <<std::endl;
  auto par = partitions_.begin();
  while (par != partitions_.end()) {
    std::cout << "    partition: "<< par->second->id;
    std::cout << "    master: " << par->second->master.ip
      << " : " << par->second->master.port << std::endl;
    par++;
  }
}

void Table::GetNodes(std::vector<Node>* nodes) {
  auto par = partitions_.begin();
  while (par != partitions_.end()) {
    nodes->push_back(par->second->master);
    par++;
  }
  std::sort(nodes->begin(), nodes->end());
  nodes->erase(
      std::unique(nodes->begin(),
                  nodes->end()),
      nodes->end());
}

}  // namespace libzp
