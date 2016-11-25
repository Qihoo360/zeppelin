/*
 * "Copyright [2016] <hrxwwd@163.com>"
 */
#include <string>
#include <vector>
#include <iostream>
#include <algorithm>

#include "include/zp_cluster.h"

void usage() {
  std::cout << "usage:\n"
            << "      zp_cli host port\n";
}

int main(int argc, char* argv[]) {
  if (argc != 3) {
    usage();
    return -1;
  }
  std::cout << "start" << std::endl;
  libZp::Options option;
  libZp::IpPort ipPort = libZp::IpPort(argv[1], atoi(argv[2]));
  option.meta_addr.push_back(ipPort);

  // cluster handle cluster operation
  std::cout << "create cluster" << std::endl;
  libZp::Cluster cluster = libZp::Cluster(option);
  std::cout << "connect cluster" << std::endl;
  // needs connect to cluster first
  libZp::Status s = cluster.Connect();
  if (!s.ok()) {
    std::cout << s.ToString() << std::endl;
    exit(-1);
  }
  std::cout << "create table" << std::endl;
  std::string table_name = "test2";
  s = cluster.CreateTable(table_name, 3);
  s = cluster.CreateTable(table_name, 6);
  if (!s.ok()) {
    std::cout << s.ToString() << std::endl;
    exit(-1);
  }

  /*
  Status s = cluster.ListMetaNode(node_list);
  node_list.clear();
  s = cluster.ListDataNode(node_list);
  // ioctx handle table operation and set/get
  libZp::Ioctx ioctx = cluster.CreateIoctx("test_pool");
  std::vector<std::pair<std::string, std::int>> node_list;
  s = ioctx.set("key","value");
  std::string val;
  s = ioctx.get("key",val);
  */
}
