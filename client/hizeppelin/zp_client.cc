/*
 * "Copyright [2016] qihoo"
 * "Author <hrxwwd@163.com>"
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
  libzp::Options option;
  libzp::IpPort ip_port(argv[1], atoi(argv[2]));
  option.meta_addr.push_back(ip_port);

  // cluster handle cluster operation
  std::cout << "create cluster" << std::endl;
  libzp::Cluster* cluster = new libzp::Cluster(option);
  std::cout << "connect cluster" << std::endl;
  // needs connect to cluster first
  libzp::Status s = cluster->Connect();
  /* operation
  s = cluster->CreateTable("test", 24);
  */
  while (true) {
    s = cluster->Pull("test");
    if (!s.ok()) {
      std::cout << s.ToString() << std::endl;
    }
    s = cluster->Set("test", "key", "value");
    if (!s.ok()) {
      std::cout << s.ToString() << std::endl;
    }
    usleep(100);
  }
  delete cluster;
}
