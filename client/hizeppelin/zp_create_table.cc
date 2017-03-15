/*
 * "Copyright [2016] qihoo"
 */
#include <string>
#include <vector>
#include <iostream>
#include <algorithm>
#include <unistd.h>

#include "include/zp_cluster.h"


void usage() {
  std::cout << "usage:\n"
            << "      zp_create_table host port table_num partition_num\n";
}

int main(int argc, char* argv[]) {
  if (argc != 5) {
    usage();
    return -1;
  }

  libzp::Cluster* cluster = new libzp::Cluster(argv[1], atoi(argv[2]));

  libzp::Status s;
  int cnt = atoi(argv[3]);
  int partition_num = atoi(argv[4]);
  if (!s.ok()) {
    std::cout << s.ToString() << std::endl;
    return -1;
  }

  for(int i = 0; i < cnt; i++) {
    s = cluster->Connect();
    if (!s.ok()) {
      std::cout << "Connect cluster failed\n";
      break;
    }
    // needs connect to cluster first
    std::string table = "table" + std::to_string(i);
    std::cout << "Start " << i << std::endl;
    s = cluster->CreateTable(table, partition_num);
    if (!s.ok()) {
      std::cout << "Create table " << table << "  failed, " << s.ToString() << std::endl;
      continue;
    }

    libzp::Status s;
    int retry = 10;
    for (int j = 0; j < retry && !s.ok(); j++) {
      // client handle io operation
      //std::cout << "create client" << std::endl;
      libzp::Client* client = new libzp::Client(argv[1], atoi(argv[2]), table);
      s = client->Connect();
      if (!s.ok()) {
        std::cout << "connect table failed, " << s.ToString() << std::endl;
        sleep(1);
        continue;
      }

      std::string key = "key" + std::to_string(i);
      s = client->Set(key, "value");
      if (!s.ok()) {
        std::cout << s.ToString() << std::endl;
      } else {
        std::cout << "Table:" << table << " set key ok" << std::endl;
      }
      delete client;
    }
  }

  delete cluster;

  return 0;
}
