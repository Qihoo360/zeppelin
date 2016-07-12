#include <stdio.h>
#include <iostream>
#include <string>

#include "client.h"

using namespace std;

using slash::Status;

//struct option const long_options[] = {
//  {"servers", required_argument, NULL, 's'},
//  {NULL, 0, NULL, 0}, };

//const char* short_options = "s:";

void handle_update(client::Cluster &cluster, int port1, int port2, int port3) {
  int pid;
  std::string ip1 = "127.0.0.1";
  //int port1 = 8001;
  std::string ip2 = "127.0.0.1";
  //int port2 = 8002;
  std::string ip3 = "127.0.0.1";
  //int port3 = 8003;
  //scanf ("%d, %s:%d, %s:%d, %s:%d}", &pid, ip1, &port1, ip2, &port2, ip3, &port3);

  ZPMeta::MetaCmd request;
  request.set_type(ZPMeta::MetaCmd_Type::MetaCmd_Type_UPDATE);

  ZPMeta::MetaCmd_Update* update = request.mutable_update();
  ZPMeta::Partitions* partition = update->add_info();
  partition->set_id(pid);
  ZPMeta::Node* master = partition->mutable_master();
  master->set_ip(ip1);
  master->set_port(port1);

  ZPMeta::Node* slave = partition->add_slaves();
  slave->set_ip(ip2);
  slave->set_port(port2);

  slave = partition->add_slaves();
  slave->set_ip(ip3);
  slave->set_port(port3);

  ZPMeta::MetaCmdResponse response;
  Status s = cluster.Update(request, response, ip1, port1 + 100);
  printf ("Handle update(%s:%d) %s\n\n", ip1.c_str(), port1 + 100, s.ToString().c_str());

  s = cluster.Update(request, response, ip2, port2 + 100);
  printf ("Handle update(%s:%d) %s\n\n", ip2.c_str(), port2 + 100, s.ToString().c_str());

  s = cluster.Update(request, response, ip3, port3 + 100);
  printf ("Handle update(%s:%d) %s\n\n", ip3.c_str(), port3 + 100, s.ToString().c_str());
}

int main(int argc, char* argv[]) {
  client::Option option;

  option.ParseFromArgs(argc, argv);

  client::Cluster cluster(option);

  handle_update(cluster, 8001, 8002, 8003);

  sleep(30);
  handle_update(cluster, 8002, 8001, 8003);

  while (1) {
    sleep(1);
  }

//  char cmd[30];
//  while (1) {
//    printf ("=========FakeMeta Send cmd=====\n"
//            "1. INPUT: update {1, ip1:port1, ip2:port2, ip3:port3}\n"
//            "2. exit\n"
//            );  
//
//    scanf ("%s", cmd);
//    if (strcmp(cmd, "update") == 0) {
//      handle_update(cluster);
//    } else if (strcmp(cmd, "exit") == 0) {
//      break;
//    } else {
//      printf ("Wrong input: (%s)\n", cmd);
//    }
//
//    sleep(1);
//  }
//
  // Test API
//  printf ("\n=====Test Set==========\n");
//
//  std::string key = "test_key";
//  std::string value = "test_value";
//  
//  Status result = cluster.Set(key, value);
//  if (result.ok()) {
//    printf ("Set ok\n");
//  } else {
//    printf ("Set failed, %s\n", result.ToString().c_str());
//  }
//
//  printf ("\n=====Test Get==========\n");
//
//  result = cluster.Get(key, &value);
//  if (result.ok()) {
//    printf ("Get ok, value is %s\n", value.c_str());
//  } else {
//    printf ("Get failed, %s\n", result.ToString().c_str());
//  }
//
  cout << "success" << endl;
  return 0;
}
