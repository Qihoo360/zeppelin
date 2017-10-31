// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#include <fstream>
#include <iostream>
#include <vector>
#include <deque>
#include <map>
#include <cstdlib>
#include <ctime>

using namespace std;

struct Host {
  // ip:port
  std::string host;
  // host id
  int host_id;
  // cabinet id
  int cab_id;
};

std::vector<std::deque<Host> > cabinets;
std::vector<std::vector<Host> > result;

const int kMaxRetry = 100;

bool Load(const std::string& file) {
  ifstream in(file);
  if (!in.is_open()) {
    std::cout << "Open Failed" << std::endl;
    return false;
  }

  std::vector<std::map<std::string, std::vector<Host> > > cabs;
  std::map<std::string, std::vector<Host> > hosts_map;
  std::vector<Host> nodes;
  std::string ip;
  int host_id = 0;
  int cab_id = 0;
  char buf[1024];
  while (!in.eof()) {
    in.getline(buf, sizeof(buf));
    if (buf[0] == '\0') {
      continue;
    } else if (buf[0] != ' ') {
      cab_id++;
      if (!nodes.empty()) {
        hosts_map.insert(std::map<std::string, std::vector<Host> >::
            value_type(ip, nodes));
      }
      nodes.clear();
      if (!hosts_map.empty()) {
        cabs.push_back(hosts_map);
        hosts_map.clear();
      }
    } else if (buf[3] == ' ') {
      nodes.push_back({ip + ":" + std::string(buf + 4), host_id, cab_id});
    } else if (buf[1] == ' ') {
      if (!nodes.empty()) {
        hosts_map.insert(std::map<std::string, std::vector<Host> >::
            value_type(ip, nodes));
      }
      ip = std::string(buf + 2);
      nodes.clear();
      host_id++;
    }
  }
  if (!nodes.empty()) {
    hosts_map.insert(std::map<std::string, std::vector<Host> >::
        value_type(ip, nodes));
  }
  nodes.clear();
  if (!hosts_map.empty()) {
    cabs.push_back(hosts_map);
    hosts_map.clear();
  }

//  std::cout << "Loading from file..." << std::endl;
//  std::cout << "Raw info" << std::endl;
//  std::cout << "-------------------------------------" << endl;
//  for (auto& c : cabs) {
//    for (auto& h : c) {
//      for (auto& n : h.second) {
//        std::cout << n.host << " " << n.cab_id << " " << n.host_id << std::endl;
//      }
//    }
//    std::cout << "++++++++++++++++++++" << std::endl;
//  }
//  std::cout << "-------------------------------------" << endl;

  for (auto& c : cabs) {
    std::deque<Host> host;
    size_t i = 0;
    while (true) {
      bool finished = true;
      for (auto& h : c) {
        if (h.second.size() >= (i + 1)) {
          host.push_back(h.second[i]);
          finished = false;
        }
      }
      if (finished) {
        cabinets.push_back(host);
        host.clear();
        break;
      }
      i++;
    }
  }

//  std::cout << "Sorted info" << std::endl;
//  std::cout << "-------------------------------------" << endl;
//  for (auto& c : cabinets) {
//    for (auto& n : c) {
//      std::cout << n.host << " " << n.cab_id << " " << n.host_id << std::endl;
//    }
//    std::cout << "++++++++++++++++++++" << std::endl;
//  }
//  std::cout << "-------------------------------------" << endl;
  return true;
}

void PickIndexFromAtLeast3Cabs(std::vector<std::deque<Host> > cab,
    int* idx) {
  idx[0] = -1;
  idx[1] = -1;
  idx[2] = -1;
  /*
   * Get the First Index
   */
  size_t max_size = 0;
  for (size_t i = 0; i < cab.size(); i++) {
    if (cab[i].size() > max_size) {
      idx[0] = i;
      max_size = cab[i].size();
    }
  }
  // The number of available cabinets is smaller than 1
  if (idx[0] == -1) {
    return;
  }

  /*
   * Get the Second Index
   */
  max_size = 0;
  for (int i = 0; i < static_cast<int>(cab.size()); i++) {
    if (cab[i].size() > max_size && i != idx[0]) {
      idx[1] = i;
      max_size = cab[i].size();
    }
  }

  // The number of available cabinets is smaller than 2
  if (idx[1] == -1) {
    return;
  }

  /*
   * Get Third Index
   */
  max_size = 0;
  for (int i = 0; i < static_cast<int>(cab.size()); i++) {
    if (cab[i].size() > max_size &&
        i != idx[0] && i != idx[1]) {
      idx[2] = i;
      max_size = cab[i].size();
    }
  }
  // if we can not found valid idx[2], idx[2] = -1;
}

void PickIndexFrom2Cabs(std::vector<std::deque<Host> > cab,
    int* idx) {
  idx[0] = -1;
  idx[1] = -1;
  idx[2] = -1;

  /*
   * Get the First Index
   */
  size_t max_size = 0;
  for (size_t i = 0; i < cab.size(); i++) {
    if (cab[i].size() > max_size) {
      idx[0] = i;
      max_size = cab[i].size();
    }
  }

  /*
   * Get the Second Index
   */
  for (size_t i = 0; i < cab.size(); i++) {
    if (!cab[i].empty() && static_cast<int>(i) != idx[0]) {
      idx[1] = i;
      break;
    }
  }
  // if we could not get the 2nd index, there must have 1
  // available cabinet at most, return
  if (idx[1] == -1) {
    return;
  }

  // Get the Third Index
  if (cab[idx[0]].size() > 1) {
    idx[2] = idx[0];
  } else if (cab[idx[1]].size() > 1) {
    idx[2] = idx[1];
  }
}

void Distribution() {
  std::vector<std::deque<Host> > cab = cabinets;
  int idx[3] = {-1, -1, -1};
  /*
   * 1. Try to get indexes from all cabinets
   * (suppose the number of available cabinets >= 3)
   */
  std::vector<Host> partition;
  while (true) {
    partition.clear();
    PickIndexFromAtLeast3Cabs(cab, idx);
    // we could not get enough indexes from 3 different cabinets, break
    if (idx[2] == -1) {
      break;
    }
    for (int i = 0; i < 3; i++) {
      partition.push_back(cab[idx[i]].front());
//      std::cout << cab[idx[i]].front().host << " ";
      cab[idx[i]].pop_front();
    }
    result.push_back(partition);
//    std::cout << std::endl;
  }
  /*
   * 2. if the second index is valid, there must have 2 available cabinets
   * so we continue to pick indexes from the 2 available cabinets
   */
  if (idx[1] != -1) {
    while (true) {
      partition.clear();
      PickIndexFrom2Cabs(cab, idx);
      // we could not get the 2nd index, break
      // and continue to try to get indexes from 1 cabinet
      if (idx[1] == -1) {
        break;
      } else if (idx[2] == -1) {
        // we only get 2 indexes from two cabinets, and we need to pick the
        // third index randomly
        int next_cab_idx = -1;
        int next_node_idx = -1;
        int retry_times = 0;
        // 1. first, try to pick the third index from other cabinets that
        // different from first two indexes
        while (retry_times < kMaxRetry) {
          next_cab_idx = std::rand() % cabinets.size();
          if (next_cab_idx != idx[0] && next_cab_idx != idx[1]) {
            next_node_idx = std::rand() % cabinets[next_cab_idx].size();
            break;
          }
          retry_times++;
        }
        // 2. there is only two cabinets in total, so we need to pick the
        // third index from random cabinet that is same with one of the
        // first two indexes
        if (retry_times == kMaxRetry) {
          next_cab_idx = std::rand() % 2 == 0 ? idx[0] : idx[1];
          retry_times = 0;
          // 2-1. try to pick the third index from different host
          while (retry_times < kMaxRetry) {
            next_node_idx = std::rand() % cabinets[next_cab_idx].size();
            if (cabinets[next_cab_idx][next_node_idx].host_id !=
                cab[next_cab_idx].front().host_id) {
              break;
            }
            retry_times++;
          }
          // 2-2. try to pick the third index from different node [same host]
          if (retry_times == kMaxRetry) {
            retry_times = 0;
            while (retry_times < kMaxRetry) {
              next_node_idx = std::rand() % cabinets[next_cab_idx].size();
              if (cabinets[next_cab_idx][next_node_idx].host !=
                  cab[next_cab_idx].front().host) {
                break;
              }
              retry_times++;
            }
          }
        }
        partition.push_back(cab[idx[0]].front());
        cab[idx[0]].pop_front();
        partition.push_back(cab[idx[1]].front());
        cab[idx[1]].pop_front();
        partition.push_back(cabinets[next_cab_idx][next_node_idx]);
      } else {
        for (int i = 0; i < 3; i++) {
          partition.push_back(cab[idx[i]].front());
  //        std::cout << cab[idx[i]].front().host << " ";
          cab[idx[i]].pop_front();
        }
      }
      result.push_back(partition);
//      std::cout << std::endl;
    }
  }

  /*
   * 3. Try to get indexes from only 1 cabinet
   * (suppose the number of available cabinets == 1)
   */
  if (idx[0] != -1) {
    // 3-1. try to get 3 indexes
    // [from maybe 3 different hosts or not]
    while (true) {
      partition.clear();
      if (cab[idx[0]].size() >= 3) {
        for (int times = 0; times < 3; times++) {
          partition.push_back(cab[idx[0]].front());
//          std::cout << cab[idx[0]].front().host << " ";
          cab[idx[0]].pop_front();
        }
        result.push_back(partition);
//        std::cout << std::endl;
      } else {
        break;
      }
    }
    // 3-2. only have 2 nodes to pick 2 indexes, we need to
    // get the last index randomly
    if (cab[idx[0]].size() == 2) {
      partition.clear();
      // 3-2-1. get the 1st & 2nd indexes first
      for (int times = 0; times < 2; times++) {
        // Do not pop here, need to use the element later
        // std::cout << cab[idx[0]].front().host << " ";
        // cab[idx[0]].pop_front();
        partition.push_back(cab[idx[0]][times]);
//        std::cout << cab[idx[0]][times].host << " ";
      }
      // 3-2-2. get the 3rd index
      int next_cab_idx = -1;
      int next_node_idx = -1;
      // we have at least 2 cabinets, so we can get the
      // last index from the different cabinet
      if (cabinets.size() > 1) {
        do {
          next_cab_idx = std::rand() % cabinets.size();
        } while (next_cab_idx == idx[0]);
        next_node_idx = std::rand() % cabinets[next_cab_idx].size();
        partition.push_back(cabinets[next_cab_idx][next_node_idx]);
//        std::cout << cabinets[next_cab_idx][next_node_idx].host << std::endl;
      } else {
        // we only have 1 cabinet, so we have to get the last
        // index from the same cabinet, but we try to get the
        // last index from the different hosts
        next_cab_idx = idx[0];
        int retry_times = 0;
        while (retry_times < kMaxRetry) {
          next_node_idx = std::rand() % cabinets[next_cab_idx].size();
          if (cabinets[next_cab_idx][next_node_idx].host_id !=
              cab[idx[0]][0].host_id &&
              cabinets[next_cab_idx][next_node_idx].host_id !=
              cab[idx[0]][1].host_id) {
            break;
          }
          retry_times++;
        }
        // After random picking, we still have replicaset on the same host,
        // so we need to continue to pick randomly, making replicaset
        // on same host but different nodes
        if (retry_times == kMaxRetry) {
          retry_times = 0;
          while (retry_times < kMaxRetry) {
            next_node_idx = std::rand() % cabinets[next_cab_idx].size();
            if (cabinets[next_cab_idx][next_node_idx].host !=
                cab[idx[0]][0].host &&
                cabinets[next_cab_idx][next_node_idx].host !=
                cab[idx[0]][1].host) {
              break;
            }
            retry_times++;
          }
        }
        partition.push_back(cabinets[next_cab_idx][next_node_idx]);
//        std::cout << cabinets[next_cab_idx][next_node_idx].host << std::endl;
      }
      result.push_back(partition);
    } else if (cab[idx[0]].size() == 1) {
      partition.clear();
    // 3-3. only have 1 node to pick 1 index, we need to
    // get the 2nd & 3rd indexes randomly
      int next_cab_idx = idx[0];
      int next_node_idx = -1;
      int idx_1_host_id = cab[idx[0]].front().host_id;
      int idx_2_host_id = -1;
      partition.push_back(cab[idx[0]].front());
//      std::cout << cab[idx[0]].front().host << " ";
      cab[idx[0]].pop_front();
      // 3-3-1. if we have 3 cabinets, we can pick the
      // 2nd & 3rd from two different cabinets
      if (cabinets.size() >= 3) {
        // get the 2nd index
        do {
          next_cab_idx = std::rand() % cabinets.size();
        } while (next_cab_idx == idx[0]);
        int the_2nd_cab_idx = next_cab_idx;
        next_node_idx = std::rand() % cabinets[next_cab_idx].size();
        partition.push_back(cabinets[next_cab_idx][next_node_idx]);
//        std::cout << cabinets[next_cab_idx][next_node_idx].host << std::endl;

        // get the 3rd index
        do {
          next_cab_idx = std::rand() % cabinets.size();
        } while (next_cab_idx == idx[0] || next_cab_idx == the_2nd_cab_idx);
        next_node_idx = std::rand() % cabinets[next_cab_idx].size();
        partition.push_back(cabinets[next_cab_idx][next_node_idx]);
//        std::cout << cabinets[next_cab_idx][next_node_idx].host << std::endl;
      } else if (cabinets.size() == 2) {
        // 3-3-2. get the 2nd index from the other cabinet
        next_cab_idx = idx[0] == 0 ? 1 : 0;
        next_node_idx = std::rand() % cabinets[next_cab_idx].size();
        partition.push_back(cabinets[next_cab_idx][next_node_idx]);
//        std::cout << cabinets[next_cab_idx][next_node_idx].host << std::endl;

        // get the 3rd index from the same cabinet with 1st index,
        // try to pick the 3rd from the different host
        int retry_times = 0;
        while (retry_times < kMaxRetry) {
          next_node_idx = std::rand() % cabinets[idx[0]].size();
          if (cabinets[next_cab_idx][next_node_idx].host_id != idx_1_host_id) {
            break;
          }
          retry_times++;
        }
        // After random picking, we still have replicaset on the same host,
        // so we need to continue to pick randomly, making replicaset
        // on same host but different nodes
        if (retry_times == kMaxRetry) {
          retry_times = 0;
          while (retry_times < kMaxRetry) {
            next_node_idx = std::rand() % cabinets[idx[0]].size();
            if (cabinets[next_cab_idx][next_node_idx].host != partition[0].host) {
              break;
            }
            retry_times++;
          }
        }
        partition.push_back(cabinets[next_cab_idx][next_node_idx]);
//        std::cout << cabinets[next_cab_idx][next_node_idx].host << " ";
      } else {
        // 3-3-3. get the 2nd & 3rd indexes from the same cabinet
        // try to pick from different host

        // get the 2nd
        int retry_times = 0;
        while (retry_times < kMaxRetry) {
          next_node_idx = std::rand() % cabinets[next_cab_idx].size();
          if (cabinets[next_cab_idx][next_node_idx].host_id != idx_1_host_id) {
            idx_2_host_id = cabinets[next_cab_idx][next_node_idx].host_id;
            break;
          }
          retry_times++;
        }
        // After random picking, we still have replicaset on the same host,
        // so we need to continue to pick randomly, making replicaset
        // on same host but different nodes
        if (retry_times == kMaxRetry) {
          retry_times = 0;
          while (retry_times < kMaxRetry) {
            next_node_idx = std::rand() % cabinets[next_cab_idx].size();
            if (cabinets[next_cab_idx][next_node_idx].host !=
                partition[0].host) {
              idx_2_host_id = cabinets[next_cab_idx][next_node_idx].host_id;
              break;
            }
            retry_times++;
          }
        }
        partition.push_back(cabinets[next_cab_idx][next_node_idx]);
//        std::cout << cabinets[next_cab_idx][next_node_idx].host << " ";

        // get the 3rd
        retry_times = 0;
        while (retry_times < kMaxRetry) {
          next_node_idx = std::rand() % cabinets[next_cab_idx].size();
          if (cabinets[next_cab_idx][next_node_idx].host_id != idx_1_host_id &&
              cabinets[next_cab_idx][next_node_idx].host_id != idx_2_host_id) {
            break;
          }
          retry_times++;
        }
        // After random picking, we still have replicaset on the same host,
        // so we need to continue to pick randomly, making replicaset
        // on same host but different nodes
        if (retry_times == kMaxRetry) {
          retry_times = 0;
          while (retry_times < kMaxRetry) {
            next_node_idx = std::rand() % cabinets[next_cab_idx].size();
            if (cabinets[next_cab_idx][next_node_idx].host !=
                partition[0].host &&
                cabinets[next_cab_idx][next_node_idx].host !=
                partition[1].host) {
              idx_2_host_id = cabinets[next_cab_idx][next_node_idx].host_id;
              break;
            }
            retry_times++;
          }
        }
        partition.push_back(cabinets[next_cab_idx][next_node_idx]);
//        std::cout << cabinets[next_cab_idx][next_node_idx].host << " ";
      }
      result.push_back(partition);
    }
  }
}

void Checkup() {
  int repl_3_in_1_host = 0;
  int repl_2_in_1_host = 0;
  int repl_3_in_1_cab = 0;
  int repl_2_in_1_cab = 0;
  int repl_3_in_1_node = 0;
  int repl_2_in_1_node = 0;
  int partition_num = 0;

  for (auto&p : result) {
    partition_num++;
    // 1. checkup node
    if (p[0].host == p[1].host && p[0].host == p[2].host) {
      repl_3_in_1_node++;
    } else if (p[0].host == p[1].host || p[0].host == p[2].host ||
        p[1].host == p[2].host) {
      repl_2_in_1_node++;
    }

    // 2. checkup host
    if (p[0].host_id == p[1].host_id && p[0].host_id == p[2].host_id) {
      repl_3_in_1_host++;
    } else if (p[0].host_id == p[1].host_id || p[0].host_id == p[2].host_id ||
        p[1].host_id == p[2].host_id) {
      repl_2_in_1_host++;
    }

    // 3. checkup cab
    if (p[0].cab_id == p[1].cab_id && p[0].cab_id == p[2].cab_id) {
      repl_3_in_1_cab++;
    } else if (p[0].cab_id == p[1].cab_id || p[0].cab_id == p[2].cab_id ||
        p[1].cab_id == p[2].cab_id) {
      repl_2_in_1_cab++;
    }
  }

  std::map<std::string, int> pcount_map;
  for (auto&p : result) {
    for (int idx = 0; idx < 3; idx++) {
      auto iter = pcount_map.find(p[idx].host);
      if (iter != pcount_map.end()) {
        iter->second++;
      } else {
        pcount_map.insert(std::map<std::string, int>::
            value_type(p[idx].host, 1));
      }
    }
  }
  std::map<int, int> result_map;

  int node_num = 0;
  for (auto&c : cabinets) {
    for (auto&h : c) {
      node_num++;
      auto iter = pcount_map.find(h.host);
      if (iter != pcount_map.end()) {
        auto it = result_map.find(iter->second);
        if (it != result_map.end()) {
          it->second++;
        } else {
          result_map.insert(std::map<int, int>::
              value_type(iter->second, 1));
        }
      } else {
        auto it = result_map.find(0);
        if (it != result_map.end()) {
          it->second++;
        } else {
          result_map.insert(std::map<int, int>::
              value_type(0, 1));
        }
      }
    }
  }

  std::cout << "+++++++++++++++++++++++++++++++++++++" << endl;
  std::cout << "Checkup distribution result" << std::endl;
  std::cout << "-------------------------" << endl;
  std::cout << "Node Num: " << node_num << std::endl;
  std::cout << "Partition Num: " << partition_num << std::endl;
  std::cout << "3 replicas in 1 node: " << repl_3_in_1_node << std::endl;
  std::cout << "2 replicas in 1 node: " << repl_2_in_1_node << std::endl;
  std::cout << "3 replicas in 1 host: " << repl_3_in_1_host << std::endl;
  std::cout << "2 replicas in 1 host: " << repl_2_in_1_host << std::endl;
  std::cout << "3 replicas in 1 cabinet: " << repl_3_in_1_cab << std::endl;
  std::cout << "2 replicas in 1 cabinet: " << repl_2_in_1_cab << std::endl;
  std::cout << "-------------------------" << endl;
  for (auto&r : result_map) {
    std::cout << r.first << " partitions node num: " << r.second << std::endl;
  }
  std::cout << "+++++++++++++++++++++++++++++++++++++" << endl;
}

void Display() {
  ofstream out("./distribution_result");
  if (!out.is_open()) {
    std::cout << "Open output file failed" << std::endl;
    return;
  }
  int p_num = 0;
  for (auto& p : result) {
    std::string content = std::to_string(p_num++);
    content.append(": ");
    for (auto&h : p) {
      content.append(h.host);
      content.append(" ");
    }
    if (content.back() == ' ') {
      content.pop_back();
    }
    out << content << '\n';
  }
  std::cout << "Distribution done!!! Write result to "
    "./distribution_result" << std::endl;
}

void Usage() {
  std::cout << "Usage: ./distribution path_to_cluster_file "
    "[partitions_per_node = 3]" << std::endl;
}

int main(int argc, char** argv) {
  if (argc != 2 && argc != 3) {
    Usage();
    return -1;
  }
  std::string file = argv[1];
  if (file == "-h") {
    Usage();
    return 0;
  }
  if (!Load(file)) {
    return -1;
  }
  int partition_per_node = 3;
  if (argc == 3) {
    partition_per_node = std::atoi(argv[2]);
  }
  std::srand(std::time(0));
  std::cout << "Distribution start..." << std::endl;
  for (int times = 0; times < partition_per_node; times++) {
    Distribution();
  }
  Display();
  Checkup();
  return 0;
}
