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
  int id;
};

std::vector<std::deque<Host> > cabinets;

bool Load() {
  ifstream in("./cluster");
  if (!in.is_open()) {
    std::cout << "Open Failed" << std::endl;
    return false;
  }

  std::vector<std::map<std::string, std::vector<Host> > > cabs;
  std::map<std::string, std::vector<Host> > hosts_map;
  std::vector<Host> nodes;
  std::string ip;
  int host_id = 0;
  char buf[1024];
  while (!in.eof()) {
    in.getline(buf, sizeof(buf));
    if (buf[0] == '\0') {
      continue;
    } else if (buf[0] != ' ') {
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
      nodes.push_back({ip + ":" + std::string(buf + 4), host_id});
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

//  for (auto& c : cabs) {
//    for (auto& h : c) {
//      for (auto& n : h.second) {
//        std::cout << n.host << " " << n.id << std::endl;
//      }
//    }
//    std::cout << "---" << std::endl;
//  }

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

  for (auto& c : cabinets) {
    for (auto& n : c) {
      std::cout << n.host << " " << n.id << std::endl;
    }
    std::cout << "---" << std::endl;
  }
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
  size_t i = 0;
  for (i = 0; i < cab.size(); i++) {
    if (!cab[i].empty()) {
      idx[0] = i;
      i++;
      break;
    }
  }

  /*
   * Get the Second Index
   */
  for (i = i; i < cab.size(); i++) {
    if (!cab[i].empty()) {
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
  while (true) {
    PickIndexFromAtLeast3Cabs(cab, idx);
    // we could not get enough indexes from 3 different cabinets, break
    if (idx[2] == -1) {
      break;
    }
    for (int i = 0; i < 3; i++) {
      std::cout << cab[idx[i]].front().host << " ";
      cab[idx[i]].pop_front();
    }
    std::cout << std::endl;
  }
  /*
   * 2. if the second index is valid, there must have 2 available cabinets 
   * so we continue to pick indexes from the 2 available cabinets
   */
  if (idx[1] != -1) {
    while (true) {
      PickIndexFrom2Cabs(cab, idx);
      // we could not get the 2nd index, break
      // and continue to try to get indexes from 1 cabinet
      if (idx[1] == -1) {
        break;
      }
      for (int i = 0; i < 3; i++) {
        std::cout << cab[idx[i]].front().host << " ";
        cab[idx[i]].pop_front();
      }
      std::cout << std::endl;
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
      if (cab[idx[0]].size() >= 3) {
        for (int times = 0; times < 3; times++) {
          std::cout << cab[idx[0]].front().host << " ";
          cab[idx[0]].pop_front();
        }
        std::cout << std::endl;
      } else {
        break;
      }
    }
    // 3-2. only have 2 nodes to pick 2 indexes, we need to
    // get the last index randomly
    if (cab[idx[0]].size() == 2) {
      // 3-2-1. get the 1st & 2nd indexes first
      for (int times = 0; times < 2; times++) {
        // Do not pop here, need to use the element later
        // std::cout << cab[idx[0]].front().host << " ";
        // cab[idx[0]].pop_front();
        std::cout << cab[idx[0]][times].host << " ";
      }
      // 3-2-2. get the 3rd index
      int next_cab_idx = -1;
      int next_node_idx = -1;
      // we have at least 2 cabinets, so we can get the
      // last index from the different cabinet
      if (cabinets.size() > 1) {
        do {
          std::srand(std::time(0));
          next_cab_idx = std::rand() % cabinets.size();
        } while (next_cab_idx == idx[0]);
        std::srand(std::time(0));
        next_node_idx = std::rand() % cabinets[next_cab_idx].size();
        std::cout << cabinets[next_cab_idx][next_node_idx].host << std::endl;
      } else {
        // we only have 1 cabinet, so we have to get the last
        // index from the same cabinet, but we try to get the
        // last index from the different hosts
        next_cab_idx = idx[0];
        int retry_times = 0;
        while (retry_times < 5) {
          std::srand(std::time(0));
          next_node_idx = std::rand() % cabinets[next_cab_idx].size();
          if (cabinets[next_cab_idx][next_node_idx].id !=
              cab[idx[0]][0].id && cabinets[next_cab_idx][next_node_idx].id !=
              cab[idx[0]][1].id) {
            break;
          }
          retry_times++;
        }
        std::cout << cabinets[next_cab_idx][next_node_idx].host << std::endl;
      }

    } else if (cab[idx[0]].size() == 1) {
    // 3-3. only have 1 node to pick 1 index, we need to
    // get the 2nd & 3rd indexes randomly
      int next_cab_idx = idx[0];
      int next_node_idx = -1;
      int idx_1_host_id = cab[idx[0]].front().id;
      int idx_2_host_id = -1;
      std::cout << cab[idx[0]].front().host << " ";
      cab[idx[0]].pop_front();
      // 3-3-1. if we have 3 cabinets, we can pick the
      // 2nd & 3rd from two different cabinets
      if (cabinets.size() >= 3) {
        // get the 2nd index
        do {
          std::srand(std::time(0));
          next_cab_idx = std::rand() % cabinets.size();
        } while (next_cab_idx == idx[0]);
        int the_2nd_cab_idx = next_cab_idx;
        std::srand(std::time(0));
        next_node_idx = std::rand() % cabinets[next_cab_idx].size();
        std::cout << cabinets[next_cab_idx][next_node_idx].host << std::endl;

        // get the 3rd index
        do {
          std::srand(std::time(0));
          next_cab_idx = std::rand() % cabinets.size();
        } while (next_cab_idx == idx[0] || next_cab_idx == the_2nd_cab_idx);
        std::srand(std::time(0));
        next_node_idx = std::rand() % cabinets[next_cab_idx].size();
        std::cout << cabinets[next_cab_idx][next_node_idx].host << std::endl;
      } else if (cabinets.size() == 2) {
        // 3-3-2. get the 2nd index from the other cabinet
        next_cab_idx = idx[0] == 0 ? 1 : 0;
        std::srand(std::time(0));
        next_node_idx = std::rand() % cabinets[next_cab_idx].size();
        std::cout << cabinets[next_cab_idx][next_node_idx].host << std::endl;

        // get the 3rd index from the same cabinet with 1st index,
        // try to pick the 3rd from the different host
        int retry_times = 0;
        while (retry_times < 5) {
          std::srand(std::time(0));
          next_node_idx = std::rand() % cabinets[idx[0]].size();
          if (cabinets[next_cab_idx][next_node_idx].id != idx_1_host_id) {
            break;
          }
          retry_times++;
        }
        std::cout << cabinets[next_cab_idx][next_node_idx].host << " ";
      } else {
        // 3-3-3. get the 2nd & 3rd indexes from the same cabinet
        // try to pick from different host

        // get the 2nd
        int retry_times = 0;
        while (retry_times < 5) {
          std::srand(std::time(0));
          next_node_idx = std::rand() % cabinets[next_cab_idx].size();
          if (cabinets[next_cab_idx][next_node_idx].id != idx_1_host_id) {
            idx_2_host_id = cabinets[next_cab_idx][next_node_idx].id;
            break;
          }
          retry_times++;
        }
        std::cout << cabinets[next_cab_idx][next_node_idx].host << " ";

        // get the 3rd
        retry_times = 0;
        while (retry_times < 5) {
          std::srand(std::time(0));
          next_node_idx = std::rand() % cabinets[next_cab_idx].size();
          if (cabinets[next_cab_idx][next_node_idx].id != idx_1_host_id &&
              cabinets[next_cab_idx][next_node_idx].id != idx_2_host_id) {
            break;
          }
          retry_times++;
        }
        std::cout << cabinets[next_cab_idx][next_node_idx].host << " ";
      }
    }
  }
}

int main() {
  if (!Load()) {
    return -1;
  }
  Distribution();
  return 0;
}
