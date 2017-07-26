#include <set>
#include <map>
#include <vector>
#include <string>
#include <iostream>
#include "slash/include/env.h"
#include "slash/include/slash_string.h"
#include "include/zp_const.h"

struct hole {
  uint32_t left;
  uint32_t right;
};

bool ExistBinlogHole(const std::string& log_path, struct hole* h) {
  // Got all files
  std::vector<std::string> children;
  int ret = slash::GetChildren(log_path, children);
  if (ret != 0) {
    std::cout << "ExistBinlogHole Get all files in log path failed!"
      << ", log_path: " << log_path
      << ", Error: " << ret << std::endl;
    exit(-1);
  }

  // Got parititon id
  int64_t index = 0;
  std::string sindex;
  std::set<uint32_t> binlog_nums;
  std::vector<std::string>::iterator it;
  for (it = children.begin(); it != children.end(); ++it) {
    if ((*it).compare(0, kBinlogPrefixLen, kBinlogPrefix) != 0) {
      continue;
    }
    sindex = (*it).substr(kBinlogPrefixLen);
    if (slash::string2l(sindex.c_str(), sindex.size(), &index) == 1) {
      binlog_nums.insert(index);
    }
  }


  std::set<uint32_t>::iterator num_it = binlog_nums.begin(),
    pre_num_it = binlog_nums.begin();
  for (++num_it; num_it != binlog_nums.end(); ++num_it, ++pre_num_it) {
    if (*num_it != *pre_num_it + 1) {
      h->left = *num_it;
      h->right = *pre_num_it;
      return true;
    }
  }
  return false;
}

void print_usage_exit() {
  std::cout << "Usage:" << std::endl;
  std::cout << "    ./check_lost log_path" << std::endl; 
  exit(-1);
}


int main(int argc, char* argv[]) {
  if (argc != 2) {
    print_usage_exit();
  }

  std::string log_path(argv[1]);

  // Get all table dir
  std::vector<std::string> tables;
  int ret = slash::GetChildren(log_path, tables);
  if (ret != 0) {
    std::cout << "Get all table dir in log path failed! ret: "
      << ret << std::endl;
    return -1;
  }

  std::string table_path, partition_path;
  std::vector<std::string> partitions;
  std::map<std::string, struct hole> failed_db;
  for (auto& table : tables) {
    table_path = log_path + "/" + table;
    if (!slash::IsDir(table_path)) {
      partitions.clear();
      ret = slash::GetChildren(table_path, partitions);
      if (ret != 0) {
        std::cout << "Get table partitions dir in table path failed! table_path: "
          << table_path << ", ret: " << ret << std::endl;
        return -1;
      }
      for (auto& p : partitions) {
        partition_path = table_path + "/" + p;
        char* endptr = NULL;
        strtol(p.c_str(), &endptr, 10);
        if (slash::IsDir(partition_path) != 0
            || endptr == p.data()) {
          std::cout << "Not a Partition Dir:" <<  partition_path << std::endl;
          return -1;
        }
        std::cout << "Check binlog files of: " << partition_path
          << std::endl;
        struct hole binlog_hole;
        if (ExistBinlogHole(partition_path, &binlog_hole)) {
          std::cout << "Failed!" << std::endl;
          failed_db.insert(std::pair<std::string, struct hole>(partition_path,
                binlog_hole));
        } else {
          std::cout << "Success!" << std::endl;
        }
        std::cout << std::endl;
      }
    }
  }
  
  std::cout << "--------------------------------------------------------" << std::endl;
  std::cout << "| Error path          | hole_left | hole_right |" << std::endl;
  for (auto& f : failed_db) {
    std::cout << "| " << f.first
      << "   |   " << f.second.left
      << "   |   " << f.second.right
      << " | " << std::endl;
  }

  return 1;
}
