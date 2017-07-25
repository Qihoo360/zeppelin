#include <string>
#include <iostream>
#include "slash/include/env.h"

void usage() {
  std::cout << "usage:\n"
            << "      empty_trash trash_path table\n";
}

void error(const std::string& msg) {
  std::cout << msg << std::endl;
  usage();
  exit(-1);
}

int main(int argc, char* argv[]) {
  if (argc < 3) {
    error("Too few arguments");
  }

  std::string trash_path(argv[1]), table_name(argv[2]);
  if (slash::IsDir(trash_path) != 0) {
    error("Not a directory");
    return -1;
  }
  
  std::cout << "Are you sure you want to permanently erase the items"
    << " in the Zeppelin Trash: " << trash_path
    << ", table: " << table_name << std::endl;
  std::cout << "Confirm? (y or n) : ";
  char confirm;
  std::cin >> confirm;
  if (confirm != 'y') {
    return 0;
  }

  std::string table_path, partition_path, type_path;
  std::vector<std::string> tables, partitions, types;
  slash::GetChildren(trash_path, tables);
  for (auto& table : tables) {
    if (table_name != table
        && table_name != "all") {
      continue;
    }
    table_path = trash_path + '/' + table;
    if (slash::IsDir(table_path) != 0) {
      error("Not a Table dir:" + table_path);
      return -1;
    }
    partitions.clear();
    slash::GetChildren(table_path, partitions);
    for (auto& partition : partitions) {
      partition_path = table_path + '/' + partition;
      char* endptr = NULL;
      strtol(partition.c_str(), &endptr, 10);
      if (slash::IsDir(partition_path) != 0
          || endptr == partition.data()) {
        error("Not a Partition Dir:" + partition_path);
        return -1;
      }
      slash::DeleteDirIfExist(partition_path);
      slash::CreateDir(partition_path);
    }
  }

  std::cout << "Finish Empty Zeppelin Trash" << std::endl;
  return 0;
}
