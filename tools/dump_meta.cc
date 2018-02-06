#include <iostream>
#include <assert.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>
#include <google/protobuf/text_format.h>

#include "src/meta/zp_meta.pb.h"

#include "rocksdb/db.h"
#include "rocksdb/options.h"

rocksdb::DB* db;

int main(int argc, char* argv[]){

  if (argc != 2 && argc != 3) {
    std::cout << "Usage:\n"
        << "    ./dump_meta path_to_RocksDB        --- do not print detail\n"
        << "    ./dump_meta path_to_RocksDB detail --- print detail table_info\n";
    return -1;
  }

  bool detail = false;
  if (argc == 3 && strcmp(argv[2], "detail") == 0) {
    detail = true;
  }
  // Create DB
  rocksdb::Options options;
  options.create_if_missing = true;
  rocksdb::Status status = rocksdb::DB::Open(options, argv[1], &db);
  if (!status.ok()) {
    std::cout << "Open db failed! path: " << argv[1]
      << ", " << status.ToString() << std::endl;
    return -1;
  }

  ZPMeta::TableName new_table_name;

  std::string text_format, value;

  // Print version
  status = db->Get(rocksdb::ReadOptions(), "##version", &value);
  if (!status.ok()) {
    std::cout << "Get Version failed: " << status.ToString() << std::endl;
    return -1;
  }
  std::cout << "----------------------------------------------" << std::endl;
  std::cout << "Print Version ====> " << std::endl << std::endl;
  std::cout << std::stoi(value) << std::endl;
  
  // Print anchor
  status = db->Get(rocksdb::ReadOptions(), "##anchor", &value);
  if (!status.ok()) {
    std::cout << "Get Anchor failed: " << status.ToString() << std::endl;
    return -1;
  }
  std::cout << "----------------------------------------------" << std::endl;
  std::cout << "Print Anchor ====> " << std::endl << std::endl;
  std::cout << value << std::endl;

  // Print nodes
  ZPMeta::Nodes nodes;
  status = db->Get(rocksdb::ReadOptions(), "##nodes", &value);
  if (!status.ok()) {
    std::cout << "Get Nodes failed: " << status.ToString() << std::endl;
    return -1;
  }
  if(!nodes.ParseFromString(value)) {
    std::cout << "Parse Nodes Error" << std::endl;
    return -1;
  }
  google::protobuf::TextFormat::PrintToString(nodes, &text_format);
  std::cout << "----------------------------------------------" << std::endl;
  std::cout << "Print Nodes ====> " << std::endl << std::endl;
  std::cout << text_format;

  // Print table list
  ZPMeta::TableName table_name;
  status = db->Get(rocksdb::ReadOptions(), "##tables", &value);
  if (!status.ok()) {
    std::cout << "Get Table list failed: " << status.ToString() << std::endl;
    return -1;
  }
  if(!table_name.ParseFromString(value)) {
    std::cout << "Parse TableList Error" << std::endl;
    return -1;
  }
  google::protobuf::TextFormat::PrintToString(table_name, &text_format);
  std::cout << "----------------------------------------------" << std::endl;
  std::cout << "Print TableList ====> " << std::endl << std::endl;
  std::cout << text_format;

  // Print table list
  ZPMeta::Table table_info;
  for (const auto t : table_name.name()) {
    status = db->Get(rocksdb::ReadOptions(), t, &value);
    if (!status.ok()) {
      std::cout << "Get TableInfo failed: " << status.ToString()
        << ", table: " << t << std::endl;
      return -1;
    }
    if(!table_info.ParseFromString(value)) {
      std::cout << "Parse TableInfo Error, table: " << t << std::endl;
      return -1;
    }
    if (detail) {
      google::protobuf::TextFormat::PrintToString(table_info, &text_format);
      std::cout << "----------------------------------------------" << std::endl;
      std::cout << "Print TableInfo ====> " << std::endl << std::endl;
      std::cout << text_format;
    }
  }

  // Print migrate
  ZPMeta::MigrateHead migrate_head;
  status = db->Get(rocksdb::ReadOptions(), "##migrate", &value);
  if (!status.ok()) {
    std::cout << "Get Migrate head failed: " << status.ToString() << std::endl;
    return -1;
  }
  if(!migrate_head.ParseFromString(value)) {
    std::cout << "Parse Migrate head Error" << std::endl;
    return -1;
  }
  google::protobuf::TextFormat::PrintToString(migrate_head, &text_format);
  std::cout << "----------------------------------------------" << std::endl;
  std::cout << "Print MigrateHead ====> " << std::endl << std::endl;
  std::cout << text_format << std::endl;

  // Print migrate diffs
  ZPMeta::RelationCmdUnit diff;
  for (const auto d : migrate_head.diff_name()) {
    status = db->Get(rocksdb::ReadOptions(), d, &value);
    if (!status.ok()) {
      std::cout << "Get Migrate diff failed: " << status.ToString()
        << ", diff key: " << d << std::endl;
      return -1;
    }
    if(!diff.ParseFromString(value)) {
      std::cout << "Parse Migrate diff Error, table: " << d << std::endl;
      return -1;
    }
    if (detail) {
      google::protobuf::TextFormat::PrintToString(diff, &text_format);
      std::cout << "----------------------------------------------" << std::endl;
      std::cout << "Print Migrate diff ====> " << std::endl << std::endl;
      std::cout << text_format << std::endl;
    }
  }

  delete db;
  return 0;
}
