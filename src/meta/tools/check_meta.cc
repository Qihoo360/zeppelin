#include <iostream>
#include <assert.h>
#include <string.h>
#include <leveldb/db.h>
#include <pthread.h>
#include <unistd.h>
#include <google/protobuf/text_format.h>

#include "zp_meta.pb.h"

leveldb::DB* db;

int main(int argc, char* argv[]){

  if (argc != 2) {
    std::cout << "Usage: ./check_meta path_to_leveldb_db" << std::endl;
    return -1;
  }
  leveldb::Options options;
  options.create_if_missing = true;
  leveldb::Status status = leveldb::DB::Open(options,argv[1], &db);
  assert(status.ok());

  ZPMeta::TableName table_name;
  ZPMeta::TableName new_table_name;
  ZPMeta::Nodes nodes;
  ZPMeta::Table table_info;

  std::string text_format;

  leveldb::Iterator* it = db->NewIterator(leveldb::ReadOptions());
  int table_num = 0;
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    if (it->key().ToString() == "##tables") {
      if(!table_name.ParseFromString(it->value().ToString())) {
        std::cout << "Parse TableList Error" << std::endl;
        return -1;
      }
      google::protobuf::TextFormat::PrintToString(table_name, &text_format);
      std::cout << "----------------------------------------------" << std::endl;
      std::cout << "Print TableList ====> " << std::endl << std::endl;
      std::cout << text_format;

    } else if (it->key().ToString() == "##version") {
      std::cout << "----------------------------------------------" << std::endl;
      std::cout << "Print Version ====> " << std::endl << std::endl;
      std::cout << atoi(it->value().data()) << std::endl;
    } else if (it->key().ToString() == "##nodes") {
      if(!nodes.ParseFromString(it->value().ToString())) {
        std::cout << "Parse Nodes Error" << std::endl;
        return -1;
      }
      google::protobuf::TextFormat::PrintToString(nodes, &text_format);
      std::cout << "----------------------------------------------" << std::endl;
      std::cout << "Print Nodes ====> " << std::endl << std::endl;
      std::cout << text_format;
    } else {
      if(!table_info.ParseFromString(it->value().ToString())) {
        std::cout << "Parse TableInfo Error, key: " << it->key().ToString() << std::endl;
        return -1;
      }
      new_table_name.add_name(table_info.name());
      table_num++;
      //       google::protobuf::TextFormat::PrintToString(table_info, &text_format);
      //       std::cout << "----------------------------------------------" << std::endl;
      //       std::cout << "Print TableInfo ====> " << std::endl << std::endl;
      //       std::cout << text_format;
    }
  }

  if (table_num != table_name.name_size()) {
    std::cout << "----------------------------------------------" << std::endl;
    std::cout << "Got Error, TableNum in floyd: " << table_num;
    std::cout << " TableList size: " << table_name.name_size();
    std::cout << " NewTableList size: " << new_table_name.name_size() << std::endl;
    std::cout << " Try to repair TableList..." << std::endl;

    std::string value;
    if (!new_table_name.SerializeToString(&value)) {
      std::cout << "Serialization new_table_name failed, value: " <<  value << std::endl;
      return -1;
    }
    status = db->Put(leveldb::WriteOptions(), "##tables", value);
    if (!status.ok()) {
      std::cout << "Update NewTableList Error: " << status.ToString() << std::endl;
      return -1;
    }
    std::cout << " Try to repair Done..." << std::endl;
  } else {
    std::cout << "----------------------------------------------" << std::endl;
    std::cout << "Check Successfully" << std::endl;
  }
  assert(it->status().ok());  // Check for any errors found during the scan
  delete it;

  delete db;
  return 0;
}
