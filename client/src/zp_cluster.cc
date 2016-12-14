/*
 * "Copyright [2016] <hrxwwd@163.com>"
 */
#include "include/zp_cluster.h"
#include "include/zp_meta_cli.h"

#include<iostream>
#include<string>


namespace libzp {

Cluster::Cluster(const Options& options) {
  auto i = options.meta_addr.begin();
  while (i != options.meta_addr.end()) {
    meta_addr_.emplace_back(*i);
    i++;
  }
  if (meta_addr_.size() == 0) {
    fprintf(stderr, "you need input at least 1 meta node address:\n");
    exit(-1);
  }
  epoch = 0;
}

Cluster::~Cluster() {
  std::unordered_map<std::string, Table*>::iterator iter = tables_.begin();
  while (iter != tables_.end()) {
    delete iter->second;
    iter++;
  }
}


Status Cluster::Set(const std::string& table, const std::string& key,
    const std::string& value) {
  Status s;
  IpPort master;
  s = GetDataMaster(table, key, &master);
  if (!s.ok()) {
    return s;
  }
  std::cout << "data ip:" << master.ip << " port:" << master.port << std::endl;
  ZpDataCli* data_cli = data_pool_.GetConnection(master);
  if (data_cli) {
    s = data_cli->Set(table, key, value);
    if (s.IsIOError()) {
      data_pool_.RemoveConnection(master);
      data_cli = data_pool_.GetConnection(master);
      std::cout << "recreate connection" << std::endl;
      if (data_cli) {
        s = data_cli->Set(table, key, value);
      } else {
        Pull(table);
        return Status::IOError("can't connect to data,repull");
      }
    }
  } else {
    Pull(table);
    s = Status::IOError("can't connect to data,repull");
  }
  return s;
}

Status Cluster::Get(const std::string& table, const std::string& key,
    std::string* value) {
  Status s;
  IpPort master;
  s = GetDataMaster(table, key, &master);
  if (!s.ok()) {
    return s;
  }
  std::cout << "data ip:" << master.ip << " port:" << master.port << std::endl;
  ZpDataCli* data_cli = data_pool_.GetConnection(master);
  if (data_cli) {
    s = data_cli->Get(table, key, value);
    if (s.IsIOError()) {
      data_pool_.RemoveConnection(master);
      data_cli = data_pool_.GetConnection(master);
      if (data_cli) {
        s = data_cli->Get(table, key, value);
      } else {
        Pull(table);
        return Status::IOError("can't connect to data,repull");
      }
    }
  } else {
    Pull(table);
    s = Status::IOError("can't connect to data,repull");
  }
  return s;
}



// TODO(all) pull meta_info from data
Status Cluster::Connect() {
  Status s;
  int attemp_count = 0;
  IpPort meta;
  ZpMetaCli* meta_cli = meta_pool_.GetExistConnection(&meta);
  if (meta_cli != NULL) {
    return Status::OK();
  }
  while (attemp_count++ < kMetaAttempt) {
    meta = GetRandomMetaAddr();
    meta_cli = meta_pool_.GetConnection(meta);
    if (meta_cli) {
      return Status::OK();
    }
  }
  return Status::IOError("can't connect meta after attempts");
}

Status Cluster::Pull(const std::string& table) {
  Status s = Status::IOError("got no meta cli");
  IpPort meta;
  ZpMetaCli* meta_cli = meta_pool_.GetExistConnection(&meta);
  if (meta_cli) {
    std::cout << "pulling" << std::endl;
    s = meta_cli->Pull(table, &tables_, &epoch);
    if (!s.ok()) {
      std::cout << s.ToString() << std::endl;
      meta_pool_.RemoveConnection(meta);
    }
  }

  if (!s.ok()) {
    meta = GetRandomMetaAddr();
    meta_cli = meta_pool_.GetConnection(meta);
    if (meta_cli) {
      s = meta_cli->Pull(table, &tables_, &epoch);
    }
  }
  return s;
}


void Cluster::DebugDumpTable(const std::string& table) {
  std::cout << "epoch:" << epoch << std::endl;
  auto it = tables_.begin();
  while (it != tables_.end()) {
    if (it->first == table) {
      it->second->DebugDump();
    }
    it++;
  }
}

// TODO(wenduo): 这里接口定义的不好, GetPartition 返回一个空
Table::Partition Cluster::GetPartition(const std::string& table,
    const std::string& key) {
  std::cout << "epoch:" << epoch << std::endl;
  auto it = tables_.begin();
  while (it != tables_.end()) {
    if (it->first == table) {
      return it->second->GetPartition(key);
    }
    it++;
  }
  return Table::Partition();
}

Status Cluster::CreateTable(const std::string& table_name,
    const int partition_num) {
  Status s = Status::IOError("got no meta cli");
  IpPort meta;
  ZpMetaCli* meta_cli = meta_pool_.GetExistConnection(&meta);
  if (meta_cli) {
    s = meta_cli->CreateTable(table_name, partition_num);
    if (s.IsIOError()) {
      meta_pool_.RemoveConnection(meta);
      meta_cli = meta_pool_.GetConnection(meta);
      if (meta_cli) {
        s = meta_cli->CreateTable(table_name, partition_num);
      }
    }
  }
  if (s.IsIOError()) {
    meta = GetRandomMetaAddr();
    meta_cli = meta_pool_.GetConnection(meta);
    if (meta_cli) {
      s = meta_cli->CreateTable(table_name, partition_num);
    }
  }
  return s;
}

IpPort Cluster::GetRandomMetaAddr() {
  std::random_device rd;
  std::mt19937 mt(rd());
  std::uniform_int_distribution<int> di(0, meta_addr_.size()-1);
  int index = di(mt);
  return meta_addr_[index];
}

Status Cluster::GetDataMaster(const std::string& table,
    const std::string& key, IpPort* master) {
  std::unordered_map<std::string, Table*>::iterator it =
    tables_.find(table);
  if (it == tables_.end()) {
    std::cout << "know nothing about this table, repull" <<std::endl;
    Pull(table);
    it = tables_.find(table);
    if (it == tables_.end()) {
      return Status::NotFound("table does not exist");
    }
  }
  *master = it->second->GetKeyMaster(key);
  return Status::OK();
}

}  // namespace libzp
