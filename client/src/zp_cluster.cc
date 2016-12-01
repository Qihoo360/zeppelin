/*
 * "Copyright [2016] <hrxwwd@163.com>"
 */
#include "include/zp_cluster.h"
#include "include/zp_meta_cli.h"

#include<iostream>
#include<string>
#define ATTEMP_TIME 10
namespace libZp {


Status Cluster::Set(const std::string& table, const std::string& key, const std::string& value) {
  Status s;
  IpPort master;
  s = GetDataMaster(master, table, key);
  std::cout << "data ip:"<< master.ip << " port:" << master.port <<std::endl;
  // @TODO use created conn
  //std::shared_ptr<ZpDataCli> data_cli = GetDataCli(master);
  std::shared_ptr<ZpDataCli> data_cli = CreateDataCli(master);
  if (data_cli) {
    s = data_cli->Set(table, key, value);
    if (!s.ok()) {
      CreateDataCli(master);
    }
  } else {
    s = Status::IOError("no data cli got");
  }
  return s;
}

Status Cluster::Get(const std::string& table, const std::string& key, std::string& value) {
  Status s;
  IpPort master;
  s = GetDataMaster(master, table, key);
  std::cout << "data ip:"<< master.ip << " port:" << master.port <<std::endl;
  //@TODO use created conn
  //std::shared_ptr<ZpDataCli> data_cli = GetDataCli(master);
  std::shared_ptr<ZpDataCli> data_cli = GetDataCli(master);
  if (data_cli) {
    s = data_cli->Get(table, key, value);
    if (!s.ok() && !s.IsNotFound()) {
      CreateDataCli(master);
    }
  } else {
    s = Status::IOError("no data cli got");
  }
  return s;
}

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
}

Cluster::~Cluster() {
}



Status Cluster::Connect() {
  Status s;
  int attemp_count = 0;
  while (attemp_count++ < ATTEMP_TIME) {
    IpPort meta = GetRandomMetaAddr();
    std::shared_ptr<ZpMetaCli> meta_cli = CreateMetaCli(meta);
    if (meta_cli) {
      return Status::OK();
    }
  }
  return Status::IOError("can't connect meta after attempts");
}

Status Cluster::Pull(const std::string& table) {
  Status s;
  std::shared_ptr<ZpMetaCli> meta_cli = GetMetaCli();
  if (meta_cli) {
    s = meta_cli->Pull(cluster_map_, table);
  } else {
    s = Status::IOError(" meta cli lose connect");
  }
  return s;
}

Status Cluster::DumpTable(const std::string& table) {
  std::cout << "epoch:" << cluster_map_.epoch << std::endl;
  std::cout << "table_num:" << cluster_map_.table_num << std::endl;
  auto it = cluster_map_.table_maps.begin();
  int table_num = 1;
  while (it != cluster_map_.table_maps.end()) {
    std::cout << "  name: "<< it->first <<std::endl;
    std::cout << "  partition: "<< it->second.partition_num <<std::endl;
    auto par = it->second.partitions.begin();
    while (par != it->second.partitions.end()) {
      std::cout << "    partition: "<< par->first ;
      std::cout << "    master: " << par->second.master.ip 
                << " : " << par->second.master.port << std::endl;
      par++;
    }
    it++;
  }
  return Status::OK();
}

Status Cluster::CreateTable(const std::string& table_name,
    const int partition_num) {
  Status s;
  std::shared_ptr<ZpMetaCli> meta_cli = GetMetaCli();
  if (meta_cli) {
    s = meta_cli->CreateTable(table_name, partition_num);
  } else {
    s = Status::IOError(" meta cli lose connect");
  }
  return s;
}

std::shared_ptr<ZpMetaCli> Cluster::CreateMetaCli(const IpPort& ipPort) {
  auto it = meta_cli_.find(ipPort);
  if (it != meta_cli_.end()) {
    meta_cli_.erase(it);
  }
  std::shared_ptr<ZpMetaCli> cli =
    std::make_shared<ZpMetaCli>(ipPort.ip, ipPort.port);
  Status s = cli->Connect(ipPort.ip, ipPort.port);
  if (s.ok()) {
    meta_cli_.emplace(ipPort, cli);
  } else {
    cli.reset();
  }
  return cli;
}

std::shared_ptr<ZpDataCli> Cluster::CreateDataCli(const IpPort& ipPort) {
  auto it = data_cli_.find(ipPort);
  if (it != data_cli_.end()) {
    data_cli_.erase(it);
  }
  std::shared_ptr<ZpDataCli> cli =
    std::make_shared<ZpDataCli>(ipPort.ip, ipPort.port);
  Status s = cli->Connect(ipPort.ip, ipPort.port);
  if (s.ok()) {
    data_cli_.emplace(ipPort, cli);
  } else {
    cli.reset();
  }
  return cli;
}

std::shared_ptr<ZpDataCli> Cluster::GetDataCli(const IpPort& ip_port) {
  std::map<IpPort, std::shared_ptr<ZpDataCli>>::iterator it =
    data_cli_.find(ip_port);
  if (it != data_cli_.end()) {
    return it->second;
  } else {
    return CreateDataCli(ip_port);
  }
}



IpPort Cluster::GetRandomMetaAddr() {
  std::random_device rd;
  std::mt19937 mt(rd());
  std::uniform_int_distribution<int> di(0, meta_addr_.size()-1);
  int index = di(mt);
  return meta_addr_[index];
}

Status Cluster::GetDataMaster(IpPort& master, const std::string& table,
    const std::string& key) {
  std::map<std::string, TableMap>::iterator it =
    cluster_map_.table_maps.find(table);
  if (it == cluster_map_.table_maps.end()) {
    return Status::NotFound("table does not exist");
  }
  int partition_num = it->second.partition_num;
  int number = std::hash<std::string>()(key) % partition_num;
  std::map<int, ReplicaSet>::iterator replica =
    it->second.partitions.find(number);
  master = replica->second.master;

  return Status::OK();
}


std::shared_ptr<ZpMetaCli> Cluster::GetMetaCli() {
  if (meta_cli_.size() != 0) {
    return meta_cli_.begin()->second;
  } else {
    IpPort meta = GetRandomMetaAddr();
    return CreateMetaCli(meta);
  }
}

}  // namespace libZp
