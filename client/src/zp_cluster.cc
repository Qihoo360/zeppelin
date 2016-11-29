/*
 * "Copyright [2016] <hrxwwd@163.com>"
 */
#include "include/zp_cluster.h"
#include "include/zp_meta_cli.h"
namespace libZp {

IoCtx::IoCtx(Cluster* cluster, const std::string& table) :
  cluster_(cluster),
  table_(table) {
}

IoCtx::~IoCtx() {
}

Status IoCtx::Set(const std::string& key, const std::string& value) {
  Status s;
  IpPort master;
  s = cluster_->GetDataMaster(master, table_, key); 
  std::shared_ptr<ZpDataCli> data_cli = cluster_->GetDataCli(master);
  if (data_cli) {
    s = data_cli->Set(table_, key, value);
  } else {
    s = Status::IOError("no data cli got");
  }
  return s;
}

Status IoCtx::Get(const std::string& key, std::string& value) {
  Status s;
  IpPort master;
  s = cluster_->GetDataMaster(master, table_, key); 
  std::shared_ptr<ZpDataCli> data_cli = cluster_->GetDataCli(master);
  if (data_cli) {
    s = data_cli->Get(table_, key, value);
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


IoCtx Cluster::CreateIoCtx(const std::string &table) {
  return IoCtx(this, table);
}

Status Cluster::Connect() {
  Status s;
  int attemp_count = 0;
  #define ATTEMP 10
    while (attemp_count++ < ATTEMP) {
      IpPort meta = GetRandomMetaAddr();
      std::shared_ptr<ZpMetaCli> meta_cli = CreateMetaCli(meta);
      if (meta_cli) {
        s = meta_cli->Pull(cluster_map_);
        return s;
      }
    }
    return Status::IOError("can't connect meta after attempts");
}

Status Cluster::Pull() {
  Status s;
  std::shared_ptr<ZpMetaCli> meta_cli = GetMetaCli();
  if (meta_cli) {
    s = meta_cli->Pull(cluster_map_);
  } else {
    s = Status::IOError(" meta cli lose connect");
  }
  return s;
}

Status Cluster::CreateTable(const std::string& table_name, const int partition_num) {
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
  std::map<IpPort, std::shared_ptr<ZpDataCli>>::iterator it = data_cli_.find(ip_port);
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
  std::map<int, ReplicaSet>::iterator replica = it->second.partitions.find(number);
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
