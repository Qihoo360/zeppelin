#include "zp_cluster.h"
#include "zp_meta_cli.h"
namespace libZp {

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
  #define ATTEMP 10
    while (attemp_count ++ < ATTEMP) {
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
Status Cluster::CreateTable(std::string& table_name, int partition_num) {
  Status s;
  std::shared_ptr<ZpMetaCli> meta_cli = GetMetaCli();
  if (meta_cli) {
    s = meta_cli->CreateTable(table_name, partition_num);
  } else {
    s = Status::IOError(" meta cli lose connect");
  }
  return s;
}
std::shared_ptr<ZpMetaCli> Cluster::CreateMetaCli(IpPort& ipPort) {
  std::shared_ptr<ZpMetaCli> cli = std::make_shared<ZpMetaCli>(ipPort.ip, ipPort.port);
  Status s = cli->Connect(ipPort.ip, ipPort.port);
  if (s.ok()) {
    meta_cli_.emplace(ipPort, cli);
  }
  return cli;
}

IpPort Cluster::GetRandomMetaAddr() {
  std::random_device rd;
  std::mt19937 mt(rd());
  std::uniform_int_distribution<int> di(0, meta_addr_.size()-1);
  int index = di(mt);
  return meta_addr_[index];
}

std::shared_ptr<ZpMetaCli> Cluster::GetMetaCli() {
  if (meta_cli_.size() != 0) {
    return meta_cli_.begin()->second;
  } else {
    IpPort meta = GetRandomMetaAddr();
    return CreateMetaCli(meta);
  }
}

  /*
     std::shared_ptr<MetaCli> Cluster::GetDataCli(IpPort& meta) {
    map<IpPort, std::shared_ptr<ZpDataCli>>::iterator cli = data_cli_.find(meta);
    if (cli != data_cli_.end()) {
      return cli.second;
    } else {
      return CreateMetaCli(meta);
    }
  }
  */

}
