/*
 * "Copyright [2016] qihoo"
 * "Author <hrxwwd@163.com>"
 */

#include "include/zp_types.h"
#include "sys/time.h"


namespace libzp {

const int kDataConnTimeout =  20000000;
IpPort::IpPort() {
}

IpPort::IpPort(const std::string& other_ip, int other_port) :
  ip(other_ip),
  port(other_port) {
}

IpPort::IpPort(const IpPort& other) :
  ip(other.ip),
  port(other.port) {
}

IpPort::~IpPort() {
}

IpPort& IpPort::operator = (const IpPort& other) {
  ip = other.ip;
  port = other.port;
  return *this;
}

bool IpPort::operator < (const IpPort& other) const {
  if (port < other.port) {
    return true;
  }
  return false;
}

bool IpPort::operator == (const IpPort& other) const {
  if (ip == other.ip && port == other.port) {
    return true;
  }
  return false;
}

Table::Table(const ZPMeta::Table& table_info) {
  table_name_ = table_info.name();
  partition_num_ = table_info.partitions_size();
  ZPMeta::Partitions partition_info;
  for (int i = 0; i < table_info.partitions_size(); i++) {
    partition_info = table_info.partitions(i);
    Partition* par = new Partition(partition_info);
    partitions_.insert(std::make_pair(partition_info.id(), par));
  }
}

Table::~Table() {
  std::map<int, Partition*>::iterator iter = partitions_.begin();
  while (iter != partitions_.end()) {
    delete iter->second;
    iter++;
  }
}

IpPort Table::GetKeyMaster(const std::string& key) {
  int par_num = std::hash<std::string>()(key) % partitions_.size();
  std::map<int, Partition*>::iterator iter = partitions_.find(par_num);
  if (iter != partitions_.end()) {
    return iter->second->master;
  } else {
    return IpPort();
  }
}

Table::Partition* Table::GetPartition(const std::string& key) {
  int par_num = std::hash<std::string>()(key) % partitions_.size();
  std::map<int, Partition*>::iterator iter = partitions_.find(par_num);
  if (iter != partitions_.end()) {
    return iter->second;
  } else {
    return NULL;
  }
}

void Table::DebugDump() {
  std::cout << "  name: "<< table_name_ <<std::endl;
  std::cout << "  partition: "<< partition_num_ <<std::endl;
  auto par = partitions_.begin();
  while (par != partitions_.end()) {
    std::cout << "    partition: "<< par->second->id;
    std::cout << "    master: " << par->second->master.ip
      << " : " << par->second->master.port << std::endl;
    par++;
  }
}

uint64_t ZpCli::NowMicros() {
  struct timeval tv;
  gettimeofday(&tv, NULL);
  return static_cast<uint64_t>(tv.tv_sec)*1000000 +
    static_cast<uint64_t>(tv.tv_usec);
}

ZpCli::ZpCli(const IpPort& ip_port)
 : cli_(new pink::PbCli()),
   ip_(ip_port.ip),
   port_(ip_port.port),
   lastchecktime_(NowMicros()) {
}

ZpCli::~ZpCli() {
  delete cli_;
}

Status ZpCli::Connect() {
  return cli_->Connect(ip_, port_);
}

Status ZpCli::ReConnect() {
  return cli_->Connect(ip_, port_);
}

Status ZpCli::Send(void *msg) {
  return cli_->Send(msg);
}

Status ZpCli::Recv(void *msg_res) {
  return cli_->Recv(msg_res);
}

Status ZpCli::CheckTimeout() {
  uint64_t now = NowMicros();
  if ((now - lastchecktime_) > kDataConnTimeout) {
    Status s = ReConnect();
    if (s.ok()) {
      lastchecktime_ = now;
      return Status::OK();
    }
    return s;
  }
  lastchecktime_ = now;
  return Status::OK();
}

ConnectionPool::ConnectionPool() {
}

ConnectionPool::~ConnectionPool() {
  std::map<IpPort, ZpCli*>::iterator iter = conn_pool_.begin();
  while (iter != conn_pool_.end()) {
    delete iter->second;
    iter++;
  }
  conn_pool_.clear();
}

ZpCli* ConnectionPool::GetConnection(const IpPort ip_port) {
  std::map<IpPort, ZpCli*>::iterator it;
  it = conn_pool_.find(ip_port);
  if (it != conn_pool_.end()) {
    Status s = it->second->CheckTimeout();
    if (s.ok()) {
      return it->second;
    } else {
      delete it->second;
      conn_pool_.erase(it);
      return NULL;
    }
  } else {
    ZpCli* cli = new ZpCli(ip_port);
    Status s = cli->Connect();
    if (s.ok()) {
      conn_pool_.insert(std::make_pair(ip_port, cli));
      return cli;
    } else {
      delete cli;
      return NULL;
    }
  }
  return NULL;
}

void ConnectionPool::RemoveConnection(const IpPort ip_port) {
  std::map<IpPort, ZpCli*>::iterator it;
  it = conn_pool_.find(ip_port);
  if (it != conn_pool_.end()) {
    delete(it->second);
    conn_pool_.erase(it);
  }
}

ZpCli* ConnectionPool::GetExistConnection(IpPort* ip_port) {
  Status s;
  while (conn_pool_.size() != 0) {
    s = conn_pool_.begin()->second->CheckTimeout();
    if (s.ok()) {
      *ip_port = conn_pool_.begin()->first;
      return conn_pool_.begin()->second;
    }
    delete conn_pool_.begin()->second;
    conn_pool_.erase(conn_pool_.begin());
  }
  return NULL;
}

}  // namespace libzp
