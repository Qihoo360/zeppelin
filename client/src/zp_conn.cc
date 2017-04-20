/*
 * "Copyright [2016] qihoo"
 * "Author <hrxwwd@163.com>"
 */

#include "sys/time.h"
#include "include/zp_conn.h"
#include "include/zp_table.h"


namespace libzp {

const int kDataConnTimeout =  20000000;

static uint64_t NowMicros() {
  struct timeval tv;
  gettimeofday(&tv, NULL);
  return static_cast<uint64_t>(tv.tv_sec)*1000000 +
    static_cast<uint64_t>(tv.tv_usec);
}

ZpCli::ZpCli(const Node& node)
: node(node),
  cli(new pink::PbCli()),
  lastchecktime(NowMicros()) {
}

ZpCli::~ZpCli() {
  delete cli;
}


Status ZpCli::CheckTimeout() {
  uint64_t now = NowMicros();
  if ((now - lastchecktime) > kDataConnTimeout) {
    pink::Status s = cli->Connect(node.ip, node.port);
    if (s.ok()) {
      lastchecktime = now;
      return Status::OK();
    }
    return Status::Corruption(s.ToString());
  }
  lastchecktime = now;
  return Status::OK();
}

ConnectionPool::ConnectionPool() {
}

ConnectionPool::~ConnectionPool() {
  std::map<Node, ZpCli*>::iterator iter = conn_pool_.begin();
  while (iter != conn_pool_.end()) {
    delete iter->second;
    iter++;
  }
  conn_pool_.clear();
}

ZpCli* ConnectionPool::GetConnection(const Node& node) {
  std::map<Node, ZpCli*>::iterator it;
  it = conn_pool_.find(node);
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
    ZpCli* cli = new ZpCli(node);
    pink::Status s = cli->cli->Connect(node.ip, node.port);
    if (s.ok()) {
      conn_pool_.insert(std::make_pair(node, cli));
      return cli;
    } else {
      delete cli;
      return NULL;
    }
  }
  return NULL;
}

void ConnectionPool::RemoveConnection(ZpCli* conn) {
  Node node = conn->node;
  std::map<Node, ZpCli*>::iterator it;
  it = conn_pool_.find(node);
  if (it != conn_pool_.end()) {
    delete(it->second);
    conn_pool_.erase(it);
  }
}

ZpCli* ConnectionPool::GetExistConnection() {
  Status s;
  while (conn_pool_.size() != 0) {
    s = conn_pool_.begin()->second->CheckTimeout();
    if (s.ok()) {
      return conn_pool_.begin()->second;
    }
    delete conn_pool_.begin()->second;
    conn_pool_.erase(conn_pool_.begin());
  }
  return NULL;
}

}  // namespace libzp
