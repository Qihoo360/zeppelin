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

ZpConn::ZpConn(const Node& node)
: node_(node),
  cli_(new pink::PbCli()),
  lastchecktime_(NowMicros()) {
}

ZpConn::~ZpConn() {
  delete cli_;
}

Status ZpConn::Connect() {
  return cli_->Connect(node_.ip, node_.port);
}

Status ZpConn::ReConnect() {
  return cli_->Connect(node_.ip, node_.port);
}

Status ZpConn::Send(void *msg) {
  return cli_->Send(msg);
}

Status ZpConn::Recv(void *msg_res) {
  return cli_->Recv(msg_res);
}

Status ZpConn::CheckTimeout() {
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
  std::map<Node, ZpConn*>::iterator iter = conn_pool_.begin();
  while (iter != conn_pool_.end()) {
    delete iter->second;
    iter++;
  }
  conn_pool_.clear();
}

ZpConn* ConnectionPool::GetConnection(const Node& node) {
  std::map<Node, ZpConn*>::iterator it;
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
    ZpConn* cli = new ZpConn(node);
    Status s = cli->Connect();
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

void ConnectionPool::RemoveConnection(ZpConn* conn) {
  Node node = conn->node_;
  std::map<Node, ZpConn*>::iterator it;
  it = conn_pool_.find(node);
  if (it != conn_pool_.end()) {
    delete(it->second);
    conn_pool_.erase(it);
  }
}

ZpConn* ConnectionPool::GetExistConnection() {
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
