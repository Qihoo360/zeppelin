#include <glog/logging.h>

#include "zp_command.h"
#include "zp_meta_node.h"
#include "zp_meta_worker_thread.h"

ZPMetaWorkerThread::ZPMetaWorkerThread(int cron_interval)
  : WorkerThread::WorkerThread(cron_interval),
    thread_querynum_(0),
    last_thread_querynum_(0),
    last_time_us_(slash::NowMicros()),
    last_sec_thread_querynum_(0) {
  cmds_.reserve(300);
  InitClientCmdTable();  
}

ZPMetaWorkerThread::~ZPMetaWorkerThread() {
  DestoryCmdTable(cmds_);

  should_exit_ = true;
  pthread_join(thread_id(), NULL);
  LOG(INFO) << "A meta worker thread" << thread_id() << " exit!!!";
}

int ZPMetaWorkerThread::ThreadClientNum() {
  slash::RWLock l(&rwlock_, false);
  return conns_.size();
}

void ZPMetaWorkerThread::InitClientCmdTable() {
  // Join Command
  Cmd* joinptr = new JoinCmd(kCmdFlagsWrite);
  cmds_.insert(std::pair<int, Cmd*>(static_cast<int>(ZPMeta::MetaCmd_Type::MetaCmd_Type_JOIN), joinptr));

  // Ping Command
  Cmd* pingptr = new PingCmd(kCmdFlagsRead);
  cmds_.insert(std::pair<int, Cmd*>(static_cast<int>(ZPMeta::MetaCmd_Type::MetaCmd_Type_PING), pingptr));

  //Pull Command
  Cmd* pullptr = new PullCmd(kCmdFlagsRead);
  cmds_.insert(std::pair<int, Cmd*>(static_cast<int>(ZPMeta::MetaCmd_Type::MetaCmd_Type_PULL), pullptr));

  //Init Command
  Cmd* initptr = new InitCmd(kCmdFlagsWrite);
  cmds_.insert(std::pair<int, Cmd*>(static_cast<int>(ZPMeta::MetaCmd_Type::MetaCmd_Type_INIT), pullptr));
}

Cmd* ZPMetaWorkerThread::GetCmd(const int op) {
  return GetCmdFromTable(op, cmds_);
}
