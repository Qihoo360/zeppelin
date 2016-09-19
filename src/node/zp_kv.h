#ifndef ZP_KV_H
#define ZP_KV_H

#include "zp_command.h"

////// kv //////
class SetCmd : public Cmd {
 public:
  SetCmd(int flag) : Cmd(flag) {}
  virtual Status Init(google::protobuf::Message *req);
  virtual void Do(google::protobuf::Message *req, google::protobuf::Message *res, void* partition, bool readonly = false);
  virtual std::string key() { 
    return key_; 
  }

 private:
  std::string key_;
};

class GetCmd : public Cmd {
 public:
  GetCmd(int flag) : Cmd(flag) {}
  //virtual Status Init(const void *buf, size_t count);
  //virtual void Do();
  virtual void Do(google::protobuf::Message *req, google::protobuf::Message *res, void* partition, bool readonly = false);
  virtual std::string key() { 
    return key_; 
  }
  
 private:
  std::string key_;
};

////// Sync //////
class SyncCmd : public Cmd {
 public:
  SyncCmd(int flag) : Cmd(flag) {}
  //virtual Status Init(const void *buf, size_t count);
  virtual void Do(google::protobuf::Message *req, google::protobuf::Message *res, void* partition, bool readonly = false);

  void set_fd(int fd) {
    fd_ = fd;
  }

 private:
  int fd_;
};


void InitClientCmdTable(std::unordered_map<int, Cmd*> *cmd_table);

#endif
