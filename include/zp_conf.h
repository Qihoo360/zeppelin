#ifndef INCLUDE_ZP_CONF_H_
#define INCLUDE_ZP_CONF_H_

#include <string>
#include <vector>

#include "base_conf.h"
#include "slash_string.h"
#include "slash_mutex.h"

#include "zp_const.h"

typedef slash::RWLock RWLock;

class ZpConf {
 public:
  ZpConf();
  ~ZpConf();

  void Dump() const;

  int Load(const std::string& path);

  std::string seed_ip() {
    RWLock l(&rwlock_, false);
    return seed_ip_;
  }

  int seed_port() {
    RWLock l(&rwlock_, false);
    return seed_port_;
  }

  std::string local_ip() {
    RWLock l(&rwlock_, false);
    return local_ip_;
  }

  int local_port() {
    RWLock l(&rwlock_, false);
    return local_port_;
  }

  int64_t timeout() {
    RWLock l(&rwlock_, false);
    return timeout_;
  }

  std::string data_path() {
    RWLock l(&rwlock_, false);
    return data_path_;
  }

  std::string log_path() {
    RWLock l(&rwlock_, false);
    return log_path_;
  }

  bool daemonize() {
    RWLock l(&rwlock_, false);
    return daemonize_;
  }

  std::string pid_file() {
    RWLock l(&rwlock_, false);
    return pid_file_;
  }

  std::string lock_file() {
    RWLock l(&rwlock_, false);
    return lock_file_;
  }

  std::vector<std::string>& meta_addr() {
    RWLock l(&rwlock_, false);
    return meta_addr_;
  }

  int meta_thread_num() {
    RWLock l(&rwlock_, false);
    return meta_thread_num_;
  }
  int data_thread_num() {
    RWLock l(&rwlock_, false);
    return data_thread_num_;
  }
  int sync_recv_thread_num() {
    RWLock l(&rwlock_, false);
    return sync_recv_thread_num_;
  }
  int sync_send_thread_num() {
    RWLock l(&rwlock_, false);
    return sync_send_thread_num_;
  }
  int max_background_flushes() {
    RWLock l(&rwlock_, false);
    return max_background_flushes_;
  }
  int max_background_compactions() {
    RWLock l(&rwlock_, false);
    return max_background_compactions_;
  }
  int slowlog_slower_than() {
    RWLock l(&rwlock_, false);
    return slowlog_slower_than_;
  }
  int db_write_buffer_size() {
    RWLock l(&rwlock_, false);
    return db_write_buffer_size_;
  }
  int db_max_write_buffer() {
    RWLock l(&rwlock_, false);
    return db_max_write_buffer_;
  }
  int db_target_file_size_base() {
    RWLock l(&rwlock_, false);
    return db_target_file_size_base_;
  }
  int db_max_open_files() {
    RWLock l(&rwlock_, false);
    return db_max_open_files_;
  }
  int db_block_size() {
    RWLock l(&rwlock_, false);
    return db_block_size_;
  }

 private:
  // copy disallowded
  ZpConf(const ZpConf& options);

  // Env
  std::vector<std::string> meta_addr_;
  std::string seed_ip_;
  int seed_port_;
  std::string local_ip_;
  int local_port_;
  int64_t timeout_;
  std::string data_path_;
  std::string log_path_;
  bool daemonize_;
  std::string pid_file_;
  std::string lock_file_;

  // Thread Num
  int meta_thread_num_;
  int data_thread_num_;
  int sync_recv_thread_num_;
  int sync_send_thread_num_;
  int max_background_flushes_;
  int max_background_compactions_;

  // DB
  int db_write_buffer_size_; //KB
  int db_max_write_buffer_; //KB
  int db_target_file_size_base_; //KB
  int db_max_open_files_;
  int db_block_size_; //KB

  // Feature
  int slowlog_slower_than_;

  pthread_rwlock_t rwlock_;
};

#endif
