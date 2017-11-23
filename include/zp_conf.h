#ifndef INCLUDE_ZP_CONF_H_
#define INCLUDE_ZP_CONF_H_

#include <string>
#include <vector>

#include "include/zp_const.h"

#include "slash/include/slash_string.h"
#include "slash/include/slash_mutex.h"

typedef slash::RWLock RWLock;

class ZpConf {
 public:
  ZpConf();
  ~ZpConf();

  void Dump() const;

  int Load(const std::string& path);

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

  std::string trash_path() {
    RWLock l(&rwlock_, false);
    return trash_path_;
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

  bool enable_data_delete() {
    RWLock l(&rwlock_, false);
    return enable_data_delete_;
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
  int binlog_remain_days() {
    RWLock l(&rwlock_, false);
    return binlog_remain_days_;
  }
  int binlog_remain_min_count() {
    RWLock l(&rwlock_, false);
    return binlog_remain_min_count_;
  }
  int binlog_remain_max_count() {
    RWLock l(&rwlock_, false);
    return binlog_remain_max_count_;
  }
  int slowlog_slower_than() {
    RWLock l(&rwlock_, false);
    return slowlog_slower_than_;
  }
  int stuck_offset_dist() {
    RWLock l(&rwlock_, false);
    return stuck_offset_dist_;
  }
  int slowdown_delay_radio() {
    RWLock l(&rwlock_, false);
    return slowdown_delay_radio_;
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
  int floyd_check_leader_us() {
    RWLock l(&rwlock_, false);
    return floyd_check_leader_us_;
  }

  int floyd_heartbeat_us() {
    RWLock l(&rwlock_, false);
    return floyd_heartbeat_us_;
  }

  int floyd_append_entries_size_once() {
    RWLock l(&rwlock_, false);
    return floyd_append_entries_size_once_;
  }
  int floyd_append_entries_count_once() {
    RWLock l(&rwlock_, false);
    return floyd_append_entries_count_once_;
  }

 private:

  // Env
  std::vector<std::string> meta_addr_;
  std::string local_ip_;
  int local_port_;
  int64_t timeout_;
  std::string data_path_;
  std::string log_path_;
  std::string trash_path_;
  bool daemonize_;
  std::string pid_file_;
  std::string lock_file_;
  bool enable_data_delete_;

  // Thread Num
  int meta_thread_num_;
  int data_thread_num_;
  int sync_recv_thread_num_;
  int sync_send_thread_num_;
  int max_background_flushes_;
  int max_background_compactions_;

  // Binlog related
  int binlog_remain_days_;
  int binlog_remain_min_count_;
  int binlog_remain_max_count_;

  // DB
  int db_write_buffer_size_; // KB
  int db_max_write_buffer_; // KB
  int db_target_file_size_base_; // KB
  int db_max_open_files_; 
  int db_block_size_; //KB

  // Feature
  int slowlog_slower_than_;
  int stuck_offset_dist_;
  int slowdown_delay_radio_;  // Percent

  // Floyd options
  int floyd_check_leader_us_;
  int floyd_heartbeat_us_;
  int floyd_append_entries_size_once_;
  int floyd_append_entries_count_once_;

  pthread_rwlock_t rwlock_;

  // copy disallowded
  ZpConf(const ZpConf& options);
};

#endif
