#include "include/zp_conf.h"
#include "include/zp_const.h"

#include "slash/include/base_conf.h"

static int64_t BoundaryLimit(int64_t target, int64_t floor, int64_t ceil) {
  target = (target < floor) ? floor : target;
  target = (target > ceil) ? ceil : target;
  return target;
}

ZpConf::ZpConf(const std::string& path)
  : conf_adaptor_(path),
  local_ip_("127.0.0.1"),
  local_port_(9999),
  timeout_(100),
  data_path_("data"),
  log_path_("log"),
  trash_path_("trash"),
  daemonize_(false),
  pid_file_(log_path_ + "/" + kZpPidFile),
  lock_file_(log_path_ + "/" + kZpLockFile),
  enable_data_delete_(true),
  meta_thread_num_(4),
  data_thread_num_(6),
  sync_recv_thread_num_(4),
  sync_send_thread_num_(4),
  max_background_flushes_(24),
  max_background_compactions_(24),
  binlog_remain_days_(kBinlogRemainMaxDay),
  binlog_remain_min_count_(kBinlogRemainMinCount),
  binlog_remain_max_count_(kBinlogRemainMaxCount),
  db_write_buffer_size_(256 * 1024), // 256KB
  db_max_write_buffer_(20 * 1024 * 1024), // 20MB
  db_target_file_size_base_(256 * 1024), // 256KB
  db_max_open_files_(4096),
  db_block_size_(16), // 16 B
  slowlog_slower_than_(-1),
  stuck_offset_dist_(kMetaOffsetStuckDist), // 100KB
  slowdown_delay_radio_(kSlowdownDelayRatio),  // 60%
  floyd_check_leader_us_(15000000),
  floyd_heartbeat_us_(6000000),
  floyd_append_entries_size_once_(1024000),
  floyd_append_entries_count_once_(128) {
    pthread_rwlock_init(&rwlock_, NULL);
  }

ZpConf::~ZpConf() {
  pthread_rwlock_destroy(&rwlock_);
}

void ZpConf::Dump() const {
  auto iter = meta_addr_.begin();
  while (iter != meta_addr_.end()) {
    fprintf(stderr, "    Config.meta_addr         : %s\n", iter->c_str());
    iter++;
  }
  fprintf (stderr, "    Config.local_ip           : %s\n", local_ip_.c_str());
  fprintf (stderr, "    Config.local_port         : %d\n", local_port_);
  fprintf (stderr, "    Config.data_path          : %s\n", data_path_.c_str());
  fprintf (stderr, "    Config.log_path           : %s\n", log_path_.c_str());
  fprintf (stderr, "    Config.trash_path         : %s\n", trash_path_.c_str());
  fprintf (stderr, "    Config.daemonize          : %s\n", daemonize_? "true":"false");
  fprintf (stderr, "    Config.pid_file           : %s\n", pid_file_.c_str());
  fprintf (stderr, "    Config.lock_file          : %s\n", lock_file_.c_str());
  fprintf (stderr, "    Config.enable_data_delete : %s\n", enable_data_delete_ ? "true":"false");

  fprintf (stderr, "    Config.meta_thread_num            : %d\n", meta_thread_num_);
  fprintf (stderr, "    Config.data_thread_num            : %d\n", data_thread_num_);
  fprintf (stderr, "    Config.sync_recv_thread_num       : %d\n", sync_recv_thread_num_);
  fprintf (stderr, "    Config.sync_send_thread_num       : %d\n", sync_send_thread_num_);
  fprintf (stderr, "    Config.max_background_flushes     : %d\n", max_background_flushes_);
  fprintf (stderr, "    Config.max_background_compactions : %d\n", max_background_compactions_);

  fprintf (stderr, "    Config.binlog_remain_days       : %d\n", binlog_remain_days_);
  fprintf (stderr, "    Config.binlog_remain_min_count  : %d\n", binlog_remain_min_count_);
  fprintf (stderr, "    Config.binlog_remain_max_count  : %d\n", binlog_remain_max_count_);

  fprintf (stderr, "    Config.db_write_buffer_size     : %dKB\n", db_write_buffer_size_ / 1024);
  fprintf (stderr, "    Config.db_max_write_buffer      : %dMB\n", db_max_write_buffer_ / 1024 / 1024);
  fprintf (stderr, "    Config.db_target_file_size_base : %dKB\n", db_target_file_size_base_ / 1024);
  fprintf (stderr, "    Config.db_max_open_files        : %d\n", db_max_open_files_);
  fprintf (stderr, "    Config.db_block_size            : %dB\n", db_block_size_);
  fprintf (stderr, "    Config.slowlog_slower_than      : %d\n", slowlog_slower_than_);
  fprintf (stderr, "    Config.stuck_offset_dist        : %dKB\n", stuck_offset_dist_ / 1024);
  fprintf (stderr, "    Config.slowdown_delay_radio     : %d%%\n", slowdown_delay_radio_);

  fprintf (stderr, "    Config.floyd_check_leader_us            : %d\n", floyd_check_leader_us_);
  fprintf (stderr, "    Config.floyd_heartbeat_us               : %d\n", floyd_heartbeat_us_);
  fprintf (stderr, "    Config.floyd_append_entries_size_once_  : %d\n", floyd_append_entries_size_once_);
  fprintf (stderr, "    Config.floyd_append_entries_count_once_ : %d\n", floyd_append_entries_count_once_);
}

bool ZpConf::Rewrite() {
  conf_adaptor_.SetConfStr("local_ip", local_ip_);
  conf_adaptor_.SetConfInt("local_port", local_port_);
  conf_adaptor_.SetConfStr("data_path", data_path_);
  conf_adaptor_.SetConfStr("log_path", log_path_);
  conf_adaptor_.SetConfStr("trash_path", trash_path_);
  conf_adaptor_.SetConfBool("daemonize", daemonize_);
  conf_adaptor_.SetConfStrVec("meta_addr", meta_addr_);
  conf_adaptor_.SetConfBool("enable_data_delete", enable_data_delete_);
  conf_adaptor_.SetConfInt("meta_thread_num", meta_thread_num_);
  conf_adaptor_.SetConfInt("data_thread_num", data_thread_num_);
  conf_adaptor_.SetConfInt("sync_recv_thread_num", sync_recv_thread_num_);
  conf_adaptor_.SetConfInt("sync_send_thread_num", sync_send_thread_num_);
  conf_adaptor_.SetConfInt("max_background_flushes", max_background_flushes_);
  conf_adaptor_.SetConfInt("max_background_compactions", max_background_compactions_);
  conf_adaptor_.SetConfInt("binlog_remain_days", binlog_remain_days_);
  conf_adaptor_.SetConfInt("binlog_remain_min_count", binlog_remain_min_count_);
  conf_adaptor_.SetConfInt("binlog_remain_max_count", binlog_remain_max_count_);
  conf_adaptor_.SetConfInt("db_write_buffer_size", db_write_buffer_size_);
  conf_adaptor_.SetConfInt("db_max_write_buffer", db_max_write_buffer_);
  conf_adaptor_.SetConfInt("db_target_file_size_base", db_target_file_size_base_);
  conf_adaptor_.SetConfInt("db_max_open_files", db_max_open_files_);
  conf_adaptor_.SetConfInt("db_block_size", db_block_size_);
  conf_adaptor_.SetConfInt("slowlog_slower_than", slowlog_slower_than_);
  conf_adaptor_.SetConfInt("stuck_offset_dist", stuck_offset_dist_);
  conf_adaptor_.SetConfInt("slowdown_delay_radio", slowdown_delay_radio_);
  conf_adaptor_.SetConfInt("floyd_check_leader_us", floyd_check_leader_us_);
  conf_adaptor_.SetConfInt("floyd_heartbeat_us", floyd_heartbeat_us_);
  conf_adaptor_.SetConfInt("floyd_append_entries_size_once", floyd_append_entries_size_once_);
  conf_adaptor_.SetConfInt("floyd_append_entries_count_once", floyd_append_entries_count_once_);
  return conf_adaptor_.WriteBack();
}

int ZpConf::Load() {
  int res = conf_adaptor_.LoadConf();
  if (res != 0) {
    return res;
  }

  bool ret = false;
  ret = conf_adaptor_.GetConfStr("local_ip", &local_ip_);
  ret = conf_adaptor_.GetConfInt("local_port", &local_port_);
  ret = conf_adaptor_.GetConfStr("data_path", &data_path_);
  ret = conf_adaptor_.GetConfStr("log_path", &log_path_);
  ret = conf_adaptor_.GetConfStr("trash_path", &trash_path_);
  ret = conf_adaptor_.GetConfBool("daemonize", &daemonize_);
  ret = conf_adaptor_.GetConfStrVec("meta_addr", &meta_addr_);
  ret = conf_adaptor_.GetConfBool("enable_data_delete", &enable_data_delete_);
  ret = conf_adaptor_.GetConfInt("meta_thread_num", &meta_thread_num_);
  ret = conf_adaptor_.GetConfInt("data_thread_num", &data_thread_num_);
  ret = conf_adaptor_.GetConfInt("sync_recv_thread_num", &sync_recv_thread_num_);
  ret = conf_adaptor_.GetConfInt("sync_send_thread_num", &sync_send_thread_num_);
  ret = conf_adaptor_.GetConfInt("max_background_flushes", &max_background_flushes_);
  ret = conf_adaptor_.GetConfInt("max_background_compactions", &max_background_compactions_);
  ret = conf_adaptor_.GetConfInt("binlog_remain_days", &binlog_remain_days_);
  ret = conf_adaptor_.GetConfInt("binlog_remain_min_count", &binlog_remain_min_count_);
  ret = conf_adaptor_.GetConfInt("binlog_remain_max_count", &binlog_remain_max_count_);
  ret = conf_adaptor_.GetConfInt("db_write_buffer_size", &db_write_buffer_size_);
  ret = conf_adaptor_.GetConfInt("db_max_write_buffer", &db_max_write_buffer_);
  ret = conf_adaptor_.GetConfInt("db_target_file_size_base", &db_target_file_size_base_);
  ret = conf_adaptor_.GetConfInt("db_max_open_files", &db_max_open_files_);
  ret = conf_adaptor_.GetConfInt("db_block_size", &db_block_size_);
  ret = conf_adaptor_.GetConfInt("slowlog_slower_than", &slowlog_slower_than_);
  ret = conf_adaptor_.GetConfInt("stuck_offset_dist", &stuck_offset_dist_);
  ret = conf_adaptor_.GetConfInt("slowdown_delay_radio", &slowdown_delay_radio_);
  ret = conf_adaptor_.GetConfInt("floyd_check_leader_us", &floyd_check_leader_us_);
  ret = conf_adaptor_.GetConfInt("floyd_heartbeat_us", &floyd_heartbeat_us_);
  ret = conf_adaptor_.GetConfInt("floyd_append_entries_size_once", &floyd_append_entries_size_once_);
  ret = conf_adaptor_.GetConfInt("floyd_append_entries_count_once", &floyd_append_entries_count_once_);
  
  if (data_path_.back() != '/') {
    data_path_.append("/");
  }
  if (log_path_.back() != '/') {
    log_path_.append("/");
  }
  if (trash_path_.back() != '/') {
    trash_path_.append("/");
  }
  std::string lock_path = log_path_;
  pid_file_ = lock_path + "pid";
  lock_file_ = lock_path + "lock";

  meta_thread_num_ = BoundaryLimit(meta_thread_num_, 1, 100);
  data_thread_num_ = BoundaryLimit(data_thread_num_, 1, 100);
  sync_recv_thread_num_ = BoundaryLimit(sync_recv_thread_num_, 1, 100);
  sync_send_thread_num_ = BoundaryLimit(sync_send_thread_num_, 1, 100);
  max_background_flushes_ = BoundaryLimit(max_background_flushes_, 10, 100);
  max_background_compactions_ = BoundaryLimit(max_background_compactions_, 10, 100);
  binlog_remain_days_ = BoundaryLimit(binlog_remain_days_, 0, 30);
  binlog_remain_min_count_ = BoundaryLimit(binlog_remain_min_count_, 10, 60);
  binlog_remain_max_count_ = BoundaryLimit(binlog_remain_max_count_, 10, 60);
  binlog_remain_min_count_ = binlog_remain_min_count_ > binlog_remain_max_count_ ?
    binlog_remain_max_count_ : binlog_remain_min_count_;
  slowlog_slower_than_ = BoundaryLimit(slowlog_slower_than_, -1, 10000000);
  stuck_offset_dist_ = BoundaryLimit(stuck_offset_dist_, 1, 100 * 1024 * 1024);
  slowdown_delay_radio_ = BoundaryLimit(slowdown_delay_radio_, 1, 100);
  db_write_buffer_size_ = BoundaryLimit(db_write_buffer_size_, 4 * 1024, 10 * 1024 * 1024); // 4M ~ 10G
  db_max_write_buffer_ = BoundaryLimit(db_max_write_buffer_, 1024 * 1024, 500 * 1024 * 1024); // 1G ~ 500G
  db_target_file_size_base_ = BoundaryLimit(db_target_file_size_base_, 4 * 1024, 10 * 1024 * 1024); // 4M ~ 10G
  db_block_size_ = BoundaryLimit(db_block_size_, 4, 1024 * 1024); // 14K ~ 1G
  return ret;
}
