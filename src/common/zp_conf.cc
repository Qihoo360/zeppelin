#include "include/zp_conf.h"

#include "slash/include/base_conf.h"

static int64_t BoundaryLimit(int64_t target, int64_t floor, int64_t ceil) {
  target = (target < floor) ? floor : target;
  target = (target > ceil) ? ceil : target;
  return target;
}

ZpConf::ZpConf() {
  pthread_rwlock_init(&rwlock_, NULL);
  local_ip_ = std::string("127.0.0.1");
  local_port_ = 9999;
  timeout_ = 100;
  data_path_ = std::string("./data/");
  log_path_ = std::string("./log/");
  trash_path_ = std::string("./trash/");
  daemonize_ = false;
  pid_file_ = std::string("./pid");
  lock_file_ = std::string("./lock");
  enable_data_delete_ = true;
  meta_thread_num_ = 4;
  data_thread_num_ = 6;
  sync_recv_thread_num_ = 4;
  sync_send_thread_num_ = 4;
  max_background_flushes_ = 24;
  max_background_compactions_ = 24;
  db_write_buffer_size_ = 256 * 1024; // 256M
  db_max_write_buffer_ = 20 * 1024 * 1024; // 20G
  db_target_file_size_base_ = 256 * 1024; // 256M
  db_max_open_files_ = 4096;
  db_block_size_ = 16; // 16K
  slowlog_slower_than_ = -1;
  stuck_offset_dist_ = 10 * 1024;
  slowdown_delay_radio_ = 60;
  floyd_check_leader_us_ = 15000000;
  floyd_heartbeat_us_ = 6000000;
  floyd_append_entries_size_once_ = 1024000;
  floyd_append_entries_count_once_ = 128;
}

ZpConf::~ZpConf() {
  pthread_rwlock_destroy(&rwlock_);
}

void ZpConf::Dump() const {
  auto iter = meta_addr_.begin();
  while (iter != meta_addr_.end()) {
    fprintf(stderr, "    Config.meta_addr   : %s\n", iter->c_str());
    iter++;
  }
  fprintf (stderr, "    Config.local_ip    : %s\n", local_ip_.c_str());
  fprintf (stderr, "    Config.local_port  : %d\n", local_port_);
  fprintf (stderr, "    Config.data_path   : %s\n", data_path_.c_str());
  fprintf (stderr, "    Config.log_path    : %s\n", log_path_.c_str());
  fprintf (stderr, "    Config.trash_path    : %s\n", trash_path_.c_str());
  fprintf (stderr, "    Config.daemonize    : %s\n", daemonize_? "true":"false");
  fprintf (stderr, "    Config.pid_file    : %s\n", pid_file_.c_str());
  fprintf (stderr, "    Config.lock_file    : %s\n", lock_file_.c_str());
  fprintf (stderr, "    Config.enable_data_delete    : %s\n", enable_data_delete_ ? "true":"false");
  fprintf (stderr, "    Config.meta_thread_num    : %d\n", meta_thread_num_);
  fprintf (stderr, "    Config.data_thread_num    : %d\n", data_thread_num_);
  fprintf (stderr, "    Config.sync_recv_thread_num   : %d\n", sync_recv_thread_num_);
  fprintf (stderr, "    Config.sync_send_thread_num   : %d\n", sync_send_thread_num_);
  fprintf (stderr, "    Config.max_background_flushes    : %d\n", max_background_flushes_);
  fprintf (stderr, "    Config.max_background_compactions   : %d\n", max_background_compactions_);
  fprintf (stderr, "    Config.db_write_buffer_size   : %dKB\n", db_write_buffer_size_);
  fprintf (stderr, "    Config.db_max_write_buffer   : %dKB\n", db_max_write_buffer_);
  fprintf (stderr, "    Config.db_target_file_size_base   : %dKB\n", db_target_file_size_base_);
  fprintf (stderr, "    Config.db_max_open_files   : %d\n", db_max_open_files_);
  fprintf (stderr, "    Config.db_block_size   : %dKB\n", db_block_size_);
  fprintf (stderr, "    Config.slowlog_slower_than   : %d\n", slowlog_slower_than_);
  fprintf (stderr, "    Config.stuck_offset_dist   : %d\n", stuck_offset_dist_);
  fprintf (stderr, "    Config.slowdown_delay_radio   : %d\n", slowdown_delay_radio_);
  fprintf (stderr, "    Config.floyd_check_leader_us   : %d\n", floyd_check_leader_us_);
  fprintf (stderr, "    Config.floyd_heartbeat_us   : %d\n", floyd_heartbeat_us_);
  fprintf (stderr, "    Config.floyd_append_entries_size_once_   : %d\n", floyd_append_entries_size_once_);
  fprintf (stderr, "    Config.floyd_append_entries_count_once_   : %d\n", floyd_append_entries_count_once_);
}

int ZpConf::Load(const std::string& path) {
  slash::BaseConf conf_reader(path);
  int res = conf_reader.LoadConf();
  if (res != 0) {
    return res;
  }

  bool ret = false;
  ret = conf_reader.GetConfStr("local_ip", &local_ip_);
  ret = conf_reader.GetConfInt("local_port", &local_port_);
  ret = conf_reader.GetConfStr("data_path", &data_path_);
  ret = conf_reader.GetConfStr("log_path", &log_path_);
  ret = conf_reader.GetConfStr("trash_path", &trash_path_);
  ret = conf_reader.GetConfBool("daemonize", &daemonize_);
  ret = conf_reader.GetConfStrVec("meta_addr", &meta_addr_);
  ret = conf_reader.GetConfBool("enable_data_delete", &enable_data_delete_);
  ret = conf_reader.GetConfInt("meta_thread_num", &meta_thread_num_);
  ret = conf_reader.GetConfInt("data_thread_num", &data_thread_num_);
  ret = conf_reader.GetConfInt("sync_recv_thread_num", &sync_recv_thread_num_);
  ret = conf_reader.GetConfInt("sync_send_thread_num", &sync_send_thread_num_);
  ret = conf_reader.GetConfInt("max_background_flushes", &max_background_flushes_);
  ret = conf_reader.GetConfInt("max_background_compactions", &max_background_compactions_);
  ret = conf_reader.GetConfInt("db_write_buffer_size", &db_write_buffer_size_);
  ret = conf_reader.GetConfInt("db_max_write_buffer", &db_max_write_buffer_);
  ret = conf_reader.GetConfInt("db_target_file_size_base", &db_target_file_size_base_);
  ret = conf_reader.GetConfInt("db_max_open_files", &db_max_open_files_);
  ret = conf_reader.GetConfInt("db_block_size", &db_block_size_);
  ret = conf_reader.GetConfInt("slowlog_slower_than", &slowlog_slower_than_);
  ret = conf_reader.GetConfInt("stuck_offset_dist", &stuck_offset_dist_);
  ret = conf_reader.GetConfInt("slowdown_delay_radio", &slowdown_delay_radio_);
  ret = conf_reader.GetConfInt("floyd_check_leader_us", &floyd_check_leader_us_);
  ret = conf_reader.GetConfInt("floyd_heartbeat_us", &floyd_heartbeat_us_);
  ret = conf_reader.GetConfInt("floyd_append_entries_size_once", &floyd_append_entries_size_once_);
  ret = conf_reader.GetConfInt("floyd_append_entries_count_once", &floyd_append_entries_count_once_);
  
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
  slowlog_slower_than_ = BoundaryLimit(slowlog_slower_than_, -1, 10000000);
  stuck_offset_dist_ = BoundaryLimit(stuck_offset_dist_, 1, 100 * 1024 * 1024);
  slowdown_delay_radio_ = BoundaryLimit(slowdown_delay_radio_, 1, 100);
  db_write_buffer_size_ = BoundaryLimit(db_write_buffer_size_, 4 * 1024, 10 * 1024 * 1024); // 4M ~ 10G
  db_max_write_buffer_ = BoundaryLimit(db_max_write_buffer_, 1024 * 1024, 500 * 1024 * 1024); // 1G ~ 500G
  db_target_file_size_base_ = BoundaryLimit(db_target_file_size_base_, 4 * 1024, 10 * 1024 * 1024); // 4M ~ 10G
  db_block_size_ = BoundaryLimit(db_block_size_, 4, 1024 * 1024); // 14K ~ 1G
  return ret;
}
