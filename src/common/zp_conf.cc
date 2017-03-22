#include "zp_conf.h"

#define READCONFINT(reader, attr, value) \
  reader.GetConfInt(std::string(#attr), &value)
#define READCONFBOOL(reader, attr, value) \
  reader.GetConfBool(std::string(#attr), &value)
#define READCONFSTR(reader, attr, value) \
  reader.GetConfStr(std::string(#attr), &value)
#define READCONFSTRVEC(reader, attr, value) \
  reader.GetConfStrVec(std::string(#attr), &value)

#define READCONF(reader, attr, value, type) \
  ret = READCONF##type(reader, attr, value); \
  if (!ret) printf("%s not set,use default\n", #attr)


static int BoundaryLimit(int target, int floor, int ceil) {
  target = (target < floor) ? floor : target;
  target = (target > ceil) ? ceil : target;
  return target;
}

ZpConf::ZpConf() {
  pthread_rwlock_init(&rwlock_, NULL);
  seed_ip_ = std::string("127.0.0.1");
  seed_port_ = 0;
  local_ip_ = std::string("127.0.0.1");
  local_port_ = 9999;
  timeout_ = 100;
  data_path_ = std::string("./data/");
  log_path_ = std::string("./log/");
  daemonize_ = false;
  pid_file_ = std::string("./pid");
  lock_file_ = std::string("./lock");
  meta_thread_num_ = 4;
  data_thread_num_ = 6;
  sync_recv_thread_num_ = 4;
  sync_send_thread_num_ = 4;
  max_background_flushes_ = 24;
  max_background_compactions_ = 24;
  slowlog_slower_than_ = -1;
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
  fprintf (stderr, "    Config.daemonize    : %s\n", daemonize_? "true":"false");
  fprintf (stderr, "    Config.pid_file    : %s\n", pid_file_.c_str());
  fprintf (stderr, "    Config.lock_file    : %s\n", lock_file_.c_str());
  fprintf (stderr, "    Config.meta_thread_num    : %d\n", meta_thread_num_);
  fprintf (stderr, "    Config.data_thread_num    : %d\n", data_thread_num_);
  fprintf (stderr, "    Config.sync_recv_thread_num   : %d\n", sync_recv_thread_num_);
  fprintf (stderr, "    Config.sync_send_thread_num   : %d\n", sync_send_thread_num_);
  fprintf (stderr, "    Config.max_background_flushes    : %d\n", max_background_flushes_);
  fprintf (stderr, "    Config.max_background_compactions   : %d\n", max_background_compactions_);
  fprintf (stderr, "    Config.slowlog_slower_than   : %d\n", slowlog_slower_than_);
}

int ZpConf::Load(const std::string& path) {
  slash::BaseConf conf_reader(path);
  int res = conf_reader.LoadConf();
  if (res != 0) {
    return res;
  }

  bool ret = false;
  READCONF(conf_reader, local_ip, local_ip_, STR);
  READCONF(conf_reader, local_port, local_port_, INT);
  READCONF(conf_reader, data_path, data_path_, STR);
  READCONF(conf_reader, log_path, log_path_, STR);
  READCONF(conf_reader, daemonize, daemonize_, BOOL);
  READCONF(conf_reader, meta_addr, meta_addr_, STRVEC);
  READCONF(conf_reader, meta_thread_num, meta_thread_num_, INT);
  READCONF(conf_reader, data_thread_num, data_thread_num_, INT);
  READCONF(conf_reader, sync_recv_thread_num, sync_recv_thread_num_, INT);
  READCONF(conf_reader, sync_send_thread_num, sync_send_thread_num_, INT);
  READCONF(conf_reader, max_background_flushes, max_background_flushes_, INT);
  READCONF(conf_reader, max_background_compactions, max_background_compactions_, INT);
  READCONF(conf_reader, slowlog_slower_than, slowlog_slower_than_, INT);
  std::string lock_path = log_path_;
  if (lock_path.back() != '/') {
    lock_path.append("/");
  }
  pid_file_ = lock_path + "pid";
  lock_file_ = lock_path + "lock";

  meta_thread_num_ = BoundaryLimit(meta_thread_num_, 1, kMaxMetaWorkerThread);
  data_thread_num_ = BoundaryLimit(data_thread_num_, 1, kMaxDataWorkerThread);
  sync_recv_thread_num_ = BoundaryLimit(sync_recv_thread_num_, 1, 100);
  sync_send_thread_num_ = BoundaryLimit(sync_send_thread_num_, 1, 100);
  max_background_flushes_ = BoundaryLimit(max_background_flushes_, 10, 100);
  max_background_compactions_ = BoundaryLimit(max_background_compactions_, 10, 100);
  slowlog_slower_than_ = BoundaryLimit(slowlog_slower_than_, -1, 10000000);
  return res;
}
