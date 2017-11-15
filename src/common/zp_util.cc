#include "include/zp_util.h"

#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "include/zp_const.h"

#include "slash/include/env.h"

extern ZpConf* g_zp_conf;

void daemonize() {
  if (fork() != 0) exit(0); /* parent exits */
  setsid(); /* create a new session */
}

void close_std() {
  int fd;
  if ((fd = open("/dev/null", O_RDWR, 0)) != -1) {
    dup2(fd, STDIN_FILENO);
    dup2(fd, STDOUT_FILENO);
    dup2(fd, STDERR_FILENO);
    if (fd > STDERR_FILENO) close(fd);
  }
}

void create_pid_file() {
  /* Try to write the pid file in a best-effort way. */
  std::string path(g_zp_conf->pid_file());

  size_t pos = path.find_last_of('/');
  if (pos != std::string::npos) {
    // mkpath(path.substr(0, pos).c_str(), 0755);
    slash::CreateDir(path.substr(0, pos));
  } else {
    path = kZpPidFile;
  }

  FILE *fp = fopen(path.c_str(), "w");
  if (fp) {
    fprintf(fp,"%d\n",(int)getpid());
    fclose(fp);
  }
}

FileLocker::FileLocker(const std::string& file)
  : file_(file) {
}

slash::Status FileLocker::Lock() {
  slash::Status s = slash::LockFile(file_, &file_lock_);
  return s;
}

FileLocker::~FileLocker() {
  if (file_lock_) {
    slash::UnlockFile(file_lock_);
  }
}

Statistic::Statistic()
    : last_querys(0),
      querys(0),
      last_qps(0),
      used_disk(0),
      free_disk(0),
      read_queries(0),
      write_queries(0),
      read_max_latency(0),
      read_avg_latency(0),
      read_min_latency(0),
      write_max_latency(0),
      write_avg_latency(0),
      write_min_latency(0) {
}

Statistic::Statistic(const Statistic& stat)
    : table_name(stat.table_name),
      last_querys(stat.last_querys),
      querys(stat.querys),
      last_qps(stat.last_qps),
      used_disk(stat.used_disk),
      free_disk(stat.free_disk),
      read_queries(stat.read_queries),
      write_queries(stat.write_queries),
      read_max_latency(stat.read_max_latency),
      read_avg_latency(stat.read_avg_latency),
      read_min_latency(stat.read_min_latency),
      write_max_latency(stat.write_max_latency),
      write_avg_latency(stat.write_avg_latency),
      write_min_latency(stat.write_min_latency) {
}

void Statistic::Reset() {
  table_name.clear();
  last_querys = 0;
  querys = 0;
  last_qps = 0;
  used_disk = 0;
  free_disk = 0;
  read_queries = 0;
  write_queries = 0;
  read_max_latency = 0;
  read_avg_latency = 0;
  read_min_latency = 0;
  write_max_latency = 0;
  write_avg_latency = 0;
  write_min_latency = 0;
}

void Statistic::Add(const Statistic& stat) {
  last_querys += stat.last_querys;
  querys += stat.querys;
  last_qps += stat.last_qps;
  used_disk += stat.used_disk;
  free_disk += stat.free_disk;
}

void Statistic::Dump() {
  printf ("   table_name : %s\n"
          "   last_querys: %lu\n"
          "   querys     : %lu\n"
          "   last_qps   : %lu\n"
          "   used_disk  : %lu\n"
          "   free_disk  : %lu\n",
          table_name.c_str(), last_querys, querys,
          last_qps, used_disk, free_disk);
  DLOG(INFO) << "   table_name : " << table_name
      << "\n   last_querys: " << last_querys
      << "\n   querys     : " << querys
      << "\n   last_qps   : " << last_qps
      << "\n   used_disk  : " << used_disk
      << "\n   free_disk  : " << free_disk << "\n";
}
