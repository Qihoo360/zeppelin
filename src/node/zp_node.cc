#include <stdio.h>
#include <unistd.h>
#include <getopt.h>
#include <iostream>
#include <sstream>
#include <glog/logging.h>

#include "zp_data_server.h"
#include "zp_options.h"

#include "env.h"

ZPDataServer* zp_data_server;

void Usage();
void ParseArgsFromFile(int argc, char* argv[], ZPOptions& options);

static void GlogInit(const ZPOptions& options) {
  if (!slash::FileExists(options.log_path)) {
    slash::CreatePath(options.log_path); 
  }

  FLAGS_alsologtostderr = true;

  FLAGS_log_dir = options.log_path;
  FLAGS_minloglevel = 0;
  FLAGS_max_log_size = 1800;
  // TODO rm
  FLAGS_logbufsecs = 0;

  ::google::InitGoogleLogging("zp");
}

void InitMetaAddr(ZPOptions &opt, std::string optarg) {
  
  std::string::size_type pos;
  while(true) {
    pos = optarg.find(",");
    if (pos == std::string::npos) {
      opt.meta_addr.push_back(optarg);
      break;
    }
    opt.meta_addr.push_back(optarg.substr(0, pos));
    optarg = optarg.substr(pos+1);
  }
}

static void IntSigHandle(const int sig) {
  LOG(INFO) << "Catch Signal " << sig << ", cleanup...";
  //zp_data_server->server_mutex_.Unlock();
  zp_data_server->Exit();
}

static void ZPDataSignalSetup() {
  signal(SIGHUP, SIG_IGN);
  signal(SIGPIPE, SIG_IGN);
  signal(SIGINT, &IntSigHandle);
  signal(SIGQUIT, &IntSigHandle);
  signal(SIGTERM, &IntSigHandle);
}

int main(int argc, char** argv) {
  ZPOptions options;

  ParseArgsFromFile(argc, argv, options);

  if (options.daemonize) {
    daemonize();
  }

  GlogInit(options);
  ZPDataSignalSetup();

  FileLocker db_lock(options.lock_file);
  Status s = db_lock.Lock();
  if (!s.ok()) {
    return 1;
  }

  if (options.daemonize) {
    create_pid_file(options);
    close_std();
  }

  zp_data_server = new ZPDataServer(options);

  zp_data_server->Start();

  //printf ("Exit\n");
  delete zp_data_server;

  ::google::ShutdownGoogleLogging();
  return 0;
}

void Usage() {
  printf ("Usage:\n"
          "  ./zp-node -c conf_file\n");
}

void ParseArgsFromFile(int argc, char* argv[], ZPOptions& options) {
  if (argc < 1) {
    Usage();
    exit(-1);
  }
  bool path_opt = false;
  char c;
  char path[1024];
  while (-1 != (c = getopt(argc, argv, "c:h"))) {
    switch (c) {
      case 'c':
        snprintf(path, 1024, "%s", optarg);
        path_opt = true;
        break;
      case 'h':
        Usage();
        exit(-1);
        return ;
      default:
        Usage();
        exit(-1);
    }
  }

  if (path_opt == false) {
    fprintf (stderr, "Please specify the conf file path\n" );
    Usage();
    exit(-1);
  }

  if (options.Load(path) != 0) {
    LOG(FATAL) << "zp-meta load conf file error";
  }
  options.Dump();
}
