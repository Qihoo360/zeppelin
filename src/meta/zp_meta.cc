#include <stdio.h>
#include <unistd.h>
#include <getopt.h>
#include <iostream>
#include <sstream>
#include <glog/logging.h>

#include "env.h"

#include "zp_meta_server.h"
#include "zp_conf.h"


ZpConf *g_zp_conf;
ZPMetaServer* g_meta_server;

void Usage();
void ParseArgsFromFile(int argc, char* argv[]);

static void GlogInit() {
  if (!slash::FileExists(g_zp_conf->log_path())) {
    slash::CreatePath(g_zp_conf->log_path());
  }

  FLAGS_alsologtostderr = true;

  FLAGS_log_dir = g_zp_conf->log_path();
  FLAGS_minloglevel = 0;
  FLAGS_max_log_size = 1800;
  ::google::InitGoogleLogging("zp");
}

static void IntSigHandle(const int sig) {
  LOG(INFO) << "Catch Signal " << sig << ", cleanup...";
  g_meta_server->Stop();
}

static void SignalSetup() {
  signal(SIGHUP, SIG_IGN);
  signal(SIGPIPE, SIG_IGN);
  signal(SIGINT, &IntSigHandle);
  signal(SIGQUIT, &IntSigHandle);
  signal(SIGTERM, &IntSigHandle);
}


void Usage() {
  printf("Usage:\n"
          "  .zp-meta -c conf\n"
          "  .zp-meta -v\n");
}


void ZpConfInit(int argc, char* argv[]) {
  if (argc < 1) {
    Usage();
    exit(-1);
  }
  bool path_opt = false;
  char c;
  char path[1024];
  while (-1 != (c = getopt(argc, argv, "c:hv"))) {
    switch (c) {
      case 'c':
        snprintf(path, 1024, "%s", optarg);
        path_opt = true;
        break;
      case 'h':
        Usage();
        exit(-1);
      case 'v':
        std::cout << "Zeppelin " << std::endl;
        std::cout << "Git ver: " << kZPVersion << std::endl;
        std::cout << "Date:    " << kZPCompileDate << std::endl;
        exit(0);
      default:
        Usage();
        exit(-1);
    }
  }

  if (path_opt == false) {
    fprintf(stderr, "Please specify the conf file path\n");
    Usage();
    exit(-1);
  }

  g_zp_conf = new ZpConf();
  if (g_zp_conf->Load(path) != 0) {
    LOG(FATAL) << "zp-meta load conf file error";
  }
  g_zp_conf->Dump();
}

int main(int argc, char** argv) {
  ZpConfInit(argc, argv);

  GlogInit();
  SignalSetup();

  if (g_zp_conf->daemonize()) {
    daemonize();
  }

  FileLocker db_lock(g_zp_conf->lock_file());
  Status s = db_lock.Lock();
  if (!s.ok()) {
    return 0;
  }

  if (g_zp_conf->daemonize()) {
    create_pid_file();
    close_std();
  }

  g_meta_server = new ZPMetaServer();
  g_meta_server->Start();

  printf("Exit\n");
  return 0;
}

