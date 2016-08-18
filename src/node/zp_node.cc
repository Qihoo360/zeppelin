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
void ParseArgs(int argc, char* argv[], ZPOptions& options);

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
  zp_data_server->server_mutex_.Unlock();
  //zp_data_server->Exit();
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

  ParseArgs(argc, argv, options);

  options.Dump();
  GlogInit(options);
  ZPDataSignalSetup();

  zp_data_server = new ZPDataServer(options);

  zp_data_server->Start();

  //printf ("Exit\n");
  delete zp_data_server;

  ::google::ShutdownGoogleLogging();
  return 0;
}

void Usage() {
  printf ("Usage:\n"
          "  ./zp-node --meta_addr ip1:port1,ip2:port2 --local_ip local_ip --local_port local_port --data_path path --log_path path\n");
}

void ParseArgs(int argc, char* argv[], ZPOptions& options) {
  if (argc < 1) {
    Usage();
    exit(-1);
  }

  const struct option long_options[] = {
    {"meta_addr", required_argument, NULL, 'm'},
    {"local_ip", required_argument, NULL, 'n'},
    {"local_port", required_argument, NULL, 'p'},
    {"data_path", required_argument, NULL, 'd'},
    {"log_path", required_argument, NULL, 'l'},
    {"help", no_argument, NULL, 'h'},
    {NULL, 0, NULL, 0}, };

  const char* short_options = "m:n:p:d:l:h";

  int ch, longindex;
  while ((ch = getopt_long(argc, argv, short_options, long_options,
                           &longindex)) >= 0) {
    switch (ch) {
      case 'm':
        InitMetaAddr(options, optarg);
        break;
      case 'n':
        options.local_ip = optarg;
        break;
      case 'p':
        options.local_port = atoi(optarg);
        break;
      case 'd':
        options.data_path = optarg;
        break;
      case 'l':
        options.log_path = optarg;
        break;
      case 'h':
        Usage();
        exit(0);
      default:
        break;
    }
  }
}
