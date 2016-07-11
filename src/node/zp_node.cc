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
  ::google::InitGoogleLogging("zp");
}

int main(int argc, char** argv) {
  ZPOptions options;

  ParseArgs(argc, argv, options);

  options.Dump();
  GlogInit(options);

  signal(SIGPIPE, SIG_IGN);

  zp_data_server = new ZPDataServer(options);

  zp_data_server->Start();

  printf ("Exit\n");

  return 0;
}

void Usage() {
  printf ("Usage:\n"
          "  ./output/bin/zp --floyd_ip ip1 --floyd_port port1 --local_port local_port --data_path path --log_path path\n");
}

void ParseArgs(int argc, char* argv[], ZPOptions& options) {
  if (argc < 1) {
    Usage();
    exit(-1);
  }

  const struct option long_options[] = {
    {"floyd_ip", required_argument, NULL, 'I'},
    {"floyd_port", required_argument, NULL, 'P'},
    {"local_port", required_argument, NULL, 'p'},
    {"data_path", required_argument, NULL, 'd'},
    {"log_path", required_argument, NULL, 'l'},
    {"help", no_argument, NULL, 'h'},
    {NULL, 0, NULL, 0}, };

  const char* short_options = "I:P:p:d:l:h";

  int ch, longindex;
  while ((ch = getopt_long(argc, argv, short_options, long_options,
                           &longindex)) >= 0) {
    switch (ch) {
      case 'I':
        options.seed_ip = optarg;
        break;
      case 'P':
        options.seed_port = atoi(optarg);
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
