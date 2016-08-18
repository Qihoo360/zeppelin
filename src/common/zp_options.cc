#include "zp_options.h"

////// ZPOptions //////
ZPOptions::ZPOptions()
  : seed_ip("127.0.0.1"),
    seed_port(8001),
    local_ip("127.0.0.1"),
    local_port(8001),
    data_path("./data"),
    log_path("./log") {
    }


ZPOptions::ZPOptions(const ZPOptions& options)
  : meta_addr(options.meta_addr),
    seed_ip(options.seed_ip),
    seed_port(options.seed_port),
    local_ip(options.local_ip),
    local_port(options.local_port),
    data_path(options.data_path),
    log_path(options.log_path) {
}

void ZPOptions::Dump() {
  auto iter = meta_addr.begin();
  while(iter != meta_addr.end()) {
    fprintf (stderr, "    Options.meta_addr   : %s\n", iter->c_str());
    iter++;
  }
  fprintf (stderr, "    Options.seed_ip     : %s\n", seed_ip.c_str());
  fprintf (stderr, "    Options.seed_port   : %d\n", seed_port);
  fprintf (stderr, "    Options.local_ip    : %s\n", local_ip.c_str());
  fprintf (stderr, "    Options.local_port  : %d\n", local_port);
  fprintf (stderr, "    Options.data_path   : %s\n", data_path.c_str());
  fprintf (stderr, "    Options.log_path    : %s\n", log_path.c_str());
}
