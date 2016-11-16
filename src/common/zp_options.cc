#include "zp_options.h"
#define READCONFINT(reader, attr, value) reader.GetConfInt(std::string(#attr), &value)
#define READCONFBOOL(reader, attr, value) reader.GetConfBool(std::string(#attr), &value)
#define READCONFSTR(reader, attr, value) reader.GetConfStr(std::string(#attr), &value)
#define READCONF(reader, attr, value, type) ret = READCONF##type(reader, attr, value); \
                                   if (!ret) printf("%s not set,use default\n",#attr)
////// ZPOptions //////
ZPOptions::ZPOptions()
  : seed_ip("127.0.0.1"),
    seed_port(8001),
    local_ip("127.0.0.1"),
    local_port(8001),
    data_path("./data"),
    log_path("./log"),
    daemonize(false) ,
    pid_file(kZpPidFile) ,
    lock_file(kZpLockFile) {
}


ZPOptions::ZPOptions(const ZPOptions& options)
  : meta_addr(options.meta_addr),
    seed_ip(options.seed_ip),
    seed_port(options.seed_port),
    local_ip(options.local_ip),
    local_port(options.local_port),
    data_path(options.data_path),
    log_path(options.log_path),
    daemonize(options.daemonize) ,
    pid_file(options.pid_file) ,
    lock_file(options.lock_file) {
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
  fprintf (stderr, "    Options.daemonize    : %s\n", daemonize? "true":"false");
  fprintf (stderr, "    Options.pid_file    : %s\n", pid_file.c_str());
  fprintf (stderr, "    Options.lock_file    : %s\n", lock_file.c_str());
}



int ZPOptions::Load(const std::string& path) {
  slash::BaseConf conf_reader(path);
  int res = conf_reader.LoadConf();
  if (res != 0) {
    return res;
  }

  bool ret = false;
  READCONF(conf_reader, seed_ip, seed_ip, STR);
  READCONF(conf_reader, seed_port, seed_port, INT);
  READCONF(conf_reader, local_ip, local_ip, STR);
  READCONF(conf_reader, local_port, local_port, INT);
  READCONF(conf_reader, data_path, data_path, STR);
  READCONF(conf_reader, log_path, log_path, STR);
  READCONF(conf_reader, daemonize, daemonize, BOOL);
  READCONF(conf_reader, pid_file, pid_file, STR);
  READCONF(conf_reader, lock_file, lock_file, STR);

  std::string meta_addr_str;
  READCONF(conf_reader, meta_addr, meta_addr_str, STR);
  std::string::size_type pos;
  while(true) {
    pos = meta_addr_str.find(",");
    if (pos == std::string::npos) {
      meta_addr.push_back(meta_addr_str);
      break;
    }
    meta_addr.push_back(meta_addr_str.substr(0, pos));
    meta_addr_str = meta_addr_str.substr(pos+1);
  }
  return res;
}
