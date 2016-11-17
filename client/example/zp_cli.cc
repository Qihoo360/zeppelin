#include <string>
#include <strings.h>
#include <vector>
#include <iostream>
#include <algorithm>
#include <arpa/inet.h>

#include "zp_cluster_client.h"
#include "string_utils.h"

void usage() {
  fprintf(stderr, "usage:\n"
                  "      zp_cli host port\n");
}


std::string DumpPullResp(::ZPMeta::MetaCmdResponse_Pull& pull_resp) {
	std::string res;
	::ZPMeta::Node node;
	::Node *local_master = NULL, *local_slave = NULL;
	std::string s_ipport;
	
	int32_t total_partition = pull_resp.info_size();
	res.append(std::string("total partition: ") + std::to_string(total_partition) + "\n");
	for (int index = 0; index != total_partition; ++index) {
		const ::ZPMeta::Partitions& p = pull_resp.info(index);
		res.append(std::string("\npartition ") + std::to_string(index) + ":\n" );

		// master part
		node = p.master();
		s_ipport = node.ip() + ":" + std::to_string(node.port());
		res.append(std::string("master: ") + s_ipport + "\n");

		// slave part
		for (int idx = 0; idx != p.slaves_size(); ++idx) {
			node = p.slaves(idx);
			s_ipport = node.ip() + ":" + std::to_string(node.port());
			res.append(std::string("slave") + std::to_string(idx) + ": " + s_ipport + "\n");
		}
	}
	return res;
}

int main(int argc, char* argv[]) {
  if (argc != 3) {
    usage();
    return -1;
  }
	std::vector<IpPort> hosts;
  std::string host = argv[1];
  int32_t port = atoi(argv[2]);
	hosts.push_back({host, port});
  ZPClusterClient zp_client(hosts);
  std::string prompt, result, line;
  std::vector<std::string> args_v;
  Status s;
  while (true)
  {
    prompt = host + ":" + std::to_string(port) + " >";
    prompt = s.msg() + " >"; 
    s.SetOk();
    while (s.ok()) {
      std::cout << prompt;
      std::getline(std::cin, line);
      if (line.empty()) {
        continue;
      }
      args_v.clear();
      utils::GetSeparateArgs(line, &args_v);
      std::transform(args_v[0].begin(), args_v[0].end(), args_v[0].begin(), ::tolower);
			if (args_v[0] == "pull") {
				if (args_v.size() != 1) {
					result = "Wrong argument number";
				}	else {
					::ZPMeta::MetaCmdResponse_Pull pull_resp;
					s = zp_client.Pull(&pull_resp);
					if (s.ok()) {
						result = DumpPullResp(pull_resp);
					} else {
						result = s.msg();						
					}
				}
			} else if (args_v[0] == "init") {
				if (args_v.size() != 2) {
					result = "wrong argument number";
				}	else {
					s = zp_client.Init(atoi(args_v[1].c_str()));
					result = s.msg();
				}
      } else if (args_v[0] == "set") {
				if (args_v.size() != 3) {
					result = "wrong argument number";
				} else {
					s = zp_client.Set(args_v[1], args_v[2]);
					result = s.msg();
				}
			} else if (args_v[0] == "get") {
				if (args_v.size() != 2) {
					result = "wrong argument number";
				} else {
					s = zp_client.Get(args_v[1]);
					result = s.ok() ? s.value() : s.msg();
				}
			} else {
        result = "Unkown command";
      }
      std::cout << result << std::endl;
    }
  }
}
