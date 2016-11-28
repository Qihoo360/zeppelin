/*
 * "Copyright [2016] <hrxwwd@163.com>"
 */
#include <string>
#include <vector>
#include <iostream>
#include <algorithm>

#include "linenoise.h"

#include "include/zp_cluster.h"

void usage() {
  std::cout << "usage:\n"
            << "      zp_cli host port\n";
}

//completion example
void completion(const char *buf, linenoiseCompletions *lc) {
  if (buf[0] == 's') {
    linenoiseAddCompletion(lc,"set");
  } else if (buf[0] == 'c') {
    linenoiseAddCompletion(lc,"create");
  }

}

//hints example
char *hints(const char *buf, int *color, int *bold) {
  if (!strcasecmp(buf,"create")) {
    *color = 35;
    *bold = 0;
    return " TABLE PARTITION";
  }
  if (!strcasecmp(buf,"set")) {
    *color = 35;
    *bold = 0;
    return " TABLE KEY VALUE";
  }
  return NULL;
}
void SplitStr(std::string& line, std::vector<std::string>& line_args) {
  line += " ";
  std::string unparse = line;
  std::string::size_type pos_start;
  std::string::size_type pos_end;
  pos_start = unparse.find_first_not_of(" ");
  while(pos_start != std::string::npos) {
    pos_end = unparse.find_first_of(" ", pos_start);
    line_args.push_back(unparse.substr(pos_start, pos_end));
    unparse = unparse.substr(pos_end);
    pos_start = unparse.find_first_not_of(" ");
  }

}
void StartRepl(libZp::Cluster& cluster) {
  char *line;
  linenoiseSetMultiLine(1);
  linenoiseSetCompletionCallback(completion);
  linenoiseSetHintsCallback(hints);
  linenoiseHistoryLoad("history.txt"); /* Load the history at startup */

  libZp::Status s;
  while((line = linenoise("zp >> ")) != NULL) {
    /* Do something with the string. */
    std::string info = line;
    std::vector<std::string> line_args;
    SplitStr(info, line_args);

    if (!strncmp(line,"create",6)) {
      linenoiseHistoryAdd(line); /* Add to the history. */
      linenoiseHistorySave("history.txt"); /* Save the history on disk. */

      std::string table_name = line_args[1];
      int partition_num = atoi(line_args[2].c_str());
      s = cluster.CreateTable(table_name, partition_num);
      if (!s.ok()) {
        std::cout << s.ToString() << std::endl;
      }
    } else if (!strncmp(line,"pull",4)) {
      linenoiseHistoryAdd(line); /* Add to the history. */
      linenoiseHistorySave("history.txt"); /* Save the history on disk. */
      //s = cluster.Pull();
    } else {
      printf("Unreconized command: %s\n", line);
    }
    free(line);
  }
}

int main(int argc, char* argv[]) {
  if (argc != 3) {
    usage();
    return -1;
  }
  std::cout << "start" << std::endl;
  libZp::Options option;
  libZp::IpPort ipPort = libZp::IpPort(argv[1], atoi(argv[2]));
  option.meta_addr.push_back(ipPort);

  // cluster handle cluster operation
  std::cout << "create cluster" << std::endl;
  libZp::Cluster cluster = libZp::Cluster(option);
  std::cout << "connect cluster" << std::endl;
  // needs connect to cluster first
  libZp::Status s = cluster.Connect();
  if (!s.ok()) {
    std::cout << s.ToString() << std::endl;
    exit(-1);
  }

  StartRepl(cluster);
  /*
  Status s = cluster.ListMetaNode(node_list);
  node_list.clear();
  s = cluster.ListDataNode(node_list);
  // ioctx handle table operation and set/get
  libZp::Ioctx ioctx = cluster.CreateIoctx("test_pool");
  std::vector<std::pair<std::string, std::int>> node_list;
  s = ioctx.set("key","value");
  std::string val;
  s = ioctx.get("key",val);
  */
}
