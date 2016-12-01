/*
 * "Copyright [2016] <hrxwwd@163.com>"
 */
#include <string>
#include <vector>
#include <iostream>
#include <algorithm>

#include "linenoise/linenoise.h"

#include "include/zp_cluster.h"

void usage() {
  std::cout << "usage:\n"
            << "      zp_cli host port\n";
}

// completion example
void completion(const char *buf, linenoiseCompletions *lc) {
  if (buf[0] == 's') {
    linenoiseAddCompletion(lc, "s");
    linenoiseAddCompletion(lc, "set");
  } else if (buf[0] == 'c') {
    linenoiseAddCompletion(lc, "c");
    linenoiseAddCompletion(lc, "create");
  } else if (buf[0] == 'g') {
    linenoiseAddCompletion(lc, "g");
    linenoiseAddCompletion(lc, "get");
  }
}

// hints example
char *hints(const char *buf, int *color, int *bold) {
  if (!strcasecmp(buf, "create")) {
    *color = 35;
    *bold = 0;
    return " TABLE PARTITION";
  }
  if (!strcasecmp(buf, "set")) {
    *color = 35;
    *bold = 0;
    return " TABLE KEY VALUE";
  }
  if (!strcasecmp(buf, "get")) {
    *color = 35;
    *bold = 0;
    return " TABLE KEY VALUE";
  }
  return NULL;
}

void SplitByBlank(std::string& line, std::vector<std::string>& line_args) {
  line += " ";
  std::string unparse = line;
  std::string::size_type pos_start;
  std::string::size_type pos_end;
  pos_start = unparse.find_first_not_of(" ");
  while (pos_start != std::string::npos) {
    pos_end = unparse.find_first_of(" ", pos_start);
    line_args.push_back(unparse.substr(pos_start, pos_end - pos_start));
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
  while ((line = linenoise("zp >> ")) != NULL) {
    linenoiseHistoryAdd(line); /* Add to the history. */
    linenoiseHistorySave("history.txt"); /* Save the history on disk. */
    /* Do something with the string. */
    std::string info = line;
    std::vector<std::string> line_args;
    SplitByBlank(info, line_args);

    if (!strncmp(line, "create ", 7)) {
      std::string table_name = line_args[1];
      int partition_num = atoi(line_args[2].c_str());
      s = cluster.CreateTable(table_name, partition_num);
      std::cout << s.ToString() << std::endl;
      std::cout << "repull table "<< table_name << std::endl;
      s = cluster.Pull(table_name);
    } else if (!strncmp(line, "pull ", 5)) {
      if (line_args.size() != 2) {
        std::cout << "arg num wrong" << std::endl;
        continue;
      }
      std::string table_name = line_args[1];
      s = cluster.Pull(table_name);
      std::cout << s.ToString() << std::endl;
      std::cout << "dump info:" << std::endl;
      cluster.DumpTable();
    } else if (!strncmp(line, "set ", 4)) {
      if (line_args.size() != 4) {
        std::cout << "arg num wrong" << std::endl;
        continue;
      }
      std::string table_name = line_args[1];
      std::string key = line_args[2];
      std::string value = line_args[3];
      s = cluster.Set(table_name, key, value);
      std::cout << s.ToString() << std::endl;
    } else if (!strncmp(line, "get ", 4)) {
      if (line_args.size() != 3) {
        std::cout << "arg num wrong" << std::endl;
        continue;
      }
      std::string table_name = line_args[1];
      std::string key = line_args[2];
      std::string value;
      s = cluster.Get(table_name, key, value);
      if (s.ok()) {
        std::cout << value << std::endl;
      } else {
        std::cout << s.ToString() << std::endl;
      }
    } else {
      printf("Unreconized command: %s\n", line);
    }
    free(line);
  }
  std::cout << "out of loop" << std::endl;
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
