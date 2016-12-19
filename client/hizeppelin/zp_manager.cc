/*
 * "Copyright [2016] qihoo"
 * "Author <hrxwwd@163.com>"
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
    linenoiseAddCompletion(lc, "set");
  } else if (buf[0] == 'c') {
    linenoiseAddCompletion(lc, "create");
  } else if (buf[0] == 'g') {
    linenoiseAddCompletion(lc, "get");
  } else if (buf[0] == 'p') {
    linenoiseAddCompletion(lc, "pull");
  } else if (buf[0] == 'l') {
    linenoiseAddCompletion(lc, "locate");
  } else if (buf[0] == 'd') {
    linenoiseAddCompletion(lc, "dump");
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
  if (!strcasecmp(buf, "pull")) {
    *color = 35;
    *bold = 0;
    return " TABLE";
  }
  if (!strcasecmp(buf, "locate")) {
    *color = 35;
    *bold = 0;
    return " TABLE KEY";
  }
  if (!strcasecmp(buf, "dump")) {
    *color = 35;
    *bold = 0;
    return " TABLE TABLE_NAME";
  }
  if (!strcasecmp(buf, "addslave")) {
    *color = 35;
    *bold = 0;
    return " TABLE PARTITON IP PORT";
  }
  if (!strcasecmp(buf, "removeslave")) {
    *color = 35;
    *bold = 0;
    return " TABLE PARTITON IP PORT";
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

void StartRepl(libzp::Cluster* cluster) {
  char *line;
  linenoiseSetMultiLine(1);
  linenoiseSetCompletionCallback(completion);
  linenoiseSetHintsCallback(hints);
  linenoiseHistoryLoad("history.txt"); /* Load the history at startup */

  libzp::Status s;
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
      s = cluster->CreateTable(table_name, partition_num);
      std::cout << s.ToString() << std::endl;
      std::cout << "repull table "<< table_name << std::endl;
      s = cluster->Pull(table_name);

    } else if (!strncmp(line, "pull ", 5)) {
      if (line_args.size() != 2) {
        std::cout << "arg num wrong" << std::endl;
        continue;
      }
      std::string table_name = line_args[1];
      s = cluster->Pull(table_name);
      std::cout << s.ToString() << std::endl;
      std::cout << "current table info:" << std::endl;
      cluster->DebugDumpTable(table_name);

    } else if (!strncmp(line, "dump table ", 11)) {
      if (line_args.size() != 3) {
        std::cout << "arg num wrong" << std::endl;
        continue;
      }
      std::string table_name = line_args[2];
      cluster->DebugDumpTable(table_name);

    } else if (!strncmp(line, "locate ", 5)) {
      if (line_args.size() != 3) {
        std::cout << "arg num wrong" << std::endl;
        continue;
      }
      std::string table_name = line_args[1];
      std::string key = line_args[2];
      libzp::Table::Partition* partition =
        cluster->GetPartition(table_name, key);
      if (partition) {
        std::cout << "partition_id: " << partition->id << std::endl;
        std::cout << "master: " << partition->master.ip << ":"
          << partition->master.port << std::endl;
        auto iter = partition->slaves.begin();
        while (iter != partition->slaves.end()) {
          std::cout << "slave: "<< iter->ip << ":"
            << iter->port << std::endl;
          iter++;
        }
      } else {
        std::cout << "doe not exist in local table" << std::endl;
      }

    } else if (!strncmp(line, "set ", 4)) {
      if (line_args.size() != 4) {
        std::cout << "arg num wrong" << std::endl;
        continue;
      }
      std::string table_name = line_args[1];
      std::string key = line_args[2];
      std::string value = line_args[3];
      s = cluster->Set(table_name, key, value);
      std::cout << s.ToString() << std::endl;

    } else if (!strncmp(line, "setmaster ", 10)) {
      if (line_args.size() != 5) {
        std::cout << "arg num wrong" << std::endl;
        continue;
      }
      std::string table_name = line_args[1];
      int partition = atoi(line_args[2].c_str());
      libzp::IpPort ip_port(line_args[3], atoi(line_args[4].c_str()));
      s = cluster->SetMaster(table_name, partition, ip_port);
      std::cout << s.ToString() << std::endl;

    } else if (!strncmp(line, "addslave ", 9)) {
      if (line_args.size() != 5) {
        std::cout << "arg num wrong" << std::endl;
        continue;
      }
      std::string table_name = line_args[1];
      int partition = atoi(line_args[2].c_str());
      libzp::IpPort ip_port(line_args[3], atoi(line_args[4].c_str()));
      s = cluster->AddSlave(table_name, partition, ip_port);
      std::cout << s.ToString() << std::endl;

    } else if (!strncmp(line, "removeslave ", 12)) {
      if (line_args.size() != 5) {
        std::cout << "arg num wrong" << std::endl;
        continue;
      }
      std::string table_name = line_args[1];
      int partition = atoi(line_args[2].c_str());
      libzp::IpPort ip_port(line_args[3], atoi(line_args[4].c_str()));
      s = cluster->RemoveSlave(table_name, partition, ip_port);
      std::cout << s.ToString() << std::endl;

    } else if (!strncmp(line, "get ", 4)) {
      if (line_args.size() != 3) {
        std::cout << "arg num wrong" << std::endl;
        continue;
      }
      std::string table_name = line_args[1];
      std::string key = line_args[2];
      std::string value;
      s = cluster->Get(table_name, key, &value);
      if (s.ok()) {
        std::cout << value << std::endl;
      } else {
        std::cout << s.ToString() << std::endl;
      }
    } else if (!strncmp(line, "listmeta", 8)) {
        if (line_args.size() != 1) {
          std::cout << "arg num wrong" << std::endl;
          continue;
        }
        std::vector<libzp::IpPort> nodes;
        s = cluster->ListMeta(nodes);
        std::vector<libzp::IpPort>::iterator iter = nodes.begin();
        while (iter != nodes.end()) {
          std::cout << iter->ip << ":" << iter->port << std::endl;
          iter++;
        }
        std::cout << s.ToString() << std::endl;

    } else if (!strncmp(line, "listnode", 8)) {
        if (line_args.size() != 1) {
          std::cout << "arg num wrong" << std::endl;
          continue;
        }
        std::vector<libzp::IpPort> nodes;
        s = cluster->ListNode(nodes);
        std::vector<libzp::IpPort>::iterator iter = nodes.begin();
        while (iter != nodes.end()) {
          std::cout << iter->ip << ":" << iter->port << std::endl;
          iter++;
        }
        std::cout << s.ToString() << std::endl;

    } else if (!strncmp(line, "listtable", 9)) {
        if (line_args.size() != 1) {
          std::cout << "arg num wrong" << std::endl;
          continue;
        }
        std::vector<std::string> tables;
        s = cluster->ListTable(tables);
        std::vector<std::string>::iterator iter = tables.begin();
        while (iter != tables.end()) {
          std::cout << *iter << std::endl;
          iter++;
        }
        std::cout << s.ToString() << std::endl;

    } else {
      printf("Unrecognized command: %s\n", line);
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
  libzp::Options option;
  libzp::IpPort ip_port(argv[1], atoi(argv[2]));
  option.meta_addr.push_back(ip_port);

  // cluster handle cluster operation
  std::cout << "create cluster" << std::endl;
  libzp::Cluster* cluster = new libzp::Cluster(option);
  std::cout << "connect cluster" << std::endl;
  // needs connect to cluster first
  libzp::Status s = cluster->Connect();
  if (!s.ok()) {
    std::cout << s.ToString() << std::endl;
    exit(-1);
  }

  StartRepl(cluster);
  /*
  Status s = cluster.ListMetaNode(node_list);
  node_list.clear();
  s = cluster.ListDataNode(node_list);
  */
}
