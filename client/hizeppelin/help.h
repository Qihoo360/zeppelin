struct CommandHelp {
  char *name;
  char *params;
  int params_num;
  char *summary;
} commandHelp[] = {
  { "SET",
  "table key value [ttl]",
  3,
  "set key"},

  { "GET",
  "table key",
  2,
  "get key's value"},

  { "CREATE",
  "table partition",
  2,
  "create table"},

  { "PULL",
  "table",
  1,
  "pull table info"},

  { "SETMASTER",
  "table partit p",
  4,
  "set a partition's master"},

  { "ADDSLAVE",
  "table partition ip port",
  4,
  "add master for partition"},

  { "REMOVESLAVE",
  "table partition ip port",
  4,
  "remove master for partition"},

  { "LISTTABLE",
  "",
  0,
  "list all tables"},

  { "DROPTABLE",
  "table",
  1,
  "drop one table"},

  { "LISTNODE",
  "",
  0,
  "list all data nodes"},

  { "LISTMETA",
  "",
  0,
  "list all meta nodes"},

  { "QPS",
  "table",
  1,
  "get qps for a table"},

  { "OFFSET",
  "table ip port",
  1,
  "get a node's binlog offset"},

  { "SPACE",
  "table",
  1,
  "get space info for a table"},

  { "DUMP",
  "table",
  1,
  "get space info for a table"},

  { "LOCATE",
  "table key",
  2,
  "locate a key, find corresponding nodes"}
};
