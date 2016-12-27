struct CommandHelp {
  char *name;
  char *params;
  int params_num;
  char *summary;
} commandHelp[] = {
  { "SET",
  "table key value",
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
  "tab par ip port",
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

  { "LISTNODE",
  "",
  0,
  "list all data nodes"},

  { "LISTMETA",
  "",
  0,
  "list all meta nodes"},

  { "LOCATE",
  "table key",
  2,
  "locate a key, find corresponding nodes"}
};
