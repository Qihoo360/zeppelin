## Description

#### check_binlog_hole
In some situation, such as the binlog lost, the binlog file number may be not continuous.
**check_binlog_hole** will check and list the error partition binlog path.

Usage:
./check_lost log_path


#### empty_trash
For safty consideration, DB or Binlog will not be actually deleted, but be move to trash when needed, this tools is to delete them permanentlly.

Usage:
./empty_trash trash_path table


#### dump_meta
Dump all meta information out from meta server db

Usage:
./dump_meta path_to_RocksDB        --- do not print detail
./dump_meta path_to_RocksDB detail --- print detail table_info

