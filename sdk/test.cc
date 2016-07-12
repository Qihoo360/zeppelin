#include <stdio.h>
#include <iostream>
#include <string>

#include "client.h"

using namespace std;

using slash::Status;

//struct option const long_options[] = {
//  {"servers", required_argument, NULL, 's'},
//  {NULL, 0, NULL, 0}, };

//const char* short_options = "s:";

int main(int argc, char* argv[]) {
  client::Option option;

  option.ParseFromArgs(argc, argv);

  client::Cluster cluster(option);

  Status result;

  printf ("\n=====Test Set==========\n");

  std::string key = "test_key";
  std::string value = "test_value1";
  
 // result = cluster.Set(key, value);
 // if (result.ok()) {
 //   printf ("Set ok\n");
 // } else {
 //   printf ("Set failed, %s\n", result.ToString().c_str());
 // }

  printf ("\n=====Test Get==========\n");

  value = "";
  result = cluster.Get(key, &value);
  if (result.ok()) {
    printf ("Get ok, value is %s\n", value.c_str());
  } else {
    printf ("Get failed, %s\n", result.ToString().c_str());
  }

  cout << "success" << endl;
  return 0;
}
