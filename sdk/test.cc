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
  
  printf ("\n=====Test Set many time==========\n");
  for (int i = 0; i < 100000; i++) {
    std::string nkey = key + std::to_string(i);
    result = cluster.Set(nkey, value);
    if (result.ok()) {
      printf ("Set(%s) ok\n", nkey.c_str());
    } else {
      printf ("Set(%s) failed, %s\n", result.ToString().c_str(), nkey.c_str());
    }

    //sleep(0.1);
  }

  for (int i = 0; i < 10; i++) {
    printf ("\n=====Test Get i=%d =========\n", i);
    value = "";
    result = cluster.Get(key, &value);
    if (result.ok()) {
      printf ("Get ok, value is %s\n", value.c_str());
    } else {
      printf ("Get failed, %s\n", result.ToString().c_str());
    }

    sleep(1);
  }

  for (int i = 0; i < 5; i++ ) {
    printf ("=====Test Get non-exist i=%d =========\n", i);
    value = "";
    result = cluster.Get("not_exist", &value);
    if (result.ok()) {
      printf ("Get non-exist ok, value is %s\n", value.c_str());
    } else {
      printf ("Get non-exist failed, %s\n", result.ToString().c_str());
    }
    sleep(1);
  }

  cout << "success" << endl;
  return 0;
}
