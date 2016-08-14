#include <stdio.h>
#include <iostream>
#include <string>
#include <sys/time.h>

#include "client.h"

using namespace std;

using slash::Status;

//struct option const long_options[] = {
//  {"servers", required_argument, NULL, 's'},
//  {NULL, 0, NULL, 0}, };

//const char* short_options = "s:";

uint64_t NowMicros() {
  struct timeval tv;
  gettimeofday(&tv, NULL);
  return static_cast<uint64_t>(tv.tv_sec) * 1000000 + tv.tv_usec;
}

int main(int argc, char* argv[]) {
  client::Option option;

  option.ParseFromArgs(argc, argv);

  client::Cluster cluster(option);

  Status result;

  printf ("\n=====Test Set==========\n");

  std::string key = "test_key";
  std::string value = "test_value1";
  
  printf ("\n=====Test Set many time==========\n");
  int64_t st = NowMicros();
  for (int i = 0; i < 200000; i++) {
    std::string nkey = key + std::to_string(i);
    result = cluster.Set(nkey, value);
    if (result.ok()) {
      //printf ("Set(%s) ok\n", nkey.c_str());
    } else {
      printf ("Set(%s) failed, %s\n", result.ToString().c_str(), nkey.c_str());
    }

    //sleep(0.1);
  }
  int64_t ed = NowMicros();

  printf ("time: %ld\n", ed - st);

//  for (int i = 0; i < 100; i++) {
//    printf ("\n=====Test Get i=%d =========\n", i);
//    std::string nkey = key + std::to_string(i);
//    value = "";
//    result = cluster.Get(nkey, &value);
//    if (result.ok()) {
//      printf ("Get(%s) ok, value is %s\n", nkey.c_str(), value.c_str());
//    } else {
//      printf ("Get(%s) failed, %s\n", nkey.c_str(), result.ToString().c_str());
//    }
//  
//  }

  //for (int i = 0; i < 5; i++) {
  //  printf ("=====Test Get non-exist i=%d =========\n", i);
  //  value = "";
  //  result = cluster.Get("not_exist", &value);
  //  if (result.ok()) {
  //    printf ("Get non-exist ok, value is %s\n", value.c_str());
  //  } else {
  //    printf ("Get non-exist failed, %s\n", result.ToString().c_str());
  //  }
  //  sleep(1);
  //}

  cout << "success" << endl;
  return 0;
}
