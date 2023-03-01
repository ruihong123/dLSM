//
// Created by ruihong on 9/30/21.
//
#include <stdlib.h>
#include <string>
#include <iostream>
#include "include/TimberSaw/db.h"
#include "include/TimberSaw/filter_policy.h"
#include "TimberSaw/comparator.h"
int main()
{
  TimberSaw::DB* db;
  TimberSaw::Options options;
  options.max_background_compactions = 1;
  options.max_background_flushes = 1;
  options.comparator = TimberSaw::BytewiseComparator();
  //TODO: implement the FIlter policy before initial the database
  auto b_policy = TimberSaw::NewBloomFilterPolicy(options.bloom_bits);
  options.filter_policy = b_policy;
  TimberSaw::Status s = TimberSaw::DB::Open(options, "mem_leak", &db);
  delete db;
//  DestroyDB("mem_leak", TimberSaw::Options());
  TimberSaw::DB::Open(options, "mem_leak", &db);
  std::string value;
  std::string key;
  auto option_wr = TimberSaw::WriteOptions();
  for (int i = 0; i<1000000; i++){
    key = std::to_string(i);
    key.insert(0, 20 - key.length(), '1');
    value = std::to_string(std::rand() % ( 10000000 ));
    value.insert(0, 400 - value.length(), '1');
    s = db->Put(option_wr, key, value);
    if (!s.ok()){
      std::cerr << s.ToString() << std::endl;
    }

    //     std::cout << "iteration number " << i << std::endl;
  }

  delete db;
  delete b_policy;
}