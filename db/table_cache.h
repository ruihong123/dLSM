// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Thread-safe (provides internal synchronization)

#ifndef STORAGE_TimberSaw_DB_TABLE_CACHE_H_
#define STORAGE_TimberSaw_DB_TABLE_CACHE_H_

#include "db/dbformat.h"
#include "db/version_edit.h"
#include <cstdint>
#include <string>
//#include <table/table_memoryside.h>

#include "TimberSaw/cache.h"
#include "TimberSaw/table.h"

#include "port/port.h"

namespace TimberSaw {

class Env;
class Table_Memory_Side;
class TableCache {
 public:
  TableCache(const std::string& dbname, const Options& options, size_t entries);
  ~TableCache();
#ifdef PROCESSANALYSIS
  static std::atomic<uint64_t> GetTimeElapseSum;
  static std::atomic<uint64_t> GetNum;
  static std::atomic<uint64_t> not_filtered;
  static std::atomic<uint64_t> DataBinarySearchTimeElapseSum;
  static std::atomic<uint64_t> IndexBinarySearchTimeElapseSum;
  static std::atomic<uint64_t> DataBlockFetchBeforeCacheElapseSum;
  static std::atomic<uint64_t> filtered;
  static std::atomic<uint64_t> foundNum;

  static std::atomic<uint64_t> cache_hit_look_up_time;
  static std::atomic<uint64_t> cache_miss_block_fetch_time;
  static std::atomic<uint64_t> cache_hit;
  static std::atomic<uint64_t> cache_miss;

#endif
  // Return an iterator for the specified file number (the corresponding
  // file length must be exactly "file_size" bytes).  If "tableptr" is
  // non-null, also sets "*tableptr" to point to the Table object
  // underlying the returned iterator, or to nullptr if no Table object
  // underlies the returned iterator.  The returned "*tableptr" object is owned
  // by the table_cache and should not be deleted, and is valid for as long as the
  // returned iterator is live.
  Iterator* NewIterator(const ReadOptions& options,
                        const std::shared_ptr<RemoteMemTableMetaData>& remote_table,
                        Table** tableptr = nullptr);
//#ifdef BYTEADDRESSABLE
//  Iterator* NewSEQIterator(const ReadOptions& options,
//                        std::shared_ptr<RemoteMemTableMetaData> remote_table,
//                        Table** tableptr = nullptr);
//#endif
  Iterator* NewIterator_MemorySide(const ReadOptions& options,
                        const std::shared_ptr<RemoteMemTableMetaData>& remote_table,
      Table_Memory_Side** tableptr = nullptr);
#ifdef PROCESSANALYSIS
  static void CleanAll(){
    GetTimeElapseSum = 0;
    GetNum = 0;
    filtered = 0;
    not_filtered = 0;
    DataBinarySearchTimeElapseSum = 0;
    IndexBinarySearchTimeElapseSum = 0;
    DataBlockFetchBeforeCacheElapseSum = 0;
    foundNum = 0;

    cache_hit_look_up_time = 0;
    cache_miss_block_fetch_time = 0;
    cache_hit = 0;
    cache_miss = 0;
    RDMA_Manager::ReadCount = 0;
    RDMA_Manager::RDMAReadTimeElapseSum = 0;
  }
#endif
  // If a seek to internal key "k" in specified file finds an entry,
  // call (*handle_result)(arg, found_key, found_value).
  Status Get(const ReadOptions& options,
             std::shared_ptr<RemoteMemTableMetaData> f, const Slice& k,
             void* arg,
             void (*handle_result)(void*, const Slice&, const Slice&));

  // Evict any entry for the specified file number
  void Evict(uint64_t file_number, uint8_t creator_node_id);
  double CheckUtilizaitonOfCache(){
#if TABLE_STRATEGY==2
    double util = static_cast<double>(cache_->TotalCharge())/ (static_cast<double>(cache_->GetCapacity())/TABLE_CACHE_SCALING_FACTOR);
#else
    double util = static_cast<double>(cache_->TotalCharge())/ (static_cast<double>(cache_->GetCapacity()));
#endif
    return util;
  }
  size_t CheckAvailableSpace(){
    size_t usage = (cache_->TotalCharge());
    size_t capacity = (cache_->GetCapacity());
    return capacity/TABLE_CACHE_SCALING_FACTOR > usage ? (capacity/TABLE_CACHE_SCALING_FACTOR - usage) : 0;
  }
  double CheckLeftSpacekilo(){
    double usage = static_cast<double>(cache_->TotalCharge());
    double capacity = static_cast<double>(cache_->GetCapacity())/TABLE_CACHE_SCALING_FACTOR;
    return (capacity - usage)/1024.0;
  }
 private:
  Status FindTable(const std::shared_ptr<RemoteMemTableMetaData>& Remote_memtable_meta,
                   Cache::Handle** handle);
  Status FindTable_MemorySide(
      const std::shared_ptr<RemoteMemTableMetaData>& Remote_memtable_meta,
      Table_Memory_Side*& table);
  Env* const env_;
  const std::string dbname_;
  const Options& options_;
  Cache* cache_;
  std::mutex hash_mtx[32];
};

}  // namespace TimberSaw

#endif  // STORAGE_TimberSaw_DB_TABLE_CACHE_H_
