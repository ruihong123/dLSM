// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/table_cache.h"

#include "db/filename.h"
#include "table/table_memoryside.h"
#include <utility>

#include "TimberSaw/env.h"
#include "TimberSaw/table.h"

#include "util/coding.h"

namespace TimberSaw {
#ifdef PROCESSANALYSIS
std::atomic<uint64_t> TableCache::GetTimeElapseSum = 0;
std::atomic<uint64_t> TableCache::GetNum = 0;
std::atomic<uint64_t> TableCache::filtered = 0;
std::atomic<uint64_t> TableCache::not_filtered = 0;
std::atomic<uint64_t> TableCache::DataBinarySearchTimeElapseSum = 0;
std::atomic<uint64_t> TableCache::IndexBinarySearchTimeElapseSum = 0;
std::atomic<uint64_t> TableCache::DataBlockFetchBeforeCacheElapseSum = 0;
std::atomic<uint64_t> TableCache::foundNum = 0;

std::atomic<uint64_t> TableCache::cache_hit_look_up_time = 0;
std::atomic<uint64_t> TableCache::cache_miss_block_fetch_time = 0;
std::atomic<uint64_t> TableCache::cache_hit = 0;
std::atomic<uint64_t> TableCache::cache_miss = 0;
#endif
union SSTable {
//  RandomAccessFile* file;
//  std::weak_ptr<RemoteMemTableMetaData> remote_table;
  Table* table_compute;
  Table_Memory_Side* table_memory;
};

static void DeleteEntry_Compute(const Slice& key, void* value) {
  SSTable* tf = reinterpret_cast<SSTable*>(value);
  delete tf->table_compute;
//  delete tf->file;
  delete tf;
}
static void DeleteEntry_Memory(const Slice& key, void* value) {
  SSTable* tf = reinterpret_cast<SSTable*>(value);
  delete tf->table_memory;
  //  delete tf->file;
  delete tf;
}
static void UnrefEntry(void* arg1, void* arg2) {
  Cache* cache = reinterpret_cast<Cache*>(arg1);
  Cache::Handle* h = reinterpret_cast<Cache::Handle*>(arg2);
  cache->Release(h);
//  table_cache->Erase()
}

TableCache::TableCache(const std::string& dbname, const Options& options,
                       int entries)
    : env_(options.env),
      dbname_(dbname),
      options_(options),
      cache_(NewLRUCache(entries)) {}

TableCache::~TableCache() {
#ifdef PROCESSANALYSIS
  if (TableCache::GetNum.load() >0)
    printf("Cache Get time statics is %zu, %zu, %zu, need binary search: "
           "%zu, filtered %zu, foundNum is %zu\n",
           TableCache::GetTimeElapseSum.load(), TableCache::GetNum.load(),
           TableCache::GetTimeElapseSum.load()/TableCache::GetNum.load(),
           TableCache::not_filtered.load(), TableCache::filtered.load(),
           TableCache::foundNum.load());
  if (TableCache::not_filtered.load() > 0){
    printf("Average time elapse for Data binary search is %zu, "
           "Average time elapse for Index binary search is %zu,"
           " Average time elapse for data block fetch before table_cache is %zu\n",
           TableCache::DataBinarySearchTimeElapseSum.load()/TableCache::not_filtered.load(),
           TableCache::IndexBinarySearchTimeElapseSum.load()/TableCache::not_filtered.load(),
           TableCache::DataBlockFetchBeforeCacheElapseSum.load()/TableCache::not_filtered.load());
  }
  if (TableCache::cache_miss>0&&TableCache::cache_hit>0){
    printf("Cache hit Num %zu, average look up time %zu, Cache miss %zu, average Block Fetch time %zu\n",
           TableCache::cache_hit.load(),TableCache::cache_hit_look_up_time.load()/TableCache::cache_hit.load(),
           TableCache::cache_miss.load(),TableCache::cache_miss_block_fetch_time.load()/TableCache::cache_miss.load());
  }
#endif
  printf("Total number of entries within the cahce is %zu", cache_->TotalCharge());
  delete cache_;
}

Status TableCache::FindTable(
    const std::shared_ptr<RemoteMemTableMetaData>& Remote_memtable_meta,
    Cache::Handle** handle) {
  Status s = Status::OK();
//  char buf[sizeof(Remote_memtable_meta->number) + sizeof(Remote_memtable_meta->creator_node_id)];
//  EncodeFixed64(buf, Remote_memtable_meta->number);
//  memcpy(buf + sizeof(Remote_memtable_meta->number), &Remote_memtable_meta->creator_node_id,
//         sizeof(Remote_memtable_meta->creator_node_id));
//  char buf[sizeof(Remote_memtable_meta->number)];
//  EncodeFixed64(buf, Remote_memtable_meta->number);
//  Slice key(buf, sizeof(buf));
//  char buf[sizeof(Remote_memtable_meta->number)];
//  EncodeFixed64(buf, Remote_memtable_meta->number);
  Slice key((char*)&Remote_memtable_meta->number, sizeof(uint64_t));


  *handle = cache_->Lookup(key);
  if (*handle == nullptr) {
    // TODO: implement a hash lock to reduce the contention here, otherwise multiple
    // readers may get the same table and RDMA read the index block several times.
    uint64_t hash_value = Remote_memtable_meta->number%32;
    hash_mtx[hash_value].lock();
    *handle = cache_->Lookup(key);
    if (*handle == nullptr) {
      Table* table = nullptr;
//          printf("Did not find the table in the table_cache, file number is %lu \n ", Remote_memtable_meta->number);
      if (s.ok()) {
        s = Table::Open(options_, &table, Remote_memtable_meta);
      }
      //TODO(ruihong): add remotememtablemeta and Table to the table_cache entry.
      if (!s.ok()) {
        assert(table == nullptr);
        //      delete file;
        // We do not table_cache error results so that if the error is transient,
        // or somebody repairs the file, we recover automatically.
      } else {
        assert(table!= nullptr);
        SSTable* tf = new SSTable;
        //      tf->file = file;
        //      tf->remote_table = Remote_memtable_meta;
        tf->table_compute = table;
        assert(table->rep != nullptr);
        *handle = cache_->Insert(key, tf, 1, &DeleteEntry_Compute);
      }
    }
    hash_mtx[hash_value].unlock();
  }

  return s;
}
Status TableCache::FindTable_MemorySide(
    const std::shared_ptr<RemoteMemTableMetaData>& Remote_memtable_meta,
    Table_Memory_Side*& table) {
{
  Status s;
  s = Table_Memory_Side::Open(options_, &table, Remote_memtable_meta);
//      DEBUG_arg("file number inserted to the table_cache is %lu ", Remote_memtable_meta.get()->number);
//      DEBUG_arg("Remote_memtable_meta pointer is %p\n", Remote_memtable_meta.get());
  return s;
}
//Status TableCache::FindTable_MemorySide(std::shared_ptr<RemoteMemTableMetaData> Remote_memtable_meta, Cache::Handle** handle){
//  {
//    Status s;
//    char buf[sizeof(Remote_memtable_meta->number) + sizeof(Remote_memtable_meta->creator_node_id)];
//    EncodeFixed64(buf, Remote_memtable_meta->number);
//    memcpy(buf + sizeof(Remote_memtable_meta->number), &Remote_memtable_meta->creator_node_id,
//           sizeof(Remote_memtable_meta->creator_node_id));
//    Slice key(buf, sizeof(buf));
//    *handle = table_cache->Lookup(key);
//    if (*handle == nullptr) {
//      Table_Memory_Side* table = nullptr;
//      //    DEBUG("FindTable_MemorySide\n");
//      if (s.ok()) {
//        s = Table_Memory_Side::Open(options_, &table, Remote_memtable_meta);
//        //      DEBUG_arg("file number inserted to the table_cache is %lu ", Remote_memtable_meta.get()->number);
//        //      DEBUG_arg("Remote_memtable_meta pointer is %p\n", Remote_memtable_meta.get());
//      }
//      //TODO(ruihong): add remotememtablemeta and Table to the table_cache entry.
//      if (!s.ok()) {
//        assert(table == nullptr);
//        //      delete file;
//        // We do not table_cache error results so that if the error is transient,
//        // or somebody repairs the file, we recover automatically.
//      } else {
//        SSTable* tf = new SSTable;
//        //      tf->file = file;
//        //      tf->remote_table = Remote_memtable_meta;
//        assert(table->Get_remote_table_ptr() != nullptr);
//        tf->table_memory = table;
//        assert(table->rep != nullptr);
//        //      assert(static_cast<RemoteMemTableMetaData*>(table->Get_remote_table_ptr())->number != 0);
//        *handle = table_cache->Insert(key, tf, 1, &DeleteEntry_Memory);
//      }
//    }
//    return s;
//  }


}
Iterator* TableCache::NewIterator(
    const ReadOptions& options,
    std::shared_ptr<RemoteMemTableMetaData> remote_table, Table** tableptr) {
  if (tableptr != nullptr) {
    *tableptr = nullptr;
  }

  Cache::Handle* handle = nullptr;
  Status s = FindTable(std::move(remote_table), &handle);
  if (!s.ok()) {
    return NewErrorIterator(s);
  }

  Table* table = reinterpret_cast<SSTable*>(cache_->Value(handle))->table_compute;
#ifndef BYTEADDRESSABLE
  Iterator* result = table->NewIterator(options);
#endif
#ifdef BYTEADDRESSABLE
  Iterator* result = table->NewSEQIterator(options);
#endif
  result->RegisterCleanup(&UnrefEntry, cache_, handle);
  if (tableptr != nullptr) {
    *tableptr = table;
  }
  return result;
}
#ifdef BYTEADDRESSABLE
Iterator* TableCache::NewSEQIterator(
    const ReadOptions& options,
    std::shared_ptr<RemoteMemTableMetaData> remote_table, Table** tableptr) {
  if (tableptr != nullptr) {
    *tableptr = nullptr;
  }

  Cache::Handle* handle = nullptr;
  Status s = FindTable(std::move(remote_table), &handle);
  if (!s.ok()) {
    return NewErrorIterator(s);
  }

  Table* table = reinterpret_cast<SSTable*>(cache_->Value(handle))->table_compute;
  Iterator* result = table->NewSEQIterator(options);
  result->RegisterCleanup(&UnrefEntry, cache_, handle);
  if (tableptr != nullptr) {
    *tableptr = table;
  }
  return result;
}
#endif
Iterator* TableCache::NewIterator_MemorySide(
    const ReadOptions& options,
    const std::shared_ptr<RemoteMemTableMetaData>& remote_table,
    Table_Memory_Side** tableptr) {
  if (tableptr != nullptr) {
    *tableptr = nullptr;
  }

  Cache::Handle* handle = nullptr;
//#ifndef NDEBUG
//  void* p = remote_table.get();
//
//  if (reinterpret_cast<long>(p) == 0x7fff94151070)
//    printf("check for NewIterator_MemorySide\n");
//#endif
  // TODO: Shall we garbage collect the Table_Memory_Side since we will not put it
  // into cache?
  Table_Memory_Side* table;
  Status s = FindTable_MemorySide(remote_table, table);
  if (!s.ok()) {
    return NewErrorIterator(s);
  }

//  Table_Memory_Side* table = reinterpret_cast<SSTable*>(table_cache->Value(handle))->table_memory;
  // The pointer of p will be different from what stored in the table_cache because the p is a new
  // created Remote memory metadata from the serialized buffer. so no need for the
  // assert of p == table->Get_rdma()
  assert(table->Get_remote_table_ptr()!= nullptr);
  Iterator* result = table->NewIterator(options);
//  result->RegisterCleanup(&UnrefEntry, table_cache, handle);
  if (tableptr != nullptr) {
    *tableptr = table;
  }
  return result;
}
Status TableCache::Get(const ReadOptions& options,
                       std::shared_ptr<RemoteMemTableMetaData> f,
                       const Slice& k, void* arg,
                       void (*handle_result)(void*, const Slice&,
                                             const Slice&)) {
#ifdef PROCESSANALYSIS
  auto start = std::chrono::high_resolution_clock::now();
#endif
  Cache::Handle* handle = nullptr;
  //TODO: not let concurrent thread finding the same tale and inserting the same
  // index block to the table_cache
  Status s = FindTable(f, &handle);
  if (s.ok()) {
    Table* t = reinterpret_cast<SSTable*>(cache_->Value(handle))->table_compute;
    s = t->InternalGet(options, k, arg, handle_result);
    //if you want to bypass the lock in cache then commet the code below
    cache_->Release(handle);
  }
#ifdef PROCESSANALYSIS
  auto stop = std::chrono::high_resolution_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
//    std::printf("Get from SSTables (not found) time elapse is %zu\n",  duration.count());
  TableCache::GetTimeElapseSum.fetch_add(duration.count());
  TableCache::GetNum.fetch_add(1);
#endif
  return s;
}

void TableCache::Evict(uint64_t file_number, uint8_t creator_node_id) {
//  char buf[sizeof(uint64_t) + sizeof(uint8_t)];
char buf[sizeof(file_number)];
  EncodeFixed64(buf, file_number);
//  memcpy(buf + sizeof(uint64_t), &creator_node_id,
//         sizeof(uint8_t));
  cache_->Erase(Slice(buf, sizeof(buf)));
}

}  // namespace TimberSaw
