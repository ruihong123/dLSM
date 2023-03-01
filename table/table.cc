// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "TimberSaw/table.h"

#include "db/table_cache.h"

#include "TimberSaw/cache.h"
#include "TimberSaw/comparator.h"
#include "TimberSaw/env.h"
#include "TimberSaw/filter_policy.h"
#include "TimberSaw/options.h"


#include "table/filter_block.h"
#include "table/format.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"

#include "full_filter_block.h"
#include "byte_addressable_RA_iterator.h"
#include "byte_addressable_SEQ_iterrator.h"

namespace TimberSaw {

//thread_local ibv_mr*  Table::Rep::mr_addr = nullptr;
//TODO: Make it compatible with multi-node setup.
Status Table::Open(const Options& options, Table** table,
                   const std::shared_ptr<RemoteMemTableMetaData>& Remote_table_meta) {
  *table = nullptr;


  // Read the index block
  Status s = Status::OK();
  BlockContents index_block_contents;
  ReadOptions opt;
  if (options.paranoid_checks) {
    opt.verify_checksums = true;
  }
  ibv_mr* remote_mr = Remote_table_meta->remote_dataindex_mrs.begin()->second;
  s = ReadDataIndexBlock(
      remote_mr, opt,
      &index_block_contents, Remote_table_meta->shard_target_node_id);

  if (s.ok()) {
    // We've successfully read the footer and the index block: we're
    // ready to serve requests.
    Block* index_block;
    if (remote_mr->length < INDEX_BLOCK_SMALL){
      index_block = new Block(index_block_contents, IndexBlock_Small);

    } else{
      index_block = new Block(index_block_contents, IndexBlock);

    }
    Rep* rep = new Table::Rep(options);
//    rep->options = options;
//    rep->file = file;
    rep->remote_table = Remote_table_meta;
//    rep->metaindex_handle = footer.metaindex_handle();
    rep->index_block = index_block;
    assert(rep->index_block->size() > 0);
    rep->cache_id = (options.block_cache ? options.block_cache->NewId() : 0);
//    rep->filter_data = nullptr;
    rep->filter = nullptr;

    *table = new Table(rep);
    (*table)->ReadFilter();
//    (*table)->ReadMeta(footer);
  }else{
    assert(false);
  }

  return s;
}

void Table::ReadFilter() {
  if (rep->options.filter_policy == nullptr) {
    return;  // Do not need any metadata
  }
  // We might want to unify with ReadDataBlock() if we start
  // requiring checksum verification in Table::Open.
  ReadOptions opt;
  if (rep->options.paranoid_checks) {
    opt.verify_checksums = true;
  }
  BlockContents block;
  auto table_meta_data = rep->remote_table.lock();
  if (!ReadFilterBlock(
           table_meta_data->remote_filter_mrs.begin()->second, opt,
           &block, table_meta_data->shard_target_node_id)
           .ok()) {
    return;
  }
//  if (block.heap_allocated) {
//    rep_->filter_data = block.data.data();  // Will need to delete later
//  }
  rep->filter = new FullFilterBlockReader(
      block.data, rep->remote_table.lock()->rdma_mg, Compute);
}

Table::~Table() {
//  printf("garbage collect the local cache of table %lu", rep->cache_id);
  delete rep;
}


static void DeleteBlock(void* arg, void* ignored) {
  delete reinterpret_cast<Block*>(arg);
}

static void DeleteCachedBlock(const Slice& key, void* value) {
  Block* block = reinterpret_cast<Block*>(value);
  delete block;
}
static void ReleaseBlock(void* arg, void* h) {
  Cache* cache = reinterpret_cast<Cache*>(arg);
  Cache::Handle* handle = reinterpret_cast<Cache::Handle*>(h);
  cache->Release(handle);
}


// Convert an index iterator value (i.e., an encoded BlockHandle)
// into an iterator over the contents of the corresponding block.
// Note the block reader no does not support muliti compute nodes.
Iterator* Table::BlockReader(void* arg, const ReadOptions& options,
                             const Slice& index_value) {
  Table* table = reinterpret_cast<Table*>(arg);
  Cache* block_cache = table->rep->options.block_cache;
  Block* block = nullptr;
  Cache::Handle* cache_handle = nullptr;

  BlockHandle handle;
  Slice input = index_value;
  Status s = handle.DecodeFrom(&input);
  // We intentionally allow extra stuff in index_value so that we
  // can add more features in the future.

  if (s.ok()) {
    BlockContents contents;
    if (block_cache != nullptr) {
//      printf("There is a table_cache!!\n");
      char cache_key_buffer[16];
      EncodeFixed64(cache_key_buffer, table->rep->cache_id);
      EncodeFixed64(cache_key_buffer + 8, handle.offset());
      Slice key(cache_key_buffer, sizeof(cache_key_buffer));
#ifdef PROCESSANALYSIS
      auto start = std::chrono::high_resolution_clock::now();
#endif
      cache_handle = block_cache->Lookup(key);
#ifdef PROCESSANALYSIS
      auto stop = std::chrono::high_resolution_clock::now();
      auto lookup_duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
#endif
      if (cache_handle != nullptr) {
        block = reinterpret_cast<Block*>(block_cache->Value(cache_handle));
//        printf("Cache hit\n");
#ifdef PROCESSANALYSIS
        TableCache::cache_hit.fetch_add(1);
        TableCache::cache_hit_look_up_time.fetch_add(lookup_duration.count());
#endif
      } else {
#ifdef PROCESSANALYSIS
        TableCache::cache_miss.fetch_add(1);
//        if(TableCache::cache_miss < TableCache::not_filtered){
//          printf("warning\n");
//        };
        start = std::chrono::high_resolution_clock::now();

#endif
        auto meta_ptr = table->rep->remote_table.lock();

        s = ReadDataBlock(&meta_ptr->remote_data_mrs,
                          options, handle, &contents, meta_ptr->shard_target_node_id);
#ifdef PROCESSANALYSIS
        stop = std::chrono::high_resolution_clock::now();
        auto blockfetch_duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
        TableCache::cache_miss_block_fetch_time.fetch_add(blockfetch_duration.count());
#endif

        if (s.ok()) {
          block = new Block(contents, DataBlock);
          if (options.fill_cache) {
            cache_handle = block_cache->Insert(key, block, block->size(),
                                               &DeleteCachedBlock);
          }
        }
      }
    } else {
//      printf("NO table_cache found!!\n");
      auto meta_ptr = table->rep->remote_table.lock();
      s = ReadDataBlock(&meta_ptr->remote_data_mrs,
                        options, handle, &contents, meta_ptr->shard_target_node_id);
      if (s.ok()) {
        block = new Block(contents, DataBlock);
      }
    }
  }

  Iterator* iter;
  if (block != nullptr) {
    iter = block->NewIterator(table->rep->options.comparator);
    if (cache_handle == nullptr) {
      iter->RegisterCleanup(&DeleteBlock, block, nullptr);
    } else {
      // Realease the table_cache handle when we delete the block iterator
      iter->RegisterCleanup(&ReleaseBlock, block_cache, cache_handle);
    }
  } else {
    iter = NewErrorIterator(s);
  }
  iter->SeekToFirst();
//  DEBUG_arg("First key after the block create %s", iter->key().ToString().c_str());
  return iter;
}

Iterator* Table::BlockReader_async(void* arg, const ReadOptions& options,
                             const Slice& index_value) {
  Table* table = reinterpret_cast<Table*>(arg);
  Cache* block_cache = table->rep->options.block_cache;
  Block* block = nullptr;
  Cache::Handle* cache_handle = nullptr;

  BlockHandle handle;
  Slice input = index_value;
  Status s = handle.DecodeFrom(&input);
  // We intentionally allow extra stuff in index_value so that we
  // can add more features in the future.

  if (s.ok()) {
    BlockContents contents;
    if (block_cache != nullptr) {
      //      printf("There is a table_cache!!\n");
      char cache_key_buffer[16];
      EncodeFixed64(cache_key_buffer, table->rep->cache_id);
      EncodeFixed64(cache_key_buffer + 8, handle.offset());
      Slice key(cache_key_buffer, sizeof(cache_key_buffer));
#ifdef PROCESSANALYSIS
      auto start = std::chrono::high_resolution_clock::now();
#endif
      cache_handle = block_cache->Lookup(key);
#ifdef PROCESSANALYSIS
      auto stop = std::chrono::high_resolution_clock::now();
      auto lookup_duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
#endif
      if (cache_handle != nullptr) {
        block = reinterpret_cast<Block*>(block_cache->Value(cache_handle));
//        printf("Cache hit\n");
#ifdef PROCESSANALYSIS
        TableCache::cache_hit.fetch_add(1);
        TableCache::cache_hit_look_up_time.fetch_add(lookup_duration.count());
#endif
      } else {
#ifdef PROCESSANALYSIS
        TableCache::cache_miss.fetch_add(1);
        //        if(TableCache::cache_miss < TableCache::not_filtered){
        //          printf("warning\n");
        //        };
        start = std::chrono::high_resolution_clock::now();

#endif
        s = ReadDataBlock(&table->rep->remote_table.lock()->remote_data_mrs,
                          options, handle, &contents, 0);
#ifdef PROCESSANALYSIS
        stop = std::chrono::high_resolution_clock::now();
        auto blockfetch_duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
        TableCache::cache_miss_block_fetch_time.fetch_add(blockfetch_duration.count());
#endif

        if (s.ok()) {
          block = new Block(contents, DataBlock);
          if (options.fill_cache) {
            cache_handle = block_cache->Insert(key, block, block->size(),
                                               &DeleteCachedBlock);
          }
        }
      }
    } else {
      //      printf("NO table_cache found!!\n");
      s = ReadDataBlock(&table->rep->remote_table.lock()->remote_data_mrs,
                        options, handle, &contents, 0);
      if (s.ok()) {
        block = new Block(contents, DataBlock);
      }
    }
  }

  Iterator* iter;
  if (block != nullptr) {
    iter = block->NewIterator(table->rep->options.comparator);
    if (cache_handle == nullptr) {
      iter->RegisterCleanup(&DeleteBlock, block, nullptr);
    } else {
      // Realease the table_cache handle when we delete the block iterator
      iter->RegisterCleanup(&ReleaseBlock, block_cache, cache_handle);
    }
  } else {
    iter = NewErrorIterator(s);
  }
  iter->SeekToFirst();
  //  DEBUG_arg("First key after the block create %s", iter->key().ToString().c_str());
  return iter;
}
// Convert an index iterator value (i.e., an encoded BlockHandle)
// into an iterator over the contents of the corresponding block.
Slice Table::KVReader(void* arg, const ReadOptions& options,
                             const Slice& index_value) {
  Table* table = reinterpret_cast<Table*>(arg);
  Cache* block_cache = table->rep->options.block_cache;
  Block* block = nullptr;
  Cache::Handle* cache_handle = nullptr;

  BlockHandle handle;
  Slice input = index_value;
  Status s = handle.DecodeFrom(&input);
  // We intentionally allow extra stuff in index_value so that we
  // can add more features in the future.
  assert(s.ok());
  Slice result;
  auto table_meta = table->rep->remote_table.lock();
  ReadKVPair(&table->rep->remote_table.lock()->remote_data_mrs, options, handle,
             &result, table_meta->shard_target_node_id);
  return result;
}

Iterator* Table::NewIterator(const ReadOptions& options) const {
  auto table_meta = rep->remote_table.lock();
  if (table_meta->table_type == byte_addressable){
#ifdef USESEQITERATOR
//    printf("Byte-addressable table created, table number is %lu\n", table_meta->number);

    return new ByteAddressableSEQIterator(
        rep->index_block->NewIterator(rep->options.comparator),
        const_cast<Table*>(this), options, true, table_meta->shard_target_node_id);
#else
    return new ByteAddressableRAIterator(
        rep->index_block->NewIterator(rep->options.comparator),
        &Table::KVReader, const_cast<Table*>(this), options, true);
#endif
  }else{
//    printf("BLock based table created, table number is %lu\n", table_meta->number);
    return NewTwoLevelIterator(
        rep->index_block->NewIterator(rep->options.comparator),
        &Table::BlockReader, const_cast<Table*>(this), options);
  }

}
//Iterator* Table::NewSEQIterator(const ReadOptions& options) const {
//
//}
Status Table::InternalGet(const ReadOptions& options, const Slice& k, void* arg,
                          void (*handle_result)(void*, const Slice&,
                                                const Slice&)) {
  Status s;
  FullFilterBlockReader* filter = rep->filter;
  if (filter != nullptr && !filter->KeyMayMatch(ExtractUserKey(k))) {
    // Not found
#ifdef PROCESSANALYSIS
    int dummy = 0;
    TableCache::filtered.fetch_add(1);
#endif
#ifdef BLOOMANALYSIS
    //assert that bloom filter is correct
    Iterator* iiter = rep_->index_block->NewIterator(rep_->options.comparator);

    iiter->Seek(k);//binary search for block index
    if (iiter->Valid()) {
      Slice handle_value = iiter->value();
      BlockHandle handle;

      Iterator* block_iter = BlockReader(this, options, iiter->value());
      block_iter->Seek(k);
      if (block_iter->Valid()) {
        (*handle_result)(arg, block_iter->key(), block_iter->value());
        assert(*block_iter->key().data() == 0);

      }
      Saver* saver = reinterpret_cast<Saver*>(arg);
//      assert(saver->state == kNotFound);
      if(saver->state == kNotFound){
//        printf("filtered key not found\n");
        int dummy = 0;
      }else{
        assert(false);
        exit(1);
//        printf("filtered key found\n");
        int dummy = 0;
      }
      delete block_iter;
    }
#endif
  } else {
    if (rep->remote_table.lock()->table_type == block_based){
      Iterator* iiter = rep->index_block->NewIterator(rep->options.comparator);
#ifdef PROCESSANALYSIS
      auto start = std::chrono::high_resolution_clock::now();
#endif
      iiter->Seek(k);//binary search for block index
#ifdef PROCESSANALYSIS
      auto stop = std::chrono::high_resolution_clock::now();
      auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
      //    std::printf("Block Reader time elapse is %zu\n",  duration.count());
      TableCache::IndexBinarySearchTimeElapseSum.fetch_add(duration.count());
#endif
      if (iiter->Valid()) {

        Slice handle_value = iiter->value();

        BlockHandle handle;
#ifdef PROCESSANALYSIS
        TableCache::not_filtered.fetch_add(1);

        start = std::chrono::high_resolution_clock::now();
#endif
        Iterator* block_iter = BlockReader(this, options, iiter->value());
#ifdef PROCESSANALYSIS
        stop = std::chrono::high_resolution_clock::now();
        duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
        //    std::printf("Block Reader time elapse is %zu\n",  duration.count());
        TableCache::DataBlockFetchBeforeCacheElapseSum.fetch_add(duration.count());
#endif
#ifdef PROCESSANALYSIS
        start = std::chrono::high_resolution_clock::now();
#endif
        block_iter->Seek(k);
#ifdef PROCESSANALYSIS
        stop = std::chrono::high_resolution_clock::now();
        duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
        //    std::printf("Block Reader time elapse is %zu\n",  duration.count());
        TableCache::DataBinarySearchTimeElapseSum.fetch_add(duration.count());
#endif
        if (block_iter->Valid()) {
          (*handle_result)(arg, block_iter->key(), block_iter->value());
          assert(*block_iter->key().data() == 0);
        }
        s = block_iter->status();
        delete block_iter;

#ifdef PROCESSANALYSIS
        Saver* saver = reinterpret_cast<Saver*>(arg);
        if(saver->state == kFound){
          TableCache::foundNum.fetch_add(1);
        }
#endif

      }else{
        printf("block iterator invalid\n");
        exit(1);
      }
      delete iiter;
    }else{
#ifdef PROCESSANALYSIS
      auto start = std::chrono::high_resolution_clock::now();
      TableCache::not_filtered.fetch_add(1);
#endif

      //    Iterator* iter = NewIterator(options);
      //    iter->Seek(k);
      // todo: Can we directly search by the index block without create a iterator?
      Iterator* iiter = rep->index_block->NewIterator(rep->options.comparator);
      iiter->Seek(k);
#ifdef PROCESSANALYSIS
      auto stop = std::chrono::high_resolution_clock::now();
      auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
      //    std::printf("Block Reader time elapse is %zu\n",  duration.count());
      TableCache::IndexBinarySearchTimeElapseSum.fetch_add(duration.count());
#endif
      if (iiter->Valid()){
        //Early leave if the key does not match.
        ParsedInternalKey parsed_key;
        Saver* saver_ = reinterpret_cast<Saver*>(arg);
        if (!ParseInternalKey(iiter->key(), &parsed_key)) {
          // TOTHINK: may be the parse internal key is too slow?
          saver_->state = kCorrupt;
        } else {
          if (saver_->ucmp->Compare(parsed_key.user_key, saver_->user_key) !=0) {
            delete iiter;
            return s;
          }
        }
        // if the key is what we want, fetch the value from the remote memory
        Slice handle = iiter->value();
        BlockHandle bhandle;
        bhandle.DecodeFrom(&handle);
        //      rdma_mg->Deallocate_Local_RDMA_Slot(mr_addr, DataChunk);

        auto rdma_mg = Env::Default()->rdma_mg;
        Slice KV;
        Slice key;
        Slice value;
        auto table_meta = rep->remote_table.lock();

        s = ReadKVPair(&table_meta->remote_data_mrs, options,
                       bhandle, &KV, table_meta->shard_target_node_id);

        char* mr_addr = (char*)KV.data();
        uint32_t key_size, value_size;
        GetFixed32(&KV, &key_size);
        GetFixed32(&KV, &value_size);
        assert(key_size + value_size == KV.size());


        key = Slice(KV.data(), key_size);
        KV.remove_prefix(key_size);
        assert(KV.size() == value_size);
        value = KV;
        (*handle_result)(arg, key, value);
        //      rdma_mg->Deallocate_Local_RDMA_Slot(mr_addr, DataChunk);
      }
      delete iiter;

    }


  }

  return s;
}
//void Table::GetKV(Iterator* iiter) {
//
//}
uint64_t Table::ApproximateOffsetOf(const Slice& key) const {
  Iterator* index_iter = rep->index_block->NewIterator(rep->options.comparator);
  index_iter->Seek(key);
  uint64_t result;
  if (index_iter->Valid()) {
    BlockHandle handle;
    Slice input = index_iter->value();
    Status s = handle.DecodeFrom(&input);
    if (s.ok()) {
      result = handle.offset();
    } else {
      // Strange: we can't decode the block handle in the index block.
      // We'll just return the offset of the metaindex block, which is
      // close to the whole file size for this case.
      result = rep->metaindex_handle.offset();
    }
  } else {
    // key is past the last key in the file.  Approximate the offset
    // by returning the offset of the metaindex block (which is
    // right near the end of the file).
    result = rep->metaindex_handle.offset();
  }
  delete index_iter;
  return result;
}


}  // namespace TimberSaw
