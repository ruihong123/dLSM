//
// Created by ruihong on 8/7/21.
//
#include "table/table_memoryside.h"

#include "db/table_cache.h"
#include <util/crc32c.h>

#include "TimberSaw/cache.h"
#include "TimberSaw/comparator.h"
#include "TimberSaw/env.h"
#include "TimberSaw/filter_policy.h"
#include "TimberSaw/options.h"


#include "table/block.h"
#include "table/filter_block.h"
#include "table/format.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"
#include "full_filter_block.h"
#include "table/byte_addressable_RA_iterator.h"
namespace TimberSaw {
struct Table_Memory_Side::Rep {
  Rep(const Options& options) : options(options) {

  }
//  friend class TableCache;
  ~Rep() {
    delete filter;
    //    delete[] filter_data;
    delete index_block;
  }

  const Options& options;
  Status status;
  // shared ptr because we do not want the cached value be garbage collected because
  // we suppose there is no version set on the memory node. the original shared ptr will be destroyed
  std::shared_ptr<RemoteMemTableMetaData> remote_table;
  //  uint64_t cache_id;
  FullFilterBlockReader* filter;
  //  const char* filter_data;

  BlockHandle metaindex_handle;  // Handle to metaindex_block: saved from footer
  Block* index_block;
//  Table_Type table_type = byte_addressable;

};

Status Table_Memory_Side::Open(const Options& options, Table_Memory_Side** table,
                               const std::shared_ptr<RemoteMemTableMetaData>& Remote_table_meta) {
  *table = nullptr;
#ifndef NDEBUG
  printf("Open table %lu, creator id %d\n", Remote_table_meta->number, Remote_table_meta->creator_node_id);
#endif
  // Read the index block
  Status s = Status::OK();
  BlockContents index_block_contents;
  char* data = (char*)Remote_table_meta->remote_dataindex_mrs.begin()->second->addr;
  size_t size = Remote_table_meta->remote_dataindex_mrs.begin()->second->length;
  size_t n = size - kBlockTrailerSize;

//  ReadOptions opt;
  {
    const uint32_t crc = crc32c::Unmask(DecodeFixed32(data + n + 1));
    const uint32_t actual = crc32c::Value(data, n + 1);
    if (actual != crc) {
      //      delete[] buf;
      DEBUG("Index block Checksum mismatch\n");
      usleep(100);
      uint32_t new_crc = crc32c::Unmask(DecodeFixed32(data + n + 1));
      uint32_t new_actual = crc32c::Value(data, n + 1);
      assert(false);
      s = Status::Corruption("block checksum mismatch");
      return s;
    }
  }
  index_block_contents.data = Slice(data, n);

  //  if (options.paranoid_checks) {
  //    opt.verify_checksums = true;
  //  }
  //  s = ReadDataIndexBlock(Remote_table_meta->remote_dataindex_mrs.begin()->second,
  //                         opt, &index_block_contents);

  if (s.ok()) {
    // We've successfully read the footer and the index block: we're
    // ready to serve requests.
    Block* index_block = new Block(index_block_contents, Block_On_Memory_Side);
    Rep* rep = new Table_Memory_Side::Rep(options);
//    rep->options = options;
    //    rep->file = file;
    rep->remote_table = Remote_table_meta;
    //    rep->metaindex_handle = footer.metaindex_handle();
    rep->index_block = index_block;
    assert(rep->index_block->size() > 0);
    //    rep->cache_id = NewId();
    //    rep->filter_data = nullptr;
    rep->filter = nullptr;
    *table = new Table_Memory_Side(rep);
    (*table)->ReadFilter();
    //    (*table)->ReadMeta(footer);
  }else{
    assert(false);
  }

  return s;
}

void Table_Memory_Side::ReadFilter() {
  Status s = Status::OK();
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
  char* data = (char*)rep->remote_table->remote_filter_mrs.begin()->second->addr;
  size_t size = rep->remote_table->remote_filter_mrs.begin()->second->length;
  size_t n = size - kBlockTrailerSize;

  //  ReadOptions opt;
  {
    const uint32_t crc = crc32c::Unmask(DecodeFixed32(data + n + 1));
    const uint32_t actual = crc32c::Value(data, n + 1);
    if (actual != crc) {
      //      delete[] buf;
      DEBUG("Index block Checksum mismatch\n");
      assert(false);
      s = Status::Corruption("block checksum mismatch");
      return;
    }
  }
  block.data = Slice(data, n);

  //  if (!ReadFilterBlock(rep_->remote_table.lock()->remote_filter_mrs.begin()->second, opt, &block).ok()) {
  //    return;
  //  }
  //  if (block.heap_allocated) {
  //    rep_->filter_data = block.data.data();  // Will need to delete later
  //  }
  rep->filter = new FullFilterBlockReader(
      block.data, rep->remote_table->rdma_mg, Memory);
}
static void DeleteBlock(void* arg, void* ignored) {
  delete reinterpret_cast<Block*>(arg);
}
Table_Memory_Side::~Table_Memory_Side() { delete rep; }


// Convert an index iterator value (i.e., an encoded BlockHandle)
// into an iterator over the contents of the corresponding block.
//TODO: Support compressed block.
Iterator* Table_Memory_Side::BlockReader(void* arg, const ReadOptions& options,
                                         const Slice& index_value) {
  Table_Memory_Side* table = reinterpret_cast<Table_Memory_Side*>(arg);
  //  Cache* block_cache = table->rep_->options.block_cache;
  Block* block = nullptr;
//  Cache::Handle* cache_handle = nullptr;

  BlockHandle handle;
  Slice input = index_value;
  Status s = handle.DecodeFrom(&input);
  // We intentionally allow extra stuff in index_value so that we
  // can add more features in the future.

  if (s.ok()) {
    BlockContents contents;
    //The function below is correct, because the handle content the block without crc.
    Find_Local_MR(&table->rep->remote_table->remote_data_mrs, handle,
                  contents.data);

    //TODO: support compressed block.
    auto n = contents.data.size();
    char* buf = (char*)contents.data.data();
    if (options.verify_checksums) {
      const uint32_t crc = crc32c::Unmask(DecodeFixed32(buf + n + 1));
      const uint32_t actual = crc32c::Value(buf, n + 1);
      if (actual != crc) {
        //      delete[] buf;
        DEBUG("Data block Checksum mismatch\n");
        assert(false);
        s = Status::Corruption("block checksum mismatch");
        return nullptr;
      }
    }
    switch (buf[n]) {
        case kNoCompression: {
          block = new Block(contents, Block_On_Memory_Side);

          break;
        }
        case kSnappyCompression: {
          size_t ulength = 0;
          if (!port::Snappy_GetUncompressedLength(buf, n, &ulength)) {
            assert(false);
          }
          char* ubuf = new char[ulength];
          if (!port::Snappy_Uncompress(buf, n, ubuf)) {
            assert(false);
            delete[] ubuf;
          }
          contents.data.Reset(ubuf, ulength);
          block = new Block(contents, Block_On_Memory_Side_Compressed);
          break;
        }
        default:
          assert(buf[n] != kNoCompression);
          assert(false);
          DEBUG("Data block illegal compression type\n");
    }

  }

  Iterator* iter;
  iter = block->NewIterator(table->rep->options.comparator);
    if (block != nullptr) {
      iter = block->NewIterator(table->rep->options.comparator);
      iter->RegisterCleanup(&DeleteBlock, block, nullptr);

    } else {
      iter = NewErrorIterator(s);
    }
  iter->SeekToFirst();
  //  DEBUG_arg("First key after the block create %s", iter->key().ToString().c_str());
  return iter;
}
Slice Table_Memory_Side::KVReader(void* arg, const ReadOptions& options,
                                         const Slice& index_value) {
  Table_Memory_Side* table = reinterpret_cast<Table_Memory_Side*>(arg);
  //  Cache* block_cache = table->rep_->options.block_cache;

  BlockHandle handle;
  Slice input = index_value;
  Status s = handle.DecodeFrom(&input);
  // We intentionally allow extra stuff in index_value so that we
  // can add more features in the future.

  assert(s.ok());
  Slice KV;
  //The function below is correct, because the handle content the block without crc.
  Find_Local_MR(&table->rep->remote_table->remote_data_mrs, handle, KV);
  return KV;


}

Iterator* Table_Memory_Side::NewIterator(const ReadOptions& options) const {
  if (rep->remote_table->table_type == block_based){
      return NewTwoLevelIterator(
          rep->index_block->NewIterator(rep->options.comparator),
          &Table_Memory_Side::BlockReader, const_cast<Table_Memory_Side*>(this), options);
  }else{
          return new ByteAddressableRAIterator(
                     rep->index_block->NewIterator(rep->options.comparator),
                     &Table_Memory_Side::KVReader, const_cast<Table_Memory_Side*>(this), options, false);
  }

}

Status Table_Memory_Side::InternalGet(const ReadOptions& options, const Slice& k, void* arg,
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

  }

  return s;
}

uint64_t Table_Memory_Side::ApproximateOffsetOf(const Slice& key) const {
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
void* Table_Memory_Side::Get_remote_table_ptr() {
    return static_cast<void*>(rep->remote_table.get());
}
}
