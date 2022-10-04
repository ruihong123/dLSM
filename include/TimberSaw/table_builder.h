//
// Created by ruihong on 8/13/21.
//

#ifndef TimberSaw_TABLE_BUILDER_H
#define TimberSaw_TABLE_BUILDER_H
//#include "TimberSaw/export.h"
//#include "TimberSaw/options.h"
//#include "TimberSaw/status.h"
//#include "TimberSaw/comparator.h"
//#include "TimberSaw/env.h"
//#include "TimberSaw/filter_policy.h"
//#include "TimberSaw/options.h"
#include "table/block_builder.h"
//#include "table/filter_block.h"
#include "table/full_filter_block.h"
#include "table/format.h"
//#include "util/coding.h"
//#include "util/crc32c.h"
namespace TimberSaw {

//class BlockBuilder;
class BlockHandle;
//class WritableFile;

enum IO_type {Compact, Flush};
class TimberSaw_EXPORT TableBuilder {
 public:
  // Create a builder that will store the contents of the table it is
  // building in *file.  Does not close the file.  It is up to the
  // caller to close the file after calling Finish().
//  TableBuilder(const Options& options, IO_type type){};
  TableBuilder();
  TableBuilder(const TableBuilder&) = delete;
  TableBuilder& operator=(const TableBuilder&) = delete;

  // REQUIRES: Either Finish() or Abandon() has been called.
  virtual ~TableBuilder();

  // Change the options used by this builder.  Note: only some of the
  // option fields can be changed after construction.  If a field is
  // not allowed to change dynamically and its value in the structure
  // passed to the constructor is different from its value in the
  // structure passed to this method, this method will return an error
  // without changing any fields.
  //  Status ChangeOptions(const Options& options);

  // Add key,value to the table being constructed.
  // REQUIRES: key is after any previously added key according to comparator.
  // REQUIRES: Finish(), Abandon() have not been called
  virtual void Add(const Slice& key, const Slice& value)=0;

  // Advanced operation: flush any buffered key/value pairs to remote memory.
  // Can be used to ensure that two adjacent entries never live in
  // the same data block.  Most clients should not need to use this method.
  // REQUIRES: Finish(), Abandon() have not been called
  virtual void FlushData()=0;
  virtual void FlushDataIndex(size_t msg_size)=0;
  virtual void FlushFilter(size_t& msg_size)=0;
  // add element into index block and filters for this data block.
  virtual void UpdateFunctionBLock()=0;

  // Return non-ok iff some error has been detected.
  virtual Status status() const=0;

  // Finish building the table.  Stops using the file passed to the
  // constructor after this function returns.
  // REQUIRES: Finish(), Abandon() have not been called
  virtual Status Finish()=0;

  // Indicate that the contents of this builder should be abandoned.  Stops
  // using the file passed to the constructor after this function returns.
  // If the caller is not going to call Finish(), it must call Abandon()
  // before destroying this builder.
  // REQUIRES: Finish(), Abandon() have not been called
  virtual void Abandon()=0;

  // Number of calls to Add() so far.
  virtual uint64_t NumEntries() const=0;

  // Size of the file generated so far.  If invoked after a successful
  // Finish() call, returns the size of the final generated file.
  virtual uint64_t FileSize() const=0;
  virtual bool ok() const { return status().ok(); }
  virtual void FinishDataBlock(BlockBuilder* block, BlockHandle* handle,
                       CompressionType compressiontype)=0;
  virtual void FinishDataIndexBlock(BlockBuilder* block, BlockHandle* handle,
                            CompressionType compressiontype,
                            size_t& block_size)=0;
  virtual void FinishFilterBlock(FullFilterBlockBuilder* block, BlockHandle* handle,
                         CompressionType compressiontype,
                         size_t& block_size)=0;
  virtual void get_datablocks_map(std::map<uint32_t, ibv_mr*>& map)=0;
  virtual void get_dataindexblocks_map(std::map<uint32_t, ibv_mr*>& map)=0;
  virtual void get_filter_map(std::map<uint32_t, ibv_mr*>& map)=0;
  virtual size_t get_numentries()=0;
 protected:


  //  struct Rep;
//  struct Rep {
//    Rep(const Options& opt, IO_type type)
//    : options(opt),
//    type_(type),
//    index_block_options(opt),
//    offset(0),
//    offset_last_flushed(0),
//
//    num_entries(0),
//    closed(false),
//    pending_index_filter_entry(false) {
//      //TOTHINK: why the block restart interval is 1 by default?
//      // This is only for index block, is it the same for rocks DB?
//      index_block_options.block_restart_interval = 1;
//      std::shared_ptr<RDMA_Manager> rdma_mg = options.env->rdma_mg;
//      ibv_mr* temp_data_mr = new ibv_mr();
//      ibv_mr* temp_index_mr = new ibv_mr();
//      ibv_mr* temp_filter_mr = new ibv_mr();
//      //first create two buffer for each slot.
//      rdma_mg->Allocate_Local_RDMA_Slot(*temp_data_mr, "FlushBuffer");
//      rdma_mg->Allocate_Local_RDMA_Slot(*temp_index_mr, "FlushBuffer");
//      rdma_mg->Allocate_Local_RDMA_Slot(*temp_filter_mr, "FlushBuffer");
//      local_data_mr.push_back(temp_data_mr);
//      local_index_mr.push_back(temp_index_mr);
//      memset(temp_filter_mr->addr, 0, temp_filter_mr->length);
//      local_filter_mr.push_back(temp_filter_mr);
//      temp_data_mr = new ibv_mr();
//      temp_index_mr = new ibv_mr();
//      temp_filter_mr = new ibv_mr();
//      rdma_mg->Allocate_Local_RDMA_Slot(*temp_data_mr, "FlushBuffer");
//      rdma_mg->Allocate_Local_RDMA_Slot(*temp_index_mr, "FlushBuffer");
//      rdma_mg->Allocate_Local_RDMA_Slot(*temp_filter_mr, "FlushBuffer");
//      local_data_mr.push_back(temp_data_mr);
//      local_index_mr.push_back(temp_index_mr);
//      memset(temp_filter_mr->addr, 0, temp_filter_mr->length);
//      local_filter_mr.push_back(temp_filter_mr);
//      //    delete temp_data_mr;
//      //    delete temp_index_mr;
//      //    delete temp_filter_mr;
//      data_block = new BlockBuilder(&options, local_data_mr[0]);
//      index_block = new BlockBuilder(&index_block_options, local_index_mr[0]);
//      if (type_ == IO_type::Compact){
//        type_string_ = "write_local_compact";
//      }else if(type_ == IO_type::Flush){
//        type_string_ = "write_local_flush";
//      }else{
//        assert(false);
//      }
//      filter_block = (opt.filter_policy == nullptr
//          ? nullptr
//          : new FullFilterBlockBuilder(local_filter_mr[0], opt.bloom_bits));
//
//      status = Status::OK();
//    }
//
//    const Options& options;
//    Options index_block_options;
//    IO_type type_;
//    std::string type_string_;
//    //  WritableFile* file;
//    std::vector<ibv_mr*> local_data_mr;
//    // the start index of the in use buffer
//    int data_inuse_start = -1;
//    // the end index of the use buffer
//    int data_inuse_end = -1;
//    // when start larger than end by 1, there could be two scenarios:
//    // First, all the buffer are outstanding
//    // second, no buffer is outstanding, those two status will both have start - end = 1
//    bool data_inuse_empty = true;
//    std::vector<ibv_mr*> local_index_mr;
//    std::vector<ibv_mr*> local_filter_mr;
//    //TODO: make the map offset -> ibv_mr*
//    std::map<uint32_t, ibv_mr*> remote_data_mrs;
//    std::map<uint32_t, ibv_mr*> remote_dataindex_mrs;
//    std::map<uint32_t, ibv_mr*> remote_filter_mrs;
//    //  std::vector<size_t> remote_mr_real_length;
//    uint64_t offset_last_flushed;
//    uint64_t offset;
//    Status status;
//    BlockBuilder* data_block;
//    BlockBuilder* index_block;
//    std::string last_key;
//    int64_t num_entries;
//    bool closed;  // Either Finish() or Abandon() has been called.
//    FullFilterBlockBuilder* filter_block;
//
//    // We do not emit the index entry for a block until we have seen the
//    // first key for the next data block.  This allows us to use shorter
//    // keys in the index block.  For example, consider a block boundary
//    // between the keys "the quick brown fox" and "the who".  We can use
//    // "the r" as the key for the index block entry since it is >= all
//    // entries in the first block and < all entries in subsequent
//    // blocks.
//    //
//    // Invariant: r->pending_index_filter_entry is true only if data_block is empty.
//    bool pending_index_filter_entry;
//    BlockHandle pending_data_handle;  // Handle to add to index block
//
//    std::string compressed_output;
//  };
//  Rep* rep_;
};


}  // namespace TimberSaw

#endif  // TimberSaw_TABLE_BUILDER_H
