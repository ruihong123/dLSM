//
// Created by ruihong on 8/7/21.
//

#ifndef TimberSaw_TABLE_BUILDER_MEMORYSIDE_H
#define TimberSaw_TABLE_BUILDER_MEMORYSIDE_H

#include "include/TimberSaw/table_builder.h"
//#include "dumpfile.h"
namespace TimberSaw {
//class BlockBuilder;
class BlockHandle;
//class WritableFile;
//enum IO_type {Compact, Flush};
class TimberSaw_EXPORT TableBuilder_Memoryside : public TableBuilder {
 public:
  // Create a builder that will store the contents of the table it is
  // building in *file.  Does not close the file.  It is up to the
  // caller to close the file after calling Finish().
  TableBuilder_Memoryside(const Options& options, IO_type type,
                          std::shared_ptr<RDMA_Manager> rdma_mg);

  TableBuilder_Memoryside(const TableBuilder_Memoryside&) = delete;
  TableBuilder_Memoryside& operator=(const TableBuilder_Memoryside&) = delete;

  // REQUIRES: Either Finish() or Abandon() has been called.
  ~TableBuilder_Memoryside() override;

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
  void Add(const Slice& key, const Slice& value) override;

  // Advanced operation: flush any buffered key/value pairs to remote memory.
  // Can be used to ensure that two adjacent entries never live in
  // the same data block.  Most clients should not need to use this method.
  // REQUIRES: Finish(), Abandon() have not been called
  void FlushData() override;
  void FlushDataIndex(size_t msg_size) override;
  void FlushFilter(size_t& msg_size) override;
  // add element into index block and filters for this data block.
  void UpdateFunctionBLock() override;

  // Return non-ok iff some error has been detected.
  Status status() const override;

  // Finish building the table.  Stops using the file passed to the
  // constructor after this function returns.
  // REQUIRES: Finish(), Abandon() have not been called
  Status Finish() override;

  // Indicate that the contents of this builder should be abandoned.  Stops
  // using the file passed to the constructor after this function returns.
  // If the caller is not going to call Finish(), it must call Abandon()
  // before destroying this builder.
  // REQUIRES: Finish(), Abandon() have not been called
  void Abandon() override;

  // Number of calls to Add() so far.
  uint64_t NumEntries() const override;

  // Size of the file generated so far.  If invoked after a successful
  // Finish() call, returns the size of the final generated file.
  uint64_t FileSize() const override;
  void get_datablocks_map(std::map<uint32_t, ibv_mr*>& map) override;
  void get_dataindexblocks_map(std::map<uint32_t, ibv_mr*>& map) override;
  void get_filter_map(std::map<uint32_t, ibv_mr*>& map) override;
  size_t get_numentries() override;

  bool ok() const override { return status().ok(); }
  void FinishDataBlock(BlockBuilder* block, BlockHandle* handle,
                       CompressionType compressiontype) override;
  void FinishDataIndexBlock(BlockBuilder* block, BlockHandle* handle,
                            CompressionType compressiontype,
                            size_t& block_size) override;
  void FinishFilterBlock(FullFilterBlockBuilder* block, BlockHandle* handle,
                         CompressionType compressiontype,
                         size_t& block_size) override;
 protected:
  struct Rep;//make it inherit the one in Table builder?
  Rep* rep_;
};
}
#endif  // TimberSaw_TABLE_BUILDER_MEMORYSIDE_H
