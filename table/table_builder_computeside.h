// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// TableBuilder provides the interface used to build a Table
// (an immutable and sorted map from keys to values).
//
// Multiple threads can invoke const methods on a TableBuilder without
// external synchronization, but if any of the threads may call a
// non-const method, all threads accessing the same TableBuilder must use
// external synchronization.

#ifndef STORAGE_TimberSaw_TABLE_BUILDER_COMPUTESIDE_H
#define STORAGE_TimberSaw_TABLE_BUILDER_COMPUTESIDE_H

//#include <cstdint>
#include "TimberSaw/export.h"
#include "TimberSaw/options.h"
#include "TimberSaw/status.h"
#include "TimberSaw/comparator.h"
#include "TimberSaw/env.h"
#include "TimberSaw/filter_policy.h"
#include "TimberSaw/options.h"
#include "table/block_builder.h"
#include "table/filter_block.h"
#include "table/full_filter_block.h"
#include "table/format.h"
#include "util/coding.h"
#include "util/crc32c.h"
#include "include/TimberSaw/table_builder.h"
namespace TimberSaw {

//class BlockBuilder;
class BlockHandle;
//class WritableFile;

//enum IO_type {Compact, Flush};
class TimberSaw_EXPORT TableBuilder_ComputeSide : public TableBuilder{
 public:
  // Create a builder that will store the contents of the table it is
  // building in *file.  Does not close the file.  It is up to the
  // caller to close the file after calling Finish().
  TableBuilder_ComputeSide(const Options& options, IO_type type,
                           uint8_t target_node_id);
//  TableBuilder_ComputeSide() = default;
  TableBuilder_ComputeSide(const TableBuilder_ComputeSide&) = delete;
  TableBuilder_ComputeSide& operator=(const TableBuilder_ComputeSide&) = delete;

  // REQUIRES: Either Finish() or Abandon() has been called.
  ~TableBuilder_ComputeSide() override;

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
  bool ok() const override { return status().ok(); }
  void FinishDataBlock(BlockBuilder* block, BlockHandle* handle,
                       CompressionType compressiontype) override;
  void FinishDataIndexBlock(BlockBuilder* block, BlockHandle* handle,
                            CompressionType compressiontype,
                            size_t& block_size) override;
  void FinishFilterBlock(FullFilterBlockBuilder* block, BlockHandle* handle,
                         CompressionType compressiontype,
                         size_t& block_size) override;
  void get_datablocks_map(std::map<uint32_t, ibv_mr*>& map) override;
  void get_dataindexblocks_map(std::map<uint32_t, ibv_mr*>& map) override;
  void get_filter_map(std::map<uint32_t, ibv_mr*>& map) override;
  size_t get_numentries() override;
 protected:


  struct Rep;

  Rep* rep_;
};


}  // namespace TimberSaw

#endif  // STORAGE_TimberSaw_INCLUDE_TABLE_BUILDER_H_
