// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_TimberSaw_TABLE_BLOCK_BUILDER_H_
#define STORAGE_TimberSaw_TABLE_BLOCK_BUILDER_H_

#include <cstdint>
#include <vector>

#include "TimberSaw/slice.h"
#include "util/rdma.h"
namespace TimberSaw {

struct Options;

class BlockBuilder {
 public:
  explicit BlockBuilder(const Options* options, ibv_mr* mr);

  BlockBuilder(const BlockBuilder&) = delete;
  BlockBuilder& operator=(const BlockBuilder&) = delete;

  // Reset the contents as if the BlockBuilder was just constructed.
  void Reset_Forward();
  void Reset_Buffer(char* data, size_t size);

  // move the buffer locate at a different place
  void Move_buffer(const char* p);
  // REQUIRES: Finish() has not been called since the last call to Reset().
  // REQUIRES: key is larger than any previously added key
  void Add(const Slice& key, const Slice& value);

  // Finish building the block and return a slice that refers to the
  // block contents.  The returned slice will remain valid for the
  // lifetime of this builder or until Reset() is called.
  Slice Finish();

  // Returns an estimate of the current (uncompressed) size of the block
  // we are building.
  size_t CurrentSizeEstimate() const;

  // Return true iff no entries have been added since the last Reset()
  bool empty() const { return buffer.empty(); }
  Slice buffer;              // Destination buffer
 private:
  const Options* options_;
  ibv_mr* local_mr;

  std::vector<uint32_t> restarts_;  // Restart points
  int counter_;                     // Number of entries emitted since restart
  bool finished_;                   // Has Finish() been called?
  std::string last_key_;
};

}  // namespace TimberSaw

#endif  // STORAGE_TimberSaw_TABLE_BLOCK_BUILDER_H_
