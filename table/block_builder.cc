// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// BlockBuilder generates blocks where keys are prefix-compressed:
//
// When we store a key, we drop the prefix shared with the previous
// string.  This helps reduce the space requirement significantly.
// Furthermore, once every K keys, we do not apply the prefix
// compression and store the entire key.  We call this a "restart
// point".  The tail end of the block stores the offsets of all of the
// restart points, and can be used to do a binary search when looking
// for a particular key.  Values are stored as-is (without compression)
// immediately following the corresponding key.
//
// An entry for a particular key-value pair has the form:
//     shared_bytes: varint32
//     unshared_bytes: varint32
//     value_length: varint32
//     key_delta: char[unshared_bytes]
//     value: char[value_length]
// shared_bytes == 0 for restart points.
//
// The trailer of the block has the form:
//     restarts: uint32[num_restarts]
//     num_restarts: uint32
// restarts[i] contains the offset within the block of the ith restart point.

#include "table/block_builder.h"

#include <algorithm>
#include <cassert>

#include "TimberSaw/comparator.h"
#include "TimberSaw/options.h"
#include "util/coding.h"

namespace TimberSaw {

BlockBuilder::BlockBuilder(const Options* options, ibv_mr* mr)
    : options_(options), local_mr(mr),
      buffer(const_cast<const char*>(static_cast<char*>(mr->addr)),0),
      restarts_(), counter_(0), finished_(false) {
  assert(options->block_restart_interval >= 1);
  restarts_.push_back(0);  // First restart point is at offset 0

}

void BlockBuilder::Reset_Forward() {
  buffer.ResetNext();
  restarts_.clear();
  restarts_.push_back(0);  // First restart point is at offset 0
  counter_ = 0;
  finished_ = false;
  last_key_.clear();
}

void BlockBuilder::Reset_Buffer(char* data, size_t size) {
  buffer.Reset(data, size);
//  restarts_.clear();
//  restarts_.push_back(0);  // First restart point is at offset 0
//  counter_ = 0;
//  finished_ = false;
//  last_key_.clear();
}
void BlockBuilder::Move_buffer(const char* p) { buffer.Reset(p,0);
}
size_t BlockBuilder::CurrentSizeEstimate() const {
  return (buffer.size() +                       // Raw data buffer
          restarts_.size() * sizeof(uint32_t) +  // Restart array
          sizeof(uint32_t));                     // Restart array length
}

Slice BlockBuilder::Finish() {
  // Append restart array
  for (size_t i = 0; i < restarts_.size(); i++) {
    PutFixed32(&buffer, restarts_[i]);
  }
//  assert(restarts_.size() > 1);
//  assert(restarts_.size() < 200000);
  PutFixed32(&buffer, restarts_.size());
  finished_ = true;
  return Slice(buffer);
}

void BlockBuilder::Add(const Slice& key, const Slice& value) {
  Slice last_key_piece(last_key_);
  assert(key.size() >= 8);
  assert(!finished_);
  assert(counter_ <= options_->block_restart_interval);
  assert(buffer.empty()  // No values yet?
         || options_->comparator->Compare(key, last_key_piece) > 0);
  size_t shared = 0;
  if (counter_ < options_->block_restart_interval) {
    // See how much sharing to do with previous string
    const size_t min_length = std::min(last_key_piece.size(), key.size());
    while ((shared < min_length) && (last_key_piece[shared] == key[shared])) {
      shared++;
    }
  } else {
    // Restart compression
    restarts_.push_back(buffer.size());
    counter_ = 0;
  }
  const size_t non_shared = key.size() - shared;

  // Add "<shared><non_shared><value_size>" to buffer
//  assert(shared <= 29);
//  assert(non_shared <= 29);
//#ifndef BYTEADDRESSABLE
  PutVarint32(&buffer, shared);
//#endif
  PutVarint32(&buffer, non_shared);

  PutVarint32(&buffer, value.size());
//  assert(value.size() == 400);

  // Add string delta to buffer followed by value
  buffer.append(key.data() + shared, non_shared);
  buffer.append(value.data(), value.size());

  // Update state
  last_key_.resize(shared);
  last_key_.append(key.data() + shared, non_shared);
  assert(Slice(last_key_) == key);
  counter_++;
}


}  // namespace TimberSaw
