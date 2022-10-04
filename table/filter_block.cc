// Copyright (c) 2012 The TimberSaw Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/filter_block.h"

#include "TimberSaw/filter_policy.h"
#include "util/coding.h"

namespace TimberSaw {

// See doc/table_format.md for an explanation of the filter block format.

// Generate new filter every 2KB of data
static const size_t kFilterBaseLg = 11;
static const size_t kFilterBase = 1 << kFilterBaseLg;

FilterBlockBuilder::FilterBlockBuilder(const FilterPolicy* policy,
                                       std::vector<ibv_mr*>* mrs,
                                       std::map<int, ibv_mr*>* remote_mrs,
                                       std::shared_ptr<RDMA_Manager> rdma_mg,
                                       std::string& type_string)
    : policy_(policy), rdma_mg_(rdma_mg),
      local_mrs(mrs), remote_mrs_(remote_mrs), type_string_(type_string),
      result((char*)(*mrs)[0]->addr, 0) {}

//TOTHINK: One block per bloom filter, then why there is a design for the while loop?
// Is it a bad design?
// Answer: Every bloomfilter corresponding to one block, but the filter offsets
// was set every 2KB. for the starting KV of a data block which lies in the same 2KB
// chunk of the last block, it was wronglly assigned to the last bloom filter
void FilterBlockBuilder::StartBlock(uint64_t block_offset) {
  uint64_t filter_index = (block_offset / kFilterBase);
  assert(filter_index >= filter_offsets_.size());
  while (filter_index > filter_offsets_.size()) {
    GenerateFilter();
  }
}
size_t FilterBlockBuilder::CurrentSizeEstimate() {
  //result plus filter offsets plus array offset plus 1 char for kFilterBaseLg
  return (result.size() + filter_offsets_.size()*4 +5);
}
void FilterBlockBuilder::AddKey(const Slice& key) {
  Slice k = key;
  start_.push_back(keys_.size());
  keys_.append(k.data(), k.size());
}

Slice FilterBlockBuilder::Finish() {
  if (!start_.empty()) {
    GenerateFilter();
  }

  // Append array of per-filter offsets
  const uint32_t array_offset = result.size();
  for (size_t i = 0; i < filter_offsets_.size(); i++) {
    PutFixed32(&result, filter_offsets_[i]);
  }

  PutFixed32(&result, array_offset);
  char length_perfilter = static_cast<char>(kFilterBaseLg);
  result.append(&length_perfilter,1);  // Save encoding parameter in result
//  Flush();
  //TOFIX: Here could be some other data structures not been cleared.


  return Slice(result);
}
void FilterBlockBuilder::Reset() {
  result.Reset(static_cast<char*>((*local_mrs)[0]->addr),0);
}
void FilterBlockBuilder::Move_buffer(const char* p){
  result.Reset(p,0);
}
// This function is actually not being used
void FilterBlockBuilder::Flush() {
  ibv_mr* remote_mr = new ibv_mr();
  size_t msg_size = result.size();
  rdma_mg_->Allocate_Remote_RDMA_Slot(*remote_mr, 0);
  rdma_mg_->RDMA_Write(remote_mr, (*local_mrs)[0], msg_size, type_string_,
                       IBV_SEND_SIGNALED, 0, 0);
  remote_mr->length = msg_size;
  if(remote_mrs_->empty()){
    remote_mrs_->insert({0, remote_mr});
  }else{
    remote_mrs_->insert({remote_mrs_->rbegin()->first+1, remote_mr});
  }
  Reset();
}
void FilterBlockBuilder::GenerateFilter() {
  const size_t num_keys = start_.size();
  if (num_keys == 0) {
    // Fast path if there are no keys for this filter
    filter_offsets_.push_back(result.size());
    return;
  }

  // Make list of keys from flattened key structure
  start_.push_back(keys_.size());  // Simplify length computation
  tmp_keys_.resize(num_keys);
  for (size_t i = 0; i < num_keys; i++) {
    char* base = keys_.data() + start_[i];
    size_t length = start_[i + 1] - start_[i];
    tmp_keys_[i] = Slice(base, length);
  }

  // Generate filter for current set of keys and append to result.
  filter_offsets_.push_back(result.size());
  policy_->CreateFilter(&tmp_keys_[0], static_cast<int>(num_keys), &result);
  tmp_keys_.clear();
  keys_.clear();
  start_.clear();
}

FilterBlockReader::FilterBlockReader(const FilterPolicy* policy,
                                     const Slice& contents,
                                     std::shared_ptr<RDMA_Manager> rdma_mg)
    : policy_(policy), data_(nullptr), offset_(nullptr), num_(0), base_lg_(0), rdma_mg_(rdma_mg) {
  size_t n = contents.size();
  if (n < 5) return;  // 1 byte for base_lg_ and 4 for start of offset array
  base_lg_ = contents[n - 1];
  uint32_t last_word = DecodeFixed32(contents.data() + n - 5);
  if (last_word > n - 5) return;
  data_ = contents.data();
  offset_ = data_ + last_word;
  num_ = (n - 5 - last_word) / 4;
}
bool FilterBlockReader::KeyMayMatch(uint64_t block_offset, const Slice& key) {
  uint64_t index = block_offset >> base_lg_;
  if (index < num_) {
    uint32_t start = DecodeFixed32(offset_ + index * 4);
    uint32_t limit = DecodeFixed32(offset_ + index * 4 + 4);
    if (start <= limit && limit <= static_cast<size_t>(offset_ - data_)) {
      Slice filter = Slice(data_ + start, limit - start);
      return policy_->KeyMayMatch(key, filter);
    } else if (start == limit) {
      // Empty filters do not match any keys
      return false;
    }
  }
  return true;  // Errors are treated as potential matches
}
FilterBlockReader::~FilterBlockReader() {
  if (!rdma_mg_->Deallocate_Local_RDMA_Slot((void*)data_, FilterChunk)){
    printf("Filter Block deregisteration failed\n");
  }else{
//    printf("Filter block deregisteration successfully\n");
  }
}

}  // namespace TimberSaw
