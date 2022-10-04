// Copyright (c) 2012 The TimberSaw Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/full_filter_block.h"

#include <utility>

#include "TimberSaw/filter_policy.h"
#include "util/coding.h"

namespace TimberSaw {

// See doc/table_format.md for an explanation of the filter block format.

FullFilterBlockBuilder::FullFilterBlockBuilder(ibv_mr* mr,
                                               int bloombits_per_key)
    : local_mr(mr), bits_per_key_(bloombits_per_key),
      num_probes_(LegacyNoLocalityBloomImpl::ChooseNumProbes(bits_per_key_)),
      result((char*)mr->addr,0) {
//  filter_bits_builder_ = std::make_unique<LegacyBloomImpl>();
}

//TOTHINK: One block per bloom filter, then why there is a design for the while loop?
// Is it a bad design?
// Answer: Every bloomfilter corresponding to one block, but the filter offsets
// was set every 2KB. for the starting KV of a data block which lies in the same 2KB
// chunk of the last block, it was wronglly assigned to the last bloom filter
void FullFilterBlockBuilder::RestartBlock(uint64_t block_offset) {
//  uint64_t filter_index = (block_offset / kFilterBase);
  hash_entries_.clear();

}
//size_t FullFilterBlockBuilder::CurrentSizeEstimate() {
//  //result plus filter offsets plus array offset plus 1 char for kFilterBaseLg
//  return (result.size() + filter_offsets_.size()*4 +5);
//}
void FullFilterBlockBuilder::AddKey(const Slice& key) {
#ifndef NDEBUG
  if (key.size() == 29)
    printf("here\n");
#endif

  uint32_t hash = BloomHash(key);
  if (hash_entries_.size() == 0 || hash != hash_entries_.back()) {
    hash_entries_.push_back(hash);
  }
}
inline void FullFilterBlockBuilder::AddHash(uint32_t h, char* data,
                                            uint32_t num_lines,
                                            uint32_t total_bits) {
#ifndef NDEBUG
  static_cast<void>(total_bits);
#endif
  assert(num_lines > 0 && total_bits > 0);

  LegacyBloomImpl::AddHash(h, num_lines, num_probes_, data,
                           std::log2(CACHE_LINE_SIZE));
}
uint32_t FullFilterBlockBuilder::GetTotalBitsForLocality(uint32_t total_bits) {
  uint32_t num_lines =
      (total_bits + CACHE_LINE_SIZE * 8 - 1) / (CACHE_LINE_SIZE * 8);

  // Make num_lines an odd number to make sure more bits are involved
  // when determining which block.
  if (num_lines % 2 == 0) {
    num_lines++;
  }
  return num_lines * (CACHE_LINE_SIZE * 8);
}
uint32_t FullFilterBlockBuilder::CalculateSpace(const int num_entry,
                                                uint32_t* total_bits,
                                                uint32_t* num_lines) {
  assert(bits_per_key_);
  if (num_entry != 0) {
    uint32_t total_bits_tmp = static_cast<uint32_t>(num_entry * bits_per_key_);

    *total_bits = GetTotalBitsForLocality(total_bits_tmp);
    *num_lines = *total_bits / (CACHE_LINE_SIZE * 8);
    assert(*total_bits > 0 && *total_bits % 8 == 0);
  } else {
    // filter is empty, just leave space for metadata
    *total_bits = 0;
    *num_lines = 0;
  }

  // Reserve space for Filter
  uint32_t sz = *total_bits / 8;
  sz += 5;  // 4 bytes for num_lines, 1 byte for num_probes
  return sz;
}
void FullFilterBlockBuilder::Finish() {
  uint32_t total_bits, num_lines;
  size_t num_entries = hash_entries_.size();
  CalculateSpace(num_entries, &total_bits, &num_lines);
//  char* data =
//      ReserveSpace(static_cast<int>(num_entries), &total_bits, &num_lines);
  char* data = static_cast<char*>(const_cast<char*>(result.data()));
  assert(total_bits%8 == 0);
//  result.Reset(result.data(), total_bits/8);
  assert(data);
  assert(total_bits/8 + 5 <= local_mr->length);
  if (total_bits != 0 && num_lines != 0) {
    for (auto h : hash_entries_) {
//      int log2_cache_line_bytes = std::log2(CACHE_LINE_SIZE);
      AddHash(h, data, num_lines, total_bits);
    }

    // Check for excessive entries for 32-bit hash function
    if (num_entries >= /* minimum of 3 million */ 3000000U) {
      // More specifically, we can detect that the 32-bit hash function
      // is causing significant increase in FP rate by comparing current
      // estimated FP rate to what we would get with a normal number of
      // keys at same memory ratio.
      double est_fp_rate = LegacyBloomImpl::EstimatedFpRate(
          num_entries, total_bits / 8, num_probes_);
      double vs_fp_rate = LegacyBloomImpl::EstimatedFpRate(
          1U << 16, (1U << 16) * bits_per_key_ / 8, num_probes_);

      if (est_fp_rate >= 1.50 * vs_fp_rate) {
        // For more details, see
        // https://github.com/facebook/rocksdb/wiki/RocksDB-Bloom-Filter
        printf(
            "Using legacy SST/BBT Bloom filter with excessive key count "
            "(%.1fM @ %dbpk), causing estimated %.1fx higher filter FP rate. "
            "Consider using new Bloom with format_version>=5, smaller SST "
            "file size, or partitioned filters.",
            num_entries / 1000000.0, bits_per_key_, est_fp_rate / vs_fp_rate);
      }
    }
  }
  // See BloomFilterPolicy::GetFilterBitsReader for metadata
  data[total_bits / 8] = static_cast<char>(num_probes_);
  EncodeFixed32(data + total_bits / 8 + 1, static_cast<uint32_t>(num_lines));

  const char* const_data = data;
  hash_entries_.clear();
  result.Reset(data, total_bits / 8 + 5);
//  return Slice(data, total_bits / 8 + 5);
}
void FullFilterBlockBuilder::Reset() {
  result.Reset(static_cast<char*>(local_mr->addr),0);
}
void FullFilterBlockBuilder::Move_buffer(const char* p){
  result.Reset(p,0);
}
//void FullFilterBlockBuilder::Flush() {
//  ibv_mr* remote_mr = new ibv_mr();
//  size_t msg_size = result.size();
//  rdma_mg_->Allocate_Remote_RDMA_Slot(*remote_mr);
//  rdma_mg_->RDMA_Write(remote_mr, (*local_mrs)[0], msg_size, type_string_,IBV_SEND_SIGNALED, 0);
//  remote_mr->length = msg_size;
//  if(remote_mrs_->empty()){
//    remote_mrs_->insert({0, remote_mr});
//  }else{
//    remote_mrs_->insert({remote_mrs_->rbegin()->first+1, remote_mr});
//  }
//  Reset();
//}
//void FullFilterBlockBuilder::GenerateFilter() {
//  const size_t num_keys = start_.size();
//  if (num_keys == 0) {
//    // Fast path if there are no keys for this filter
//    filter_offsets_.push_back(result.size());
//    return;
//  }
//
//  // Make list of keys from flattened key structure
//  start_.push_back(keys_.size());  // Simplify length computation
//  tmp_keys_.resize(num_keys);
//  for (size_t i = 0; i < num_keys; i++) {
//    char* base = keys_.data() + start_[i];
//    size_t length = start_[i + 1] - start_[i];
//    tmp_keys_[i] = Slice(base, length);
//  }
//
//  // Generate filter for current set of keys and append to result.
//  filter_offsets_.push_back(result.size());
//  policy_->CreateFilter(&tmp_keys_[0], static_cast<int>(num_keys), &result);
//  tmp_keys_.clear();
//  keys_.clear();
//  start_.clear();
//}

FullFilterBlockReader::FullFilterBlockReader(
    const Slice& contents, std::shared_ptr<RDMA_Manager> rdma_mg,
    FilterSide side)
    :filter_content(contents), data_(contents.data()), rdma_mg_(rdma_mg),
    filter_side(side){
  uint32_t len_with_meta = static_cast<uint32_t>(contents.size());
  if (len_with_meta <= 5) {
    // filter is empty or broken. Treat like zero keys added.
    std::cerr << "corrupt bloom filter" << std::endl;
  }

  // Legacy Bloom filter data:
  //             0 +-----------------------------------+
  //               | Raw Bloom filter data             |
  //               | ...                               |
  //           len +-----------------------------------+
  //               | byte for num_probes or            |
  //               |   marker for new implementations  |
  //         len+1 +-----------------------------------+
  //               | four bytes for number of table_cache    |
  //               |   lines                           |
  // len_with_meta +-----------------------------------+

  num_probes_ =
      static_cast<int>(contents.data()[len_with_meta - 5]);
  // NB: *num_probes > 30 and < 128 probably have not been used, because of
  // BloomFilterPolicy::initialize, unless directly calling
  // LegacyBloomBitsBuilder as an API, but we are leaving those cases in
  // limbo with LegacyBloomBitsReader for now.

  if (num_probes_ < 1) {
    // Note: < 0 (or unsigned > 127) indicate special new implementations
    // (or reserved for future use)
    if (num_probes_ == -1) {
      // Marker for newer Bloom implementations
      std::cerr << "corrupt bloom filter" << std::endl;
      exit(1);

    }
    // otherwise
    // Treat as zero probes (always FP) for now.
    std::cerr << "corrupt bloom filter" << std::endl;
    exit(1);

  }
  // else attempt decode for LegacyBloomBitsReader

  assert(num_probes_ >= 1);
  assert(num_probes_ <= 127);

  uint32_t len = len_with_meta - 5;
  assert(len > 0);

  num_lines_ = DecodeFixed32(contents.data() + len_with_meta - 4);
//  uint32_t log2_cache_line_size;
  if (num_lines_ * CACHE_LINE_SIZE == len) {
    // Common case
    log2_cache_line_size_ = std::log2(CACHE_LINE_SIZE);
  } else if (num_lines_ == 0 || len % num_lines_ != 0) {
    // Invalid (no solution to num_lines * x == len)
    // Treat as zero probes (always FP) for now.
    std::cerr << "corrupt bloom filter" << std::endl;
    exit(1);
  }
}

//bool FullFilterBlockReader::KeyMayMatch(uint64_t block_offset, const Slice& key) {
//  uint64_t index = block_offset >> base_lg_;
//  if (index < num_) {
//    uint32_t start = DecodeFixed32(offset_ + index * 4);
//    uint32_t limit = DecodeFixed32(offset_ + index * 4 + 4);
//    if (start <= limit && limit <= static_cast<size_t>(offset_ - data_)) {
//      Slice filter = Slice(data_ + start, limit - start);
//      return policy_->KeyMayMatch(key, filter);
//    } else if (start == limit) {
//      // Empty filters do not match any keys
//      return false;
//    }
//  }
//  return true;  // Errors are treated as potential matches
//}
bool FullFilterBlockReader::KeyMayMatch(const Slice& key) {
//  auto start = std::chrono::high_resolution_clock::now();
  uint32_t hash = BloomHash(key);
  uint32_t byte_offset;
  LegacyBloomImpl::PrepareHashMayMatch(
      hash, num_lines_, data_, /*out*/ &byte_offset, log2_cache_line_size_);
  bool ret = LegacyBloomImpl::HashMayMatchPrepared(
      hash, num_probes_, data_ + byte_offset, log2_cache_line_size_);
//  auto stop = std::chrono::high_resolution_clock::now();
//  auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
//      std::printf("bloom filter check time elapse is %zu\n",  duration.count());
  return ret;



}
FullFilterBlockReader::~FullFilterBlockReader() {
  if (filter_side == Compute){
    if (!rdma_mg_->Deallocate_Local_RDMA_Slot((void*)filter_content.data(), FilterChunk)){
      DEBUG("Filter Block deregisteration failed\n");
    }else{
//          printf("Filter block deregisteration successfully\n");
    }
  }

}

}  // namespace TimberSaw
