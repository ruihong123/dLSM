//
// Created by ruihong on 7/9/21.
//

#ifndef TimberSaw_FULL_FILTER_BLOCK_H
#define TimberSaw_FULL_FILTER_BLOCK_H
#include <complex>
#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "TimberSaw/options.h"
#include "TimberSaw/slice.h"
#include "util/bloom_impl.h"
#include "util/hash.h"

#include "block_builder.h"

namespace TimberSaw {
using LegacyBloomImpl = LegacyLocalityBloomImpl</*ExtraRotates*/ false>;
class FilterPolicy;
class Env;
class Options;
enum FilterSide { Compute, Memory};

// A FullFilterBlockBuilder is used to construct all of the filters for a
// particular Table.  It generates a single string which is stored as
// a special block in the Table.
//
// The sequence of calls to FullFilterBlockBuilder must match the regexp:
//      (StartBlock AddKey*)* Finish
class FullFilterBlockBuilder {
 public:
  explicit FullFilterBlockBuilder(ibv_mr* mr, int bloombits_per_key);
  FullFilterBlockBuilder(const FullFilterBlockBuilder&) = delete;
  FullFilterBlockBuilder& operator=(const FullFilterBlockBuilder&) = delete;

  void RestartBlock(uint64_t block_offset);
//  size_t CurrentSizeEstimate();
  void AddKey(const Slice& key);
//  void AddHash(uint32_t h, char* data, uint32_t num_lines, uint32_t total_bits);
  void Finish();
  void Reset();
//  void Flush();
  void Move_buffer(const char* p);
  void AddHash(uint32_t h, char* data, uint32_t num_lines, uint32_t total_bits);
  uint32_t GetTotalBitsForLocality(uint32_t total_bits);
  uint32_t CalculateSpace(const int num_entry, uint32_t* total_bits,
                          uint32_t* num_lines);
  Slice result;           // Filter data computed so far
 private:
//  void GenerateFilter();

//  const FilterPolicy* policy_;
//  std::shared_ptr<RDMA_Manager> rdma_mg_;
  ibv_mr* local_mr;
//  std::map<uint32_t, ibv_mr*>* remote_mrs_;
//  std::unique_ptr<LegacyBloomImpl> filter_bits_builder_;
  int bits_per_key_;
  int num_probes_;
  std::vector<uint32_t> hash_entries_;
//  std::string keys_;             // Flattened key contents
//  std::vector<size_t> start_;    // Starting index in keys_ of each key
  //todo Make result Slice; make Policy->CreateFilter accept Slice rather than string
//  std::vector<Slice> tmp_keys_;  // policy_->CreateFilter() argument
//  std::vector<uint32_t> filter_offsets_; // The filters' offset within the filter block.
//  std::string type_string_;

};
class FullFilterBlockReader {
 public:
  Slice filter_content;

  // REQUIRES: "contents" and *policy must stay live while *this is live.
  FullFilterBlockReader(const Slice& contents,
                        std::shared_ptr<RDMA_Manager> rdma_mg, FilterSide side);
  ~FullFilterBlockReader();
  bool KeyMayMatch(const Slice& key); // full filter.
 private:
//  const FilterPolicy* policy_;
//  std::unique_ptr<FilterBitsReader> filter_bits_reader_;

  const char* data_;
  int num_probes_ = 0;
  uint32_t num_lines_ = 0;
  uint32_t log2_cache_line_size_ = 0;

//  const char* data_;    // Pointer to filter data (at block-start)
  size_t filter_size;

  std::shared_ptr<RDMA_Manager> rdma_mg_;
  FilterSide filter_side;
};

}  // namespace TimberSaw
#endif  // TimberSaw_FULL_FILTER_BLOCK_H
