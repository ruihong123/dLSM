// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/format.h"

#include "TimberSaw/env.h"
#include "port/port.h"
#include "table/block.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace TimberSaw {

void BlockHandle::EncodeTo(std::string* dst) const {
  // Sanity check that all fields have been set
  assert(offset_ != ~static_cast<uint64_t>(0));
  assert(size_ != ~static_cast<uint64_t>(0));
  PutVarint64(dst, offset_);
  PutVarint64(dst, size_);
}

Status BlockHandle::DecodeFrom(Slice* input) {
  if (GetVarint64(input, &offset_) && GetVarint64(input, &size_)) {
    return Status::OK();
  } else {
    return Status::Corruption("bad block handle");
  }
}

void Footer::EncodeTo(std::string* dst) const {
  const size_t original_size = dst->size();
  metaindex_handle_.EncodeTo(dst);
  index_handle_.EncodeTo(dst);
  dst->resize(2 * BlockHandle::kMaxEncodedLength);  // Padding
  PutFixed32(dst, static_cast<uint32_t>(kTableMagicNumber & 0xffffffffu));
  PutFixed32(dst, static_cast<uint32_t>(kTableMagicNumber >> 32));
  assert(dst->size() == original_size + kEncodedLength);
  (void)original_size;  // Disable unused variable warning.
}

Status Footer::DecodeFrom(Slice* input) {
  const char* magic_ptr = input->data() + kEncodedLength - 8;
  const uint32_t magic_lo = DecodeFixed32(magic_ptr);
  const uint32_t magic_hi = DecodeFixed32(magic_ptr + 4);
  const uint64_t magic = ((static_cast<uint64_t>(magic_hi) << 32) |
                          (static_cast<uint64_t>(magic_lo)));
  if (magic != kTableMagicNumber) {
    return Status::Corruption("not an sstable (bad magic number)");
  }

  Status result = metaindex_handle_.DecodeFrom(input);
  if (result.ok()) {
    result = index_handle_.DecodeFrom(input);
  }
  if (result.ok()) {
    // We skip over any leftover data (just padding for now) in "input"
    const char* end = magic_ptr + 8;
    *input = Slice(end, input->data() + input->size() - end);
  }
  return result;
}
void Find_Remote_MR(std::map<uint32_t, ibv_mr*>* remote_data_blocks,
                    const BlockHandle& handle, ibv_mr* remote_mr) {
  uint64_t  position = handle.offset();
//  auto iter = remote_data_blocks.begin();
  auto iter = remote_data_blocks->upper_bound(position);
  position = position - (iter->first - iter->second->length);
//  assert(position + handle.size() + kBlockTrailerSize <= iter->second->length);
//  assert(handle.size() +kBlockTrailerSize <= )
  *(remote_mr) = *(iter->second);
//      DEBUG_arg("Block buffer position %lu\n", position);
  remote_mr->addr = static_cast<void*>(static_cast<char*>(iter->second->addr) + position);

}

void Find_Local_MR(std::map<uint32_t, ibv_mr*>* remote_data_blocks,
                    const BlockHandle& handle, Slice& data) {
  uint64_t  position = handle.offset();
  //  auto iter = remote_data_blocks.begin();
  auto iter = remote_data_blocks->upper_bound(position);
  position = position - (iter->first - iter->second->length);
//  assert(position + handle.size() + kBlockTrailerSize <= iter->second->length);
  //  assert(handle.size() +kBlockTrailerSize <= )
  //      DEBUG_arg("Block buffer position %lu\n", position);
  data.Reset((static_cast<char*>(iter->second->addr) + position), handle.size());
}
bool Find_prefetch_MR(std::map<uint32_t, ibv_mr*>* remote_data_blocks,
                      const size_t& offset , ibv_mr* remote_mr) {
  uint64_t  position = offset;
  //  auto iter = remote_data_blocks.begin();
  auto iter = remote_data_blocks->upper_bound(position);
  if (iter == remote_data_blocks->end()){
    return false;
  }
  position = position - (iter->first - iter->second->length);
  assert(position  <= iter->second->length - 8);
  //  assert(handle.size() +kBlockTrailerSize <= )
  *(remote_mr) = *(iter->second);
  //      DEBUG_arg("Block buffer position %lu\n", position);
  remote_mr->addr = static_cast<void*>(static_cast<char*>(iter->second->addr) + position);
  remote_mr->length = remote_mr->length - position;
  return true;
}
//TODO: Make the block mr searching and creating outside this function, so that datablock is
// the same as data index block and filter block.
Status ReadDataBlock(std::map<uint32_t, ibv_mr*>* remote_data_blocks,
                     const ReadOptions& options, const BlockHandle& handle,
                     BlockContents* result, uint8_t target_node_id) {

  //TODO: Make it use thread local read buffer rather than allocate one every time.
//#ifdef GETANALYSIS
//  auto start = std::chrono::high_resolution_clock::now();
//#endif
  result->data = Slice();
//  result->cachable = false;
//  result->heap_allocated = false;
  // Read the block contents as well as the type/crc footer.
  // See table_builder.cc for the code that built this structure.
  Status s = Status::OK();

  std::shared_ptr<RDMA_Manager> rdma_mg = Env::Default()->rdma_mg;

  size_t n = static_cast<size_t>(handle.size());
  assert(n + kBlockTrailerSize <= rdma_mg->name_to_chunksize.at(DataChunk));
  ibv_mr* contents;
  ibv_mr remote_mr = {};
//#ifndef NDEBUG
//  ibv_wc wc;
//  int check_poll_number =
//      rdma_mg->try_poll_completions(&wc, 1, "read_local");
//  assert( check_poll_number == 0);
//#endif

//  if (){

#ifdef GETANALYSIS
  auto start1 = std::chrono::high_resolution_clock::now();
#endif
  Find_Remote_MR(remote_data_blocks, handle, &remote_mr);
#ifdef GETANALYSIS
  auto stop1 = std::chrono::high_resolution_clock::now();
  auto duration1 = std::chrono::duration_cast<std::chrono::nanoseconds>(stop1 - start1);
  RDMA_Manager::RDMAFindmrElapseSum.fetch_add(duration1.count());
#endif
#ifdef GETANALYSIS
  start1 = std::chrono::high_resolution_clock::now();
#endif
//    rdma_mg->Allocate_Local_RDMA_Slot(contents, DataChunk);
  //Need to fix here
    contents = rdma_mg->Get_local_read_mr();
#ifdef GETANALYSIS
  stop1 = std::chrono::high_resolution_clock::now();
  duration1 = std::chrono::duration_cast<std::chrono::nanoseconds>(stop1 - start1);
  RDMA_Manager::RDMAMemoryAllocElapseSum.fetch_add(duration1.count());
  RDMA_Manager::ReadCount1.fetch_add(1);
#endif
#ifdef PROCESSANALYSIS
  auto start = std::chrono::high_resolution_clock::now();
#endif
  rdma_mg->RDMA_Read(&remote_mr, contents, n + kBlockTrailerSize, "read_local",
                     IBV_SEND_SIGNALED, 1, target_node_id);
#ifdef PROCESSANALYSIS
  auto stop = std::chrono::high_resolution_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
  assert(n + kBlockTrailerSize <= rdma_mg->name_to_chunksize.at(DataChunk));
  RDMA_Manager::RDMAReadTimeElapseSum.fetch_add(duration.count());
  RDMA_Manager::ReadCount.fetch_add(1);
#endif
    //
//  }else{
//    s = Status::Corruption("Remote memtable out of buffer");
//  }
//#ifndef NDEBUG
//  usleep(100);
//  check_poll_number = rdma_mg->try_poll_completions(&wc,1);
//  assert( check_poll_number == 0);
//#endif
  // Check the crc of the type and the block contents
//#ifdef GETANALYSIS
//  auto start = std::chrono::high_resolution_clock::now();
//#endif
  // create a new buffer and copy to the new buffer. However, there could still
  // be a contention at the c++ allocator.
  char* data = new char[rdma_mg->name_to_chunksize.at(DataChunk)];

  memcpy(data, contents->addr, rdma_mg->name_to_chunksize.at(DataChunk));
//  printf("/Create buffer for cache, start addr %p, length %lu content is %s\n", data, n, data+1);

//  const char* data = static_cast<char*>(contents->addr);  // Pointer to where Read put the data
  if (options.verify_checksums) {
    const uint32_t crc = crc32c::Unmask(DecodeFixed32(data + n + 1));
    const uint32_t actual = crc32c::Value(data, n + 1);
    if (actual != crc) {
//      delete[] buf;
      DEBUG("Data block Checksum mismatch\n");
      assert(false);
      s = Status::Corruption("block checksum mismatch");
      return s;
    }
  }
//  printf("data[n] is %c\n", data[n]);
  switch (data[n]) {
    case kNoCompression:
        result->data = Slice(data, n);
//        result->heap_allocated = false;
//        result->cachable = false;  // Do not double-table_cache


      // Ok
        break;
//    case kSnappyCompression: {
//      size_t ulength = 0;
//      if (!port::Snappy_GetUncompressedLength(data, n, &ulength)) {
//        rdma_mg->Deallocate_Local_RDMA_Slot(data, "DataBlock")
//        return Status::Corruption("corrupted compressed block contents");
//      }
//      char* ubuf = new char[ulength];
//      if (!port::Snappy_Uncompress(data, n, ubuf)) {
//        delete[] buf;
////        delete[] ubuf;
//        return Status::Corruption("corrupted compressed block contents");
//      }
//      delete[] buf;
//      result->data = Slice(ubuf, ulength);
//      result->heap_allocated = true;
//      result->cachable = true;
//      break;
//    }
    default:
      assert(data[n] != kNoCompression);
      assert(false);
//      rdma_mg->Deallocate_Local_RDMA_Slot(static_cast<void*>(const_cast<char *>(data)), DataChunk);
      DEBUG("Data block illegal compression type\n");
      return Status::Corruption("bad block type");
  }
//#ifdef GETANALYSIS
//  auto stop = std::chrono::high_resolution_clock::now();
//  auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
//  printf("CRC check elapse is %zu\n",  duration.count());
//#endif
  return Status::OK();
}
Status ReadKVPair(std::map<uint32_t, ibv_mr*>* remote_data_blocks,
                  const ReadOptions& options, const BlockHandle& handle,
                  Slice* result, uint8_t target_node_id) {
  //#ifdef GETANALYSIS
  //  auto start = std::chrono::high_resolution_clock::now();
  //#endif
  //  result->cachable = false;
  //  result->heap_allocated = false;
  // Read the block contents as well as the type/crc footer.
  // See table_builder.cc for the code that built this structure.
  Status s = Status::OK();

  std::shared_ptr<RDMA_Manager> rdma_mg = Env::Default()->rdma_mg;

  size_t n = static_cast<size_t>(handle.size());
  assert(n + kBlockTrailerSize <= rdma_mg->name_to_chunksize.at(DataChunk));
//  ibv_mr contents = {};
  ibv_mr* contents;
  ibv_mr remote_mr = {};
  //#ifndef NDEBUG
  //  ibv_wc wc;
  //  int check_poll_number =
  //      rdma_mg->try_poll_completions(&wc, 1, "read_local");
  //  assert( check_poll_number == 0);
  //#endif

  //  if (){

#ifdef GETANALYSIS
  auto start1 = std::chrono::high_resolution_clock::now();
#endif
  Find_Remote_MR(remote_data_blocks, handle, &remote_mr);
#ifdef GETANALYSIS
  auto stop1 = std::chrono::high_resolution_clock::now();
  auto duration1 = std::chrono::duration_cast<std::chrono::nanoseconds>(stop1 - start1);
  RDMA_Manager::RDMAFindmrElapseSum.fetch_add(duration1.count());
#endif
#ifdef GETANALYSIS
  start1 = std::chrono::high_resolution_clock::now();
#endif
//  rdma_mg->Allocate_Local_RDMA_Slot(contents, DataChunk);
  contents = rdma_mg->Get_local_read_mr();
//  contents = (ibv_mr*)rdma_mg->read_buffer->Get();
//  if (contents == nullptr){
//
//  }
#ifdef GETANALYSIS
  stop1 = std::chrono::high_resolution_clock::now();
  duration1 = std::chrono::duration_cast<std::chrono::nanoseconds>(stop1 - start1);
  RDMA_Manager::RDMAMemoryAllocElapseSum.fetch_add(duration1.count());
  RDMA_Manager::ReadCount1.fetch_add(1);
#endif
#ifdef PROCESSANALYSIS
  auto start = std::chrono::high_resolution_clock::now();
#endif
  rdma_mg->RDMA_Read(&remote_mr, contents, n, "read_local", IBV_SEND_SIGNALED,
                     1, target_node_id);
#ifdef PROCESSANALYSIS
  auto stop = std::chrono::high_resolution_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
  assert(n  <= rdma_mg->name_to_chunksize.at(DataChunk));
  RDMA_Manager::RDMAReadTimeElapseSum.fetch_add(duration.count());
  RDMA_Manager::ReadCount.fetch_add(1);
#endif

  const char* data = static_cast<char*>(contents->addr);  // Pointer to where Read put the data

  result->Reset(data, n);


  //#ifdef GETANALYSIS
  //  auto stop = std::chrono::high_resolution_clock::now();
  //  auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
  //  printf("CRC check elapse is %zu\n",  duration.count());
  //#endif
  return Status::OK();
}
Status ReadDataIndexBlock(ibv_mr* remote_mr, const ReadOptions& options,
                          BlockContents* result, uint8_t target_node_id) {
  result->data = Slice();
//  result->cachable = false;
//  result->heap_allocated = false;
  // Read the block contents as well as the type/crc footer.
  // See table_builder.cc for the code that built this structure.
  Status s = Status::OK();
  std::shared_ptr<RDMA_Manager> rdma_mg = Env::Default()->rdma_mg;
  size_t n = remote_mr->length - kBlockTrailerSize;
  assert(n>0);
  assert(n + kBlockTrailerSize < rdma_mg->name_to_chunksize.at(IndexChunk));
  ibv_mr contents = {};
  if (remote_mr->length < INDEX_BLOCK_SMALL){
    rdma_mg->Allocate_Local_RDMA_Slot(contents, IndexChunk_Small);

  }else{
    rdma_mg->Allocate_Local_RDMA_Slot(contents, IndexChunk);
  }
  rdma_mg->RDMA_Read(remote_mr, &contents, n + kBlockTrailerSize, "read_local",
                     IBV_SEND_SIGNALED, 1, target_node_id);

//  printf("Fetch a Index Block");

  // Check the crc of the type and the block contents
  const char* data = static_cast<char*>(contents.addr);  // Pointer to where Read put the data
  if (options.verify_checksums) {
    const uint32_t crc = crc32c::Unmask(DecodeFixed32(data + n + 1));
    const uint32_t actual = crc32c::Value(data, n + 1);
    if (actual != crc) {
//      delete[] buf;
      DEBUG("Index block Checksum mismatch\n");
      assert(false);
      s = Status::Corruption("block checksum mismatch");
      return s;
    }else{
//      DEBUG_arg("SSTable %p open successfully\n", remote_mr->addr);
//      DEBUG_arg("CRC IS %d\n", crc);
//      DEBUG_arg("Actual IS %d\n", actual);
    }
  }

  switch (data[n]) {
    case kNoCompression:
      //block content do not contain compression type and check sum
      result->data = Slice(data, n);
//      result->heap_allocated = false;
//      result->cachable = false;  // Do not double-table_cache


      // Ok
      break;
//    case kSnappyCompression: {
//      size_t ulength = 0;
//      if (!port::Snappy_GetUncompressedLength(data, n, &ulength)) {
//        rdma_mg->Deallocate_Local_RDMA_Slot(data, "DataBlock")
//        return Status::Corruption("corrupted compressed block contents");
//      }
//      char* ubuf = new char[ulength];
//      if (!port::Snappy_Uncompress(data, n, ubuf)) {
//        delete[] buf;
////        delete[] ubuf;
//        return Status::Corruption("corrupted compressed block contents");
//      }
//      delete[] buf;
//      result->data = Slice(ubuf, ulength);
//      result->heap_allocated = true;
//      result->cachable = true;
//      break;
//    }
    default:
//      rdma_mg->Deallocate_Local_RDMA_Slot(static_cast<void*>(const_cast<char *>(data)), DataChunk);
      DEBUG("index block illegal compression type\n");
      return Status::Corruption("bad block type");
  }

  return Status::OK();
}
Status ReadFilterBlock(ibv_mr* remote_mr, const ReadOptions& options,
                       BlockContents* result, uint8_t target_node_id) {
  result->data = Slice();
//  result->cachable = false;
//  result->heap_allocated = false;
  // Read the block contents as well as the type/crc footer.
  // See table_builder.cc for the code that built this structure.
  Status s = Status::OK();
  std::shared_ptr<RDMA_Manager> rdma_mg = Env::Default()->rdma_mg;
  size_t n = remote_mr->length - kBlockTrailerSize;
  assert(n + kBlockTrailerSize < rdma_mg->name_to_chunksize.at(FilterChunk));
  ibv_mr contents = {};
  rdma_mg->Allocate_Local_RDMA_Slot(contents, FilterChunk);
  rdma_mg->RDMA_Read(remote_mr, &contents, n + kBlockTrailerSize, "read_local",
                     IBV_SEND_SIGNALED, 1, target_node_id);

  // Check the crc of the type and the block contents
  const char* data = static_cast<char*>(contents.addr);  // Pointer to where Read put the data
  if (options.verify_checksums) {
    const uint32_t crc = crc32c::Unmask(DecodeFixed32(data + n + 1));
    const uint32_t actual = crc32c::Value(data, n + 1);
    if (actual != crc) {
//      delete[] buf;
      DEBUG("Filter Checksum mismatch\n");
//      assert(false);
      s = Status::Corruption("block checksum mismatch");
      return s;
    }
  }

  switch (data[n]) {
    case kNoCompression:
      result->data = Slice(data, n);
//      result->heap_allocated = false;
//      result->cachable = false;  // Do not double-table_cache


      // Ok
      break;
//    case kSnappyCompression: {
//      size_t ulength = 0;
//      if (!port::Snappy_GetUncompressedLength(data, n, &ulength)) {
//        rdma_mg->Deallocate_Local_RDMA_Slot(data, "DataBlock")
//        return Status::Corruption("corrupted compressed block contents");
//      }
//      char* ubuf = new char[ulength];
//      if (!port::Snappy_Uncompress(data, n, ubuf)) {
//        delete[] buf;
////        delete[] ubuf;
//        return Status::Corruption("corrupted compressed block contents");
//      }
//      delete[] buf;
//      result->data = Slice(ubuf, ulength);
//      result->heap_allocated = true;
//      result->cachable = true;
//      break;
//    }
    default:
//      rdma_mg->Deallocate_Local_RDMA_Slot(static_cast<void*>(const_cast<char *>(data)), DataChunk);
      DEBUG("Filter illegal compression type\n");
      return Status::Corruption("bad block type");
  }
  assert(result->data.size() != 0);
  return Status::OK();
}
}  // namespace TimberSaw
