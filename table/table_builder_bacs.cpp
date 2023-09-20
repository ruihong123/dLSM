//
// Created by ruihong on 1/20/22.
//

#include "table_builder_bacs.h"
#include "db/dbformat.h"
#include <cassert>
namespace TimberSaw {
//TOthink: how to save the remote mr?
//TOFIX : now we suppose the index and filter block will not over the write buffer.
// TODO: make the Option of tablebuilder a pointer avoiding large data copying
struct TableBuilder_BACS::Rep {
  Rep(const Options& opt, IO_type type, uint8_t target_node_id)
      : options(opt),
        index_block_options(opt),
        type_(type),
        offset_last_flushed(0),
        offset(0),

        num_entries(0),
        closed(false),
        pending_index_filter_entry(false),
        target_node_id_(target_node_id)
  {
    //TOTHINK: why the block restart interval is 1 by default?
    // This is only for index block, is it the same for rocks DB?
    index_block_options.block_restart_interval = 1;
    std::shared_ptr<RDMA_Manager> rdma_mg = options.env->rdma_mg;
    ibv_mr* temp_data_mr = new ibv_mr();
    ibv_mr* temp_index_mr = new ibv_mr();
    ibv_mr* temp_filter_mr = new ibv_mr();
    //first create two buffer for each slot.
    rdma_mg->Allocate_Local_RDMA_Slot(*temp_data_mr, FlushBuffer);
    rdma_mg->Allocate_Local_RDMA_Slot(*temp_index_mr, IndexChunk);
    rdma_mg->Allocate_Local_RDMA_Slot(*temp_filter_mr, FilterChunk);
    local_data_mr.push_back(temp_data_mr);
    local_index_mr.push_back(temp_index_mr);
    memset(temp_filter_mr->addr, 0, temp_filter_mr->length);
    local_filter_mr.push_back(temp_filter_mr);
    temp_data_mr = new ibv_mr();
    temp_index_mr = new ibv_mr();
    temp_filter_mr = new ibv_mr();
    rdma_mg->Allocate_Local_RDMA_Slot(*temp_data_mr, FlushBuffer);
    rdma_mg->Allocate_Local_RDMA_Slot(*temp_index_mr, IndexChunk);
    rdma_mg->Allocate_Local_RDMA_Slot(*temp_filter_mr, FilterChunk);
    local_data_mr.push_back(temp_data_mr);
    local_index_mr.push_back(temp_index_mr);
    memset(temp_filter_mr->addr, 0, temp_filter_mr->length);
    local_filter_mr.push_back(temp_filter_mr);
    //    delete temp_data_mr;
    //    delete temp_index_mr;
    //    delete temp_filter_mr;
    data_buff = Slice((char*)local_data_mr.at(0)->addr,0);
    index_block = new BlockBuilder(&index_block_options, local_index_mr[0]);
    if (type_ == IO_type::Compact){
      type_string_ = "write_local_compact";
    }else if(type_ == IO_type::Flush){
      type_string_ = "write_local_flush";
    }else{
      assert(false);
    }
    filter_block = (opt.filter_policy == nullptr
                        ? nullptr
                        : new FullFilterBlockBuilder(local_filter_mr[0], opt.bloom_bits));

    status = Status::OK();
  }

  const Options& options;
  Options index_block_options;
  IO_type type_;
  std::string type_string_;
  //  WritableFile* file;
  std::vector<ibv_mr*> local_data_mr;
  // the start index of the in use buffer
  int data_inuse_start = -1;
  // the end index of the use buffer
  int data_inuse_end = -1;
  // when start larger than end by 1, there could be two scenarios:
  // First, all the buffer are outstanding
  // second, no buffer is outstanding, those two status will both have start - end = 1
  bool data_inuse_empty = true;
  std::vector<ibv_mr*> local_index_mr;
  std::vector<ibv_mr*> local_filter_mr;
  //TODO: make the map offset -> ibv_mr*
  std::map<uint32_t, ibv_mr*> remote_data_mrs;
  std::map<uint32_t, ibv_mr*> remote_dataindex_mrs;
  std::map<uint32_t, ibv_mr*> remote_filter_mrs;
  //  std::vector<size_t> remote_mr_real_length;
  uint64_t offset_last_flushed = 0;
  uint64_t offset_last_added = 0;

  uint64_t offset = 0;
  uint32_t chunk_offset = 0;

  Status status;
//  BlockBuilder* data_block;
  Slice data_buff;
  BlockBuilder* index_block;
  std::string last_key;
  int64_t num_entries;
  bool closed;  // Either Finish() or Abandon() has been called.
  FullFilterBlockBuilder* filter_block;

  // We do not emit the index entry for a block until we have seen the
  // first key for the next data block.  This allows us to use shorter
  // keys in the index block.  For example, consider a block boundary
  // between the keys "the quick brown fox" and "the who".  We can use
  // "the r" as the key for the index block entry since it is >= all
  // entries in the first block and < all entries in subsequent
  // blocks.
  //
  // Invariant: r->pending_index_filter_entry is true only if data_block is empty.
  bool pending_index_filter_entry;
  BlockHandle pending_data_handle;  // Handle to add to index block

  std::string compressed_output;
  uint8_t target_node_id_;
};
TableBuilder_BACS::TableBuilder_BACS(const Options& options, IO_type type,
                                     uint8_t target_node_id)
    : rep_(new Rep(options, type, target_node_id)) {
  if (rep_->filter_block != nullptr) {
    rep_->filter_block->RestartBlock(0);
  }
}

TableBuilder_BACS::~TableBuilder_BACS() {
  assert(rep_->closed);  // Catch errors where caller forgot to call Finish()
  if (rep_->filter_block != nullptr){
    delete rep_->filter_block;
  }
  std::shared_ptr<RDMA_Manager> rdma_mg = rep_->options.env->rdma_mg;
  for(auto iter : rep_->local_data_mr){
    rdma_mg->Deallocate_Local_RDMA_Slot(iter->addr, FlushBuffer);
    delete iter;
  }
  for(auto iter : rep_->local_index_mr){
    rdma_mg->Deallocate_Local_RDMA_Slot(iter->addr, IndexChunk);
    delete iter;
  }
  for(auto iter : rep_->local_filter_mr){
    rdma_mg->Deallocate_Local_RDMA_Slot(iter->addr, FilterChunk);
    delete iter;
  }
//  delete rep_->data_block;
  delete rep_->index_block;
  delete rep_;
}

//Status TableBuilder::ChangeOptions(const Options& options) {
//  // Note: if more fields are added to Options, update
//  // this function to catch changes that should not be allowed to
//  // change in the middle of building a Table.
//  if (options.comparator != rep_->options.comparator) {
//    return Status::InvalidArgument("changing comparator while building table");
//  }
//
//  // Note that any live BlockBuilders point to rep_->options and therefore
//  // will automatically pick up the updated options.
//  rep_->options = options;
//  rep_->index_block_options = options;
//  rep_->index_block_options.block_restart_interval = 1;
//  return Status::OK();
//}
//TODO: make it create a block every blocksize, flush every 1M. When flushing do not poll completion
// pool the completion at the same time in the end
void TableBuilder_BACS::Add(const Slice& key, const Slice& value) {
  Rep* r = rep_;
  assert(!r->closed);
  if (!ok()) return;
  if (r->num_entries > 0) {
    assert(r->options.comparator->Compare(key, Slice(r->last_key)) > 0);
  }
  //  DEBUG_arg("ADD new key data, key is %s\n", key.ToString().c_str());
  //  DEBUG_arg("number of entry is %ld\n", r->num_entries);
  //todo: MAKE IT a remote block size which could be 1M
  // How to predict the datasize before actually serilizae the data, so that there will
  // not be buffer overflow.
  // First, predict whether the block will be full
  // *   if so then finish the old data to a block make it insert to a new block
  // *   Second, if new block finished, check whether the write buffer can hold a new block size.
  // *           if not, the flush temporal buffer content to the remote memory.


  if ((r->offset - r->offset_last_flushed + key.size() + value.size() + 2*sizeof(uint32_t)) >  r->local_data_mr[0]->length) {
    FlushData();// reset the buffer inside

  }
//  const size_t estimated_block_size = r->data_block->CurrentSizeEstimate();
//  if (estimated_block_size + key.size() + value.size() +sizeof(size_t) + kBlockTrailerSize >= r->options.block_size) {
//
//  }

  //Create a new index entry for every items.
  {

    // Add to index block from the second data, and when finishing the index block
    // add the smallest successor of the last key.
    std::string handle_encoding;
    //This index point to this offset and the key is this key.
    r->pending_data_handle.set_offset(r->offset);// This is the offset of the begginning of this block.
    r->pending_data_handle.set_size(key.size() + value.size() + 2*sizeof(uint32_t));
    r->pending_data_handle.EncodeTo(&handle_encoding);

    r->index_block->Add(key, Slice(handle_encoding));



  }

  if (r->filter_block != nullptr) {
    r->filter_block->AddKey(ExtractUserKey(key));
  }

  r->last_key.assign(key.data(), key.size());
  //  assert(key.size() == 28 || key.size() == 29);
  //  assert(r->last_key.c_str()[8] == 060);
  r->num_entries++;
  // append k-V pair to the buffer.
  PutFixed32(&r->data_buff, key.size());
  PutFixed32(&r->data_buff, value.size());
  r->data_buff.append(key.data(), key.size());
  r->data_buff.append(value.data(), value.size());
  r->offset_last_added = r->offset;
  r->offset +=  key.size() + value.size() + 2*sizeof(uint32_t);



}

void TableBuilder_BACS::UpdateFunctionBLock() {

//  Rep* r = rep_;
//  assert(!r->closed);
//  if (!ok()) return;
//  if (r->data_block->empty()) return;
//  assert(!r->pending_index_filter_entry);
//  FinishDataBlock(r->data_block, &r->pending_data_handle, r->options.compression);
//  //set data block pointer to next one, clear the block state
//  //  r->data_block->Reset();
//  if (ok()) {
//    r->pending_index_filter_entry = true;
//    //    r->status = r->file->FlushData();
//  }

}
//Note: there are three types of finish function for different blocks, the main
//difference is whether update the offset which will record the size of the data block.
//And the filter blocks has a different way to reset the block.
void TableBuilder_BACS::FinishDataBlock(BlockBuilder* block, BlockHandle* handle,
                                               CompressionType compressiontype) {
  // File format contains a sequence of blocks where each block has:
  //    block_data: uint8[n]
  //    compressiontype: uint8
  //    crc: uint32
  assert(false);
  Rep* r = rep_;
  block->Finish();

//  Slice* raw = &(r->data_block->buffer);
  Slice* raw ;

  Slice* block_contents;
  //  CompressionType compressiontype = r->options.compression;
  //TOTHINK: temporally disable the compression, because it can increase the latency but it could
  // increase the available bandwidth. THis part depends on whether the in-memory write can catch
  // up with the high RDMA bandwidth.
  switch (compressiontype) {
    case kNoCompression:
      block_contents = raw;
      break;

      //    case kSnappyCompression: {
      //      std::string* compressed = &r->compressed_output;
      //      if (port::Snappy_Compress(raw->data(), raw->size(), compressed) &&
      //          compressed->size() < raw->size() - (raw->size() / 8u)) {
      //        block_contents = *compressed;
      //        memcpy(block_contents.data);
      //      } else {
      //        // Snappy not supported, or compressed less than 12.5%, so just
      //        // store uncompressed form
      //        block_contents = raw;
      //        compressiontype = kNoCompression;
      //      }
      //      break;
      //    }
  }
  //#ifndef NDEBUG
  //  if (r->offset == 72100)
  //    printf("mark!!\n");
  //#endif
  handle->set_offset(r->offset);// This is the offset of the begginning of this block.
  handle->set_size(block_contents->size());
  assert(block_contents->size() <= r->options.block_size - kBlockTrailerSize);
  if (r->status.ok()) {
    char trailer[kBlockTrailerSize];
    trailer[0] = compressiontype;
    uint32_t crc = crc32c::Value(block_contents->data(), block_contents->size());
    crc = crc32c::Extend(crc, trailer, 1);  // Extend crc to cover block compressiontype
    EncodeFixed32(trailer + 1, crc32c::Mask(crc));
    block_contents->append(trailer, kBlockTrailerSize);
    // block_type == 0 means data block
    if (r->status.ok()) {
      r->offset += block_contents->size();
      //      DEBUG_arg("Offset is %lu", r->offset);
      assert(r->offset - r->offset_last_flushed <= r->local_data_mr[0]->length);
    }
  }
  r->compressed_output.clear();
  block->Reset_Forward();
}
void TableBuilder_BACS::FinishDataIndexBlock(BlockBuilder* block,
                                                    BlockHandle* handle,
                                                    CompressionType compressiontype,
                                                    size_t& block_size) {
  // File format contains a sequence of blocks where each block has:
  //    block_data: uint8[n]
  //    compressiontype: uint8
  //    crc: uint32
  assert(ok());
  Rep* r = rep_;
  block->Finish();

  Slice* raw = &(r->index_block->buffer);
  Slice* block_contents;
  switch (compressiontype) {
    case kNoCompression:
      block_contents = raw;
      break;

      //    case kSnappyCompression: {
      //      std::string* compressed = &r->compressed_output;
      //      if (port::Snappy_Compress(raw.data(), raw.size(), compressed) &&
      //          compressed->size() < raw.size() - (raw.size() / 8u)) {
      //        block_contents = *compressed;
      //      } else {
      //        // Snappy not supported, or compressed less than 12.5%, so just
      //        // store uncompressed form
      //        block_contents = raw;
      //        compressiontype = kNoCompression;
      //      }
      //      break;
      //    }
  }
  handle->set_offset(r->offset);
  handle->set_size(block_contents->size());
  if (r->status.ok()) {
    char trailer[kBlockTrailerSize];
    trailer[0] = compressiontype;
    uint32_t crc = crc32c::Value(block_contents->data(), block_contents->size());
    crc = crc32c::Extend(crc, trailer, 1);  // Extend crc to cover block compressiontype
    EncodeFixed32(trailer + 1, crc32c::Mask(crc));
    block_contents->append(trailer, kBlockTrailerSize);
  }
  assert(block_contents->size() <= r->local_index_mr.at(0)->length);
  r->compressed_output.clear();
  block_size = block_contents->size();
  DEBUG_arg("index block size: %zu \n", block_size);
#ifndef NDEBUG

  printf(" start of the this block is");
  for (int i = 0; i < 30; ++i) {
    printf("%o, ", *(unsigned char*)(block_contents->data()+i));
  }
  printf("\n");
//  char c = 255;
//  printf("a full character %o", )
#endif
  block->Reset_Forward();

}
void TableBuilder_BACS::FinishFilterBlock(FullFilterBlockBuilder* block, BlockHandle* handle,
                                                 CompressionType compressiontype,
                                                 size_t& block_size) {
  Rep* r = rep_;
  block->Finish();

  Slice* raw = &(r->filter_block->result);
  assert(raw->size()!=0);
  Slice* block_contents;
  switch (compressiontype) {
    case kNoCompression:
      block_contents = raw;
      break;

      //    case kSnappyCompression: {
      //      std::string* compressed = &r->compressed_output;
      //      if (port::Snappy_Compress(raw.data(), raw.size(), compressed) &&
      //          compressed->size() < raw.size() - (raw.size() / 8u)) {
      //        block_contents = *compressed;
      //      } else {
      //        // Snappy not supported, or compressed less than 12.5%, so just
      //        // store uncompressed form
      //        block_contents = raw;
      //        compressiontype = kNoCompression;
      //      }
      //      break;
      //    }
  }
  handle->set_offset(r->offset);
  handle->set_size(block_contents->size());
  if (r->status.ok()) {
    char trailer[kBlockTrailerSize];
    trailer[0] = compressiontype;
    uint32_t crc = crc32c::Value(block_contents->data(), block_contents->size());
    crc = crc32c::Extend(crc, trailer, 1);  // Extend crc to cover block compressiontype
    EncodeFixed32(trailer + 1, crc32c::Mask(crc));
    block_contents->append(trailer, kBlockTrailerSize);
    assert(block_contents->size() <= r->local_filter_mr[0]->length);
  }
  r->compressed_output.clear();
  block_size = block_contents->size();
  block->Reset();
}
//TODO make flushing flush the data to the remote memory flushing to remote memory
void TableBuilder_BACS::FlushData(){
  Rep* r = rep_;
  size_t msg_size = r->offset - r->offset_last_flushed;
  ibv_mr* remote_mr = new ibv_mr();
  std::shared_ptr<RDMA_Manager> rdma_mg =  r->options.env->rdma_mg;
  rdma_mg->Allocate_Remote_RDMA_Slot(*remote_mr, rep_->target_node_id_,
                                     FlushBuffer);
  //TOTHINK: check the logic below.
  // I was thinking that the start represent the oldest outgoing buffer, while the
  // end is the latest buffer. When polling a result, the start index will moving forward
  // and the start index will be changed to 0 when meeting the end of this index.
  // If the start index meet the end index, then we need to allocate a new buffer,
  // and insert to the local buffer vector.
  // the end index will move forward because a new buffer will be in used, the start
  // index will also increased by 1 because the insert will cause all the index
  // after that position to be increased by 1.
  if (r->data_inuse_start == -1){
    // first time flush
    assert(r->data_inuse_end == -1 && r->local_data_mr.size() == 2);
    rdma_mg->RDMA_Write(remote_mr, r->local_data_mr[0], msg_size,
                        r->type_string_, IBV_SEND_SIGNALED,
                        0, rep_->target_node_id_);
    r->data_inuse_end = 0;
    r->data_inuse_start = 0;
    r->data_inuse_empty = false;

  }else{
    // check the maximum outstanding buffer number, if not set it the flushing and compaction will be messed up
    //    int maximum_poll_number = r->data_inuse_end - r->data_inuse_start + 1 >= 0 ?
    //                      r->data_inuse_end - r->data_inuse_start + 1:
    //                      (int)(r->local_data_mr.size()) - r->data_inuse_start + r->data_inuse_end +1;
    int maximum_poll_number = 5;
    auto* wc = new ibv_wc[maximum_poll_number];
    int poll_num = 0;
    poll_num = rdma_mg->try_poll_completions(
        wc, maximum_poll_number, r->type_string_, true, rep_->target_node_id_);
    // move the start index
    r->data_inuse_start += poll_num;
    if(r->data_inuse_start >= r->local_data_mr.size()){
      r->data_inuse_start = r->data_inuse_start - r->local_data_mr.size();
    }
    //    DEBUG_arg("Poll the completion %d\n", poll_num);

    //move forward the end of the outstanding buffer
    r->data_inuse_end = r->data_inuse_end == r->local_data_mr.size()-1 ? 0:r->data_inuse_end+1;
    rdma_mg->RDMA_Write(remote_mr, r->local_data_mr[r->data_inuse_end],
                        msg_size, r->type_string_, IBV_SEND_SIGNALED, 0, rep_->target_node_id_);
    //Check whether there is available buffer to serialize the memtable onto,
    // if not allocate a new one and insert it to the vector
    if (r->data_inuse_start - r->data_inuse_end == 1 ||
        r->data_inuse_end - r->data_inuse_start == r->local_data_mr.size()-1){
      ibv_mr* new_local_mr = new ibv_mr();
      rdma_mg->Allocate_Local_RDMA_Slot(*new_local_mr,FlushBuffer);
      if(r->data_inuse_start == 0){// if start is 0 then the insert will also increase the index of end.
        r->data_inuse_end++;
      }
      // insert new mr at start while increase start by 1.
      r->local_data_mr.insert(r->local_data_mr.begin() + r->data_inuse_start++, new_local_mr);
      DEBUG_arg("One more local write buffer is added, now %zu total\n", r->local_data_mr.size());
    }
    delete[] wc;
  }
  remote_mr->length = msg_size;
  //  if(r->remote_data_mrs.empty()){
  //    r->remote_data_mrs.insert({0, remote_mr});
  //  }else{
  //#ifndef NDEBUG
  //    for (auto iter : r->remote_data_mrs) {
  //      assert(remote_mr->addr != iter.second->addr);
  //
  //    }
  //#endif
  //    r->remote_data_mrs.insert({r->remote_data_mrs.rbegin()->first+1, remote_mr});
  //  }
  if(r->remote_data_mrs.empty()){
    r->remote_data_mrs.insert({r->offset, remote_mr});
  }else{
#ifndef NDEBUG
    for (auto iter : r->remote_data_mrs) {
      assert(remote_mr->addr != iter.second->addr);
    }
#endif
    r->remote_data_mrs.insert({r->offset, remote_mr});
  }

  r->offset_last_flushed = r->offset;
  // Move the datablock pointer to the start of the next write buffer, the other state of the data_block
  // has already reseted before
  int next_buffer_index = r->data_inuse_end == r->local_data_mr.size()-1 ? 0:r->data_inuse_end+1;

  assert(next_buffer_index != r->data_inuse_start);
  r->data_buff.Reset((char*)r->local_data_mr[next_buffer_index]->addr, 0);
  //  DEBUG_arg("In use start is %d\n", r->data_inuse_start);
  //  DEBUG_arg("In use end is %d\n", r->data_inuse_end);
  //  DEBUG_arg("Next write buffer to use %d\n", next_buffer_index);
  //  DEBUG_arg("Total local write buffer number is %zu\n", r->local_data_mr.size());
  //  DEBUG_arg("MR element number is %lu\n", r->remote_data_mrs.size());
  //  assert(r->data_inuse_start!= r->data_inuse_end);
  // No need to record the flushing times, because we can check from the remote mr map element number.
}
void TableBuilder_BACS::FlushDataIndex(size_t msg_size) {
  Rep* r = rep_;
  ibv_mr* remote_mr = new ibv_mr();
  std::shared_ptr<RDMA_Manager> rdma_mg =  r->options.env->rdma_mg;
  rdma_mg->Allocate_Remote_RDMA_Slot(*remote_mr, rep_->target_node_id_,
                                     FlushBuffer);
  rdma_mg->RDMA_Write(remote_mr, r->local_index_mr[0], msg_size,
                      r->type_string_, IBV_SEND_SIGNALED, 0, rep_->target_node_id_);
  remote_mr->length = msg_size;
  if(r->remote_dataindex_mrs.empty()){
    r->remote_dataindex_mrs.insert({1, remote_mr});
  }else{
    r->remote_dataindex_mrs.insert({r->remote_dataindex_mrs.rbegin()->first+1, remote_mr});
  }
  //TOFIX: the index may overflow and need to create a new index write buffer, otherwise
  // it would be overwrited.
  //  DEBUG_arg("Index block size is %zu", msg_size);
  r->index_block->Move_buffer(static_cast<char*>(r->local_index_mr[0]->addr));

}
void TableBuilder_BACS::FlushFilter(size_t& msg_size) {
  Rep* r = rep_;
  ibv_mr* remote_mr = new ibv_mr();
  std::shared_ptr<RDMA_Manager> rdma_mg =  r->options.env->rdma_mg;
  rdma_mg->Allocate_Remote_RDMA_Slot(*remote_mr, rep_->target_node_id_,
                                     FilterChunk);
  rdma_mg->RDMA_Write(remote_mr, r->local_filter_mr[0], msg_size,
                      r->type_string_, IBV_SEND_SIGNALED, 0, rep_->target_node_id_);
  remote_mr->length = msg_size;
  if(r->remote_filter_mrs.empty()){
    r->remote_filter_mrs.insert({1, remote_mr});
  }else{
    r->remote_filter_mrs.insert({r->remote_filter_mrs.rbegin()->first+1, remote_mr});
  }
  //TOFIX: the index may overflow and need to create a new index write buffer, otherwise
  // it would be overwrited.
  r->filter_block->Move_buffer(static_cast<char*>(r->local_filter_mr[0]->addr));

}

Status TableBuilder_BACS::status() const { return rep_->status; }

Status TableBuilder_BACS::Finish() {
  Rep* r = rep_;
//  UpdateFunctionBLock();
  if (r->offset - r->offset_last_flushed >0){
    FlushData();
  }

  assert(!r->closed);
  r->closed = true;
  DEBUG_arg("sst offset is %lu\n", r->offset);
  BlockHandle filter_block_handle, metaindex_block_handle, index_block_handle;

  // Write filter block
  //TODO:
  // i found that it could have many bloom filters for each data block but there will be only one filter block.
  // the result will append many bloom filters.
  //TOthink why not compress the block here.
  if (ok() && r->filter_block != nullptr) {
    if(r->pending_index_filter_entry){
      //      r->filter_block->RestartBlock(r->offset);
    }

    size_t msg_size;
    FinishFilterBlock(r->filter_block, &filter_block_handle, kNoCompression, msg_size);
    FlushFilter(msg_size);
//    printf("BloomFilter block size is %zu", msg_size);
  }


  // Write metaindex block
  //  if (ok()) {
  //    BlockBuilder meta_index_block(&r->options, nullptr);
  //    if (r->filter_block != nullptr) {
  //      // Add mapping from "filter.Name" to location of filter data
  //      std::string key = "filter.";
  //      key.append(r->options.filter_policy->Name());
  //      std::string handle_encoding;
  //      filter_block_handle.EncodeTo(&handle_encoding);
  //      meta_index_block->Add(key, handle_encoding);
  //    }
  //
  //    // TODO(postrelease): Add stats and other meta blocks
  //    FinishDataBlock(&meta_index_block, &metaindex_block_handle);
  //  }

  // Write index block
  if (ok()) {
//    if(r->pending_index_filter_entry){
//      r->options.comparator->FindShortSuccessor(&r->last_key);
//      std::string handle_encoding;
//      r->pending_data_handle.set_offset(r->offset_last_added);// This is the offset of the begginning of this block.
//      r->pending_data_handle.set_size(r->offset - r->offset_last_added);
//      r->pending_data_handle.EncodeTo(&handle_encoding);
//      r->index_block->Add(r->last_key, Slice(handle_encoding));
//      r->pending_index_filter_entry = false;
//    }
    size_t msg_size;
    FinishDataIndexBlock(r->index_block, &index_block_handle,
                         kNoCompression, msg_size);
    FlushDataIndex(msg_size);
//    printf("Index block size is %zu", msg_size);
  }
  //  DEBUG_arg("for a sst the remote data chunks number %zu\n", r->remote_data_mrs.size());
  //TODO: the polling number here sometime is not correct.
  int num_of_poll = r->data_inuse_end - r->data_inuse_start + 1 >= 0 ?
                                                                     r->data_inuse_end - r->data_inuse_start + 1:
                                                                     (int)(r->local_data_mr.size()) - r->data_inuse_start + r->data_inuse_end +1;
  // add one more for the index block,if have filter block add 2
  if (r->filter_block != nullptr){
    num_of_poll = num_of_poll + 2;
  }else{
    num_of_poll = num_of_poll + 1;
  }
  ibv_wc wc[num_of_poll];
  r->options.env->rdma_mg->poll_completion(
      wc, num_of_poll, r->type_string_, true,
      rep_->target_node_id_); //it does not matter whether it is true or false
#ifndef NDEBUG
  usleep(10);
  int check_poll_number = r->options.env->rdma_mg->try_poll_completions(
      wc, 1, r->type_string_, true, rep_->target_node_id_);
  assert( check_poll_number == 0);
#endif
  //  printf("A table finsihed flushing\n");
  //  // Write footer
  //  if (ok()) {
  //    Footer footer;
  //    footer.set_metaindex_handle(metaindex_block_handle);
  //    footer.set_index_handle(index_block_handle);
  //    std::string footer_encoding;
  //    footer.EncodeTo(&footer_encoding);
  //    r->status = r->file->Append(footer_encoding);
  //    if (r->status.ok()) {
  //      r->offset += footer_encoding.size();
  //    }
  //  }
  return r->status;
}

void TableBuilder_BACS::Abandon() {
  Rep* r = rep_;
  assert(!r->closed);
  r->closed = true;
}

uint64_t TableBuilder_BACS::NumEntries() const { return rep_->num_entries; }

uint64_t TableBuilder_BACS::FileSize() const { return rep_->offset; }
void TableBuilder_BACS::get_datablocks_map(std::map<uint32_t, ibv_mr*>& map) {
  map = rep_->remote_data_mrs;
}
void TableBuilder_BACS::get_dataindexblocks_map(std::map<uint32_t, ibv_mr*>& map) {
  map = rep_->remote_dataindex_mrs;
}
void TableBuilder_BACS::get_filter_map(std::map<uint32_t, ibv_mr*>& map) {
  map = rep_->remote_filter_mrs;
}
size_t TableBuilder_BACS::get_numentries() {
  return rep_->num_entries;
}


}  // namespace TimberSaw