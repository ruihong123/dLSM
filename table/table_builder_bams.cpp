//
// Created by ruihong on 1/20/22.
//

#include "table_builder_bams.h"
#include "db/dbformat.h"
#include <cassert>
namespace TimberSaw {
struct TableBuilder_BAMS::Rep {
  Rep(const Options& opt, IO_type type, std::shared_ptr<RDMA_Manager> rdma)
      : options(opt),
        type_(type),
        index_block_options(opt),
        offset(0),
//        offset_last_added(0),
        offset_last_flushed(0),
        
        num_entries(0),
        closed(false),
        pending_index_filter_entry(false) {
    //TOTHINK: why the block restart interval is 1 by default?
    // This is only for index block, is it the same for rocks DB?
    index_block_options.block_restart_interval = 1;
    rdma_mg = rdma;
    local_data_mr = new ibv_mr();
    local_index_mr = new ibv_mr();
    local_filter_mr = new ibv_mr();
    //first create two buffer for each slot.
    rdma_mg->Allocate_Local_RDMA_Slot(*local_data_mr, FlushBuffer);
    rdma_mg->Allocate_Local_RDMA_Slot(*local_index_mr, FlushBuffer);
    rdma_mg->Allocate_Local_RDMA_Slot(*local_filter_mr, FlushBuffer);
    memset(local_filter_mr->addr, 0, local_filter_mr->length);

    //    temp_data_mr = new ibv_mr();
    //    temp_index_mr = new ibv_mr();
    //    temp_filter_mr = new ibv_mr();
    //    rdma_mg->Allocate_Local_RDMA_Slot(*temp_data_mr, "FlushBuffer");
    //    rdma_mg->Allocate_Local_RDMA_Slot(*temp_index_mr, "FlushBuffer");
    //    rdma_mg->Allocate_Local_RDMA_Slot(*temp_filter_mr, "FlushBuffer");
    //    local_data_mr.push_back(temp_data_mr);
    //    local_index_mr.push_back(temp_index_mr);
    //    memset(temp_filter_mr->addr, 0, temp_filter_mr->length);
    //    local_filter_mr.push_back(temp_filter_mr);
    //    delete temp_data_mr;
    //    delete temp_index_mr;
    //    delete temp_filter_mr;
    data_buff = Slice((char*)local_data_mr->addr,0);
    index_block = new BlockBuilder(&index_block_options, local_index_mr);
#ifndef NDEBUG
    printf("Sucessfully allocate an block %p", local_index_mr->addr);
#endif
    if (type_ == IO_type::Compact){
      type_string_ = "write_local_compact";
    }else if(type_ == IO_type::Flush){
      type_string_ = "write_local_flush";
    }else{
      assert(false);
    }
    filter_block = (opt.filter_policy == nullptr
                        ? nullptr
                        : new FullFilterBlockBuilder(local_filter_mr, opt.bloom_bits));

    status = Status::OK();
  }

  const Options& options;
  Options index_block_options;
  IO_type type_;
  std::string type_string_;
  std::shared_ptr<RDMA_Manager> rdma_mg;
  //  WritableFile* file;

  // the start index of the in use buffer
  int data_inuse_start = -1;
  // the end index of the use buffer
  int data_inuse_end = -1;
  // when start larger than end by 1, there could be two scenarios:
  // First, all the buffer are outstanding
  // second, no buffer is outstanding, those two status will both have start - end = 1
  bool data_inuse_empty = true;
  //TODO: garbage collect all the local unused MR when destroyingnthis table builder.
  ibv_mr* local_data_mr;
  ibv_mr* local_index_mr;
  ibv_mr* local_filter_mr;
  //TODO: make the map offset -> ibv_mr*
  std::map<uint32_t, ibv_mr*> local_data_mrs;
  std::map<uint32_t, ibv_mr*> local_dataindex_mrs;
  std::map<uint32_t, ibv_mr*> local_filter_mrs;
  //  std::vector<size_t> remote_mr_real_length;
  uint64_t offset_last_flushed;
//  uint64_t offset_last_added;
  uint64_t offset;
  Status status;
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
};
TableBuilder_BAMS::TableBuilder_BAMS(
    const Options& options, IO_type type, std::shared_ptr<RDMA_Manager> rdma_mg)
    :rep_(new TableBuilder_BAMS::Rep(options, type, std::move(rdma_mg))) {
  if (rep_->filter_block != nullptr) {
    rep_->filter_block->RestartBlock(0);
  }
}

TableBuilder_BAMS::~TableBuilder_BAMS() {
  assert(rep_->closed);  // Catch errors where caller forgot to call Finish()
  if (rep_->filter_block != nullptr){
    delete rep_->filter_block;
  }
  rep_->rdma_mg->Deallocate_Local_RDMA_Slot(rep_->local_data_mr->addr, FlushBuffer);
  rep_->rdma_mg->Deallocate_Local_RDMA_Slot(rep_->local_index_mr->addr, FlushBuffer);
  rep_->rdma_mg->Deallocate_Local_RDMA_Slot(rep_->local_filter_mr->addr, FlushBuffer);

  //  std::shared_ptr<RDMA_Manager> rdma_mg = rep_->rdma_mg;
  //  for(auto iter : rep_->local_data_mr){
  //    rdma_mg->Deallocate_Local_RDMA_Slot(iter->addr, "FlushBuffer");
  //    delete iter;
  //  }
  //  for(auto iter : rep_->local_index_mr){
  //    rdma_mg->Deallocate_Local_RDMA_Slot(iter->addr, "FlushBuffer");
  //    delete iter;
  //  }
  //  for(auto iter : rep_->local_filter_mr){
  //    rdma_mg->Deallocate_Local_RDMA_Slot(iter->addr, "FlushBuffer");
  //    delete iter;
  //  }
//  delete rep_->data_block;
  delete rep_->index_block;
  delete rep_;
}

//Status TableBuilder_BAMS::ChangeOptions(const Options& options) {
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
void TableBuilder_BAMS::Add(const Slice& key, const Slice& value) {
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
  if ((r->offset - r->offset_last_flushed + key.size() + value.size() + 2*sizeof(uint32_t)) >  r->local_data_mr->length) {
    FlushData();// reset the buffer inside

  }
  //  const size_t estimated_block_size = r->data_block->CurrentSizeEstimate();
  //  if (estimated_block_size + key.size() + value.size() +sizeof(size_t) + kBlockTrailerSize >= r->options.block_size) {
  //
  //  }

  //Create a new index entry for every items.
  {
    //    assert(r->data_block->empty());
    //#ifndef NDEBUG
    //    size_t key_length = r->last_key.size();
    //#endif
    //    assert(r->last_key.size()>= 8);
//    if (!r->last_key.empty()){
//      r->options.comparator->FindShortestSeparator(&r->last_key, key);
      //    assert(r->last_key.size() >= 8  );
      std::string handle_encoding;
      //This index point to this offset and the key is this key.
      r->pending_data_handle.set_offset(r->offset);// This is the offset of the begginning of this block.
      r->pending_data_handle.set_size(key.size() + value.size() + 2*sizeof(uint32_t));
      r->pending_data_handle.EncodeTo(&handle_encoding);

      r->index_block->Add(key, Slice(handle_encoding));
    }

//  }

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
//  r->offset_last_added = r->offset;
  r->offset +=  key.size() + value.size() + 2*sizeof(uint32_t);




}

void TableBuilder_BAMS::UpdateFunctionBLock() {

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
void TableBuilder_BAMS::FinishDataBlock(BlockBuilder* block, BlockHandle* handle,
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
      assert(r->offset - r->offset_last_flushed <= r->local_data_mr->length);
    }
  }
  r->compressed_output.clear();
  block->Reset_Forward();
}
void TableBuilder_BAMS::FinishDataIndexBlock(BlockBuilder* block,
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
  r->compressed_output.clear();
  block_size = block_contents->size();
  DEBUG_arg("index block size: %zu \n", block_size);
  block->Reset_Forward();

}
void TableBuilder_BAMS::FinishFilterBlock(FullFilterBlockBuilder* block, BlockHandle* handle,
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
    assert(block_contents->size() <= r->local_filter_mr->length);
  }
  r->compressed_output.clear();
  block_size = block_contents->size();
  block->Reset();
}
//TODO make flushing flush the data to the remote memory flushing to remote memory
void TableBuilder_BAMS::FlushData(){

  Rep* r = rep_;
  //  std::shared_ptr<RDMA_Manager> rdma_mg =  r->options.env->rdma_mg;
  //  size_t msg_size = r->offset - r->offset_last_flushed;
  r->local_data_mr->length = r->offset - r->offset_last_flushed;
  r->local_data_mrs.insert({r->offset, r->local_data_mr});
  r->offset_last_flushed = r->offset;
  r->local_data_mr = new ibv_mr();
  r->rdma_mg->Allocate_Local_RDMA_Slot(*r->local_data_mr, FlushBuffer);
  r->data_buff.Reset((char*)r->local_data_mr->addr, 0);

  //  DEBUG_arg("In use start is %d\n", r->data_inuse_start);
  //  DEBUG_arg("In use end is %d\n", r->data_inuse_end);
  //  DEBUG_arg("Next write buffer to use %d\n", next_buffer_index);
  //  DEBUG_arg("Total local write buffer number is %zu\n", r->local_data_mr.size());
  //  DEBUG_arg("MR element number is %lu\n", r->remote_data_mrs.size());
  //  assert(r->data_inuse_start!= r->data_inuse_end);
  // No need to record the flushing times, because we can check from the remote mr map element number.
}
void TableBuilder_BAMS::FlushDataIndex(size_t msg_size) {
  Rep* r = rep_;
  //  std::shared_ptr<RDMA_Manager> rdma_mg =  r->options.env->rdma_mg;
  r->local_index_mr->length = msg_size;
  assert(r->local_index_mr!= nullptr);
  r->local_dataindex_mrs.insert({r->offset, r->local_index_mr});

  //TOFIX: the index may overflow and need to create a new index write buffer, otherwise
  // it would be overwrited.
  //  DEBUG_arg("Index block size is %zu", msg_size);
  r->local_index_mr = new ibv_mr();
  r->rdma_mg->Allocate_Local_RDMA_Slot(*r->local_index_mr, FlushBuffer);

  r->index_block->Move_buffer((char*)r->local_index_mr->addr);

}
void TableBuilder_BAMS::FlushFilter(size_t& msg_size) {
  Rep* r = rep_;
  //  std::shared_ptr<RDMA_Manager> rdma_mg =  r->options.env->rdma_mg;
  r->local_filter_mr->length = msg_size;
  r->local_filter_mrs.insert({r->offset, r->local_filter_mr});
  //TOFIX: the index may overflow and need to create a new index write buffer, otherwise
  // it would be overwrited.
  r->local_filter_mr = new ibv_mr();
  r->rdma_mg->Allocate_Local_RDMA_Slot(*r->local_filter_mr, FlushBuffer);
  r->filter_block->Move_buffer((char*)r->local_filter_mr->addr);

}

Status TableBuilder_BAMS::status() const { return rep_->status; }

Status TableBuilder_BAMS::Finish() {
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
      //No need to add the last index entry.
//      r->options.comparator->FindShortSuccessor(&r->last_key);
      //      assert(r->last_key.size()>= 8);
//      std::string handle_encoding;
//      r->pending_data_handle.set_offset(r->offset_last_added);// This is the offset of the begginning of this block.
//      r->pending_data_handle.set_size(r->offset - r->offset_last_added);
//      r->pending_data_handle.EncodeTo(&handle_encoding);
//
//      r->index_block->Add(r->last_key, Slice(handle_encoding));
//      r->pending_index_filter_entry = false;
//    }
    size_t msg_size;
    FinishDataIndexBlock(r->index_block, &index_block_handle,
                         r->options.compression, msg_size);
    FlushDataIndex(msg_size);
  }

  return r->status;
}

void TableBuilder_BAMS::Abandon() {
  Rep* r = rep_;
  assert(!r->closed);
  r->closed = true;
}

uint64_t TableBuilder_BAMS::NumEntries() const { return rep_->num_entries; }

uint64_t TableBuilder_BAMS::FileSize() const { return rep_->offset; }
void TableBuilder_BAMS::get_datablocks_map(std::map<uint32_t, ibv_mr*>& map) {
  map = rep_->local_data_mrs;
}
void TableBuilder_BAMS::get_dataindexblocks_map(std::map<uint32_t, ibv_mr*>& map) {
  map = rep_->local_dataindex_mrs;
}
void TableBuilder_BAMS::get_filter_map(std::map<uint32_t, ibv_mr*>& map) {
  map = rep_->local_filter_mrs;
}
size_t TableBuilder_BAMS::get_numentries() {
  return rep_->num_entries;
}
}