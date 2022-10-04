// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/version_edit.h"

#include "db/version_set.h"
#include "util/coding.h"
#include "memory_node/memory_node_keeper.h"
#include "TimberSaw/env.h"
#include "table_cache.h"
namespace TimberSaw {
//std::shared_ptr<RDMA_Manager> RemoteMemTableMetaData::rdma_mg = Env::Default()->rdma_mg;
//RemoteMemTableMetaData::RemoteMemTableMetaData()  : table_type(0), allowed_seeks(1 << 30) {
//  rdma_mg = Env::Default()->rdma_mg;
//  node_id = rdma_mg->node_id;
//}
// Every file can only belongs to one remote memory
/***************************************/
// this_machine_type 0 means compute node, 1 means memory node
// creater_node_id  odd means compute node, even means memory node
RemoteMemTableMetaData::RemoteMemTableMetaData(int machine_type,
                                               TableCache* cache, uint8_t id)
    : this_machine_type(machine_type), shard_target_node_id(id),
      allowed_seeks(1 << 30),
      table_cache(cache) {
//  assert(shard_target_node_id < 36);
  // Tothink: Is this_machine_type the same as rdma_mg->node_id?
  // Node_id is unique for every node, while the this_machine_type only distinguish
  // the compute node from the memory node.
  if (machine_type == 0){
    rdma_mg = Env::Default()->rdma_mg;
    creator_node_id = rdma_mg->node_id; // rdma_mg->node_id = machine_type
  }else{
    rdma_mg = Memory_Node_Keeper::rdma_mg;
    creator_node_id = rdma_mg->node_id;
  }
}
RemoteMemTableMetaData::RemoteMemTableMetaData(int side)
    : this_machine_type(side), shard_target_node_id(99), allowed_seeks(1 << 30),table_cache(nullptr) {
  // Tothink: Is this_machine_type the same as rdma_mg->node_id?
  //  Node_id is unique for every node, while the this_machine_type only distinguish the compute node from the memory node.
  if (side == 0) {
    rdma_mg = Env::Default()->rdma_mg;
    creator_node_id = rdma_mg->node_id;  // rdma_mg->node_id = side
  } else {
    rdma_mg = Memory_Node_Keeper::rdma_mg;
    creator_node_id = rdma_mg->node_id;
  }
}
RemoteMemTableMetaData::~RemoteMemTableMetaData() {
  //TODO and Tothink: when destroy this metadata check whether this is compute node, if yes, send a message to
  // home node to deference. Or the remote dereference is conducted in the granularity of version.
  assert(remote_dataindex_mrs.size() == 1);
  assert(this_machine_type ==0 || this_machine_type == 1);
//  assert(creator_node_id == 0 || creator_node_id == 1);

  if (this_machine_type == 0){
    assert(table_cache!= nullptr);
    if (table_cache != nullptr){
      table_cache->Evict(number, creator_node_id);
    }
    if (creator_node_id == rdma_mg->node_id){
      //#ifndef NDEBUG
      //        printf("Destroying RemoteMemtableMetaData locally on compute node, Table number is %lu, creator node id is %d \n", number, creator_node_id);
      //#endif
      if(Remote_blocks_deallocate(remote_data_mrs) &&
          Remote_blocks_deallocate(remote_dataindex_mrs) &&
          Remote_blocks_deallocate(remote_filter_mrs)){
        DEBUG("Remote blocks deleted successfully\n");
      }else{
        DEBUG("Remote memory collection not found\n");
        assert(false);
      }
    }else{
      //#ifndef NDEBUG
      //        printf("chunks will be garbage collected on the memory node, Table number is %lu, "
      //            "creator node id is %d index block pointer is %p\n", number, creator_node_id, remote_dataindex_mrs.begin()->second->addr);
      //#endif
      //        assert(remote_dataindex_mrs.size() == 1);
      Prepare_Batch_Deallocate();
    }

  } else if (this_machine_type == 1){
    for (auto it = remote_data_mrs.begin(); it != remote_data_mrs.end(); it++){
      delete it->second;
    }
    for (auto it = remote_dataindex_mrs.begin(); it != remote_dataindex_mrs.end(); it++){
      delete it->second;
    }
    for (auto it = remote_filter_mrs.begin(); it != remote_filter_mrs.end(); it++){
      delete it->second;
    }
  }

  //    else if(this_machine_type == 1 && creator_node_id == rdma_mg->node_id){
  //      //TODO: memory collection for the remote memory.
  //      if(Local_blocks_deallocate(remote_data_mrs) &&
  //      Local_blocks_deallocate(remote_dataindex_mrs) &&
  //      Local_blocks_deallocate(remote_filter_mrs)){
  //        DEBUG("Local blocks deleted successfully\n");
  //      }else{
  //        DEBUG("Local memory collection not found\n");
  //        assert(false);
  //      }
  //    }


}
void RemoteMemTableMetaData::EncodeTo(std::string* dst) const {
  PutFixed64(dst, level);
  PutFixed64(dst, number);
//  printf("Node id is %u", creator_node_id);
  dst->append(reinterpret_cast<const char*>(&creator_node_id), sizeof(creator_node_id));
  dst->append(reinterpret_cast<const char*>(&shard_target_node_id), sizeof(shard_target_node_id));

  PutFixed64(dst, file_size);
  PutLengthPrefixedSlice(dst, smallest.Encode());
  PutLengthPrefixedSlice(dst, largest.Encode());
  uint64_t remote_data_chunk_num = remote_data_mrs.size();
  uint64_t remote_dataindex_chunk_num = remote_dataindex_mrs.size();
  uint64_t remote_filter_chunk_num = remote_filter_mrs.size();
//  assert(remote_filter_chunk_num = 1);
  PutFixed64(dst, remote_data_chunk_num);
  PutFixed64(dst, remote_dataindex_chunk_num);
  PutFixed64(dst, remote_filter_chunk_num);
  //Here we suppose all the remote memory chuck for the same table have similar infomation  below,
  // the only difference is the addr and lenght
//#ifndef NDEBUG
//  uint32_t rkey = remote_data_mrs.begin()->second->rkey;
//  for(auto iter : remote_data_mrs){
//    assert(rkey == iter.second->rkey);
////    rkey =
//  }
//#endif
  PutFixed64(dst, (uint64_t)remote_data_mrs.begin()->second->context);
  PutFixed64(dst, (uint64_t)remote_data_mrs.begin()->second->pd);
  PutFixed32(dst, (uint32_t)remote_data_mrs.begin()->second->handle);
//  PutFixed32(dst, (uint32_t)remote_data_mrs.begin()->second->lkey);
//  PutFixed32(dst, (uint32_t)remote_data_mrs.begin()->second->rkey);
  for(auto iter : remote_data_mrs){
    PutFixed32(dst, iter.first);
    mr_serialization(dst,iter.second);
  }
  for(auto iter : remote_dataindex_mrs){
    PutFixed32(dst, iter.first);
    mr_serialization(dst,iter.second);
  }
  for(auto iter : remote_filter_mrs){
    PutFixed32(dst, iter.first);
    mr_serialization(dst,iter.second);
  }
//  size_t
}
Status RemoteMemTableMetaData::DecodeFrom(Slice& src) {
#ifndef NDEBUG
  Slice debug_src = src;
#endif
  Status s = Status::OK();
  if (this_machine_type == 0)
    rdma_mg = Env::Default()->rdma_mg;
  else
    rdma_mg = Memory_Node_Keeper::rdma_mg;

  GetFixed64(&src, &level);
  assert(level < config::kNumLevels);

  GetFixed64(&src, &number);
//  node_id = reinterpret_cast<uint8_t*>(src.data());
  memcpy(&creator_node_id, src.data(), sizeof(creator_node_id));
  src.remove_prefix(sizeof(creator_node_id));
//  printf("Node id is %u", creator_node_id);
  memcpy(&shard_target_node_id, src.data(), sizeof(shard_target_node_id));
  src.remove_prefix(sizeof(shard_target_node_id));
  assert(shard_target_node_id < 36);

  GetFixed64(&src, &file_size);
  Slice temp;
  GetLengthPrefixedSlice(&src, &temp);
  smallest.DecodeFrom(temp);
  GetLengthPrefixedSlice(&src, &temp);
  largest.DecodeFrom(temp);
  uint64_t remote_data_chunk_num;
  uint64_t remote_dataindex_chunk_num;
  uint64_t remote_filter_chunk_num;
  GetFixed64(&src, &remote_data_chunk_num);
  GetFixed64(&src, &remote_dataindex_chunk_num);
  GetFixed64(&src, &remote_filter_chunk_num);
  uint64_t context_temp;
  uint64_t pd_temp;
  uint32_t handle_temp;
  uint32_t lkey_temp;
  uint32_t rkey_temp;
  GetFixed64(&src, &context_temp);
  GetFixed64(&src, &pd_temp);
  GetFixed32(&src, &handle_temp);
//  GetFixed32(&src, &lkey_temp);
//  GetFixed32(&src, &rkey_temp);
  assert(remote_data_chunk_num < 1000 && remote_data_chunk_num>0);
  for(auto i = 0; i< remote_data_chunk_num; i++){
    //Todo: check whether the reinterpret_cast here make the correct value.
    ibv_mr* mr = new ibv_mr{.context =  reinterpret_cast<ibv_context*>(context_temp),
                            .pd =  reinterpret_cast<ibv_pd*>(pd_temp), .addr =  nullptr,
                            .length =  0,
                            .handle = handle_temp, .lkey = lkey_temp, .rkey = rkey_temp};
    uint32_t offset = 0;
    GetFixed32(&src, &offset);
    GetFixed64(&src, reinterpret_cast<uint64_t*>(&mr->addr));
    GetFixed64(&src, &mr->length);
    GetFixed32(&src, &mr->lkey);
    GetFixed32(&src, &mr->rkey);
    remote_data_mrs.insert({offset, mr});
    assert(debug_src.data() - src.data() <=16384);
  }
  for(auto i = 0; i< remote_dataindex_chunk_num; i++){
    //Todo: check whether the reinterpret_cast here make the correct value.
    ibv_mr* mr = new ibv_mr{.context =  reinterpret_cast<ibv_context*>(context_temp),
                            .pd =  reinterpret_cast<ibv_pd*>(pd_temp), .addr =  nullptr,
                            .length =  0,
                            .handle = handle_temp, .lkey = lkey_temp, .rkey = rkey_temp};
    uint32_t offset = 0;
    GetFixed32(&src, &offset);
    GetFixed64(&src, reinterpret_cast<uint64_t*>(&mr->addr));
    GetFixed64(&src, &mr->length);
    GetFixed32(&src, &mr->lkey);
    GetFixed32(&src, &mr->rkey);
    assert(mr!= nullptr);
    assert(offset != 0);
    remote_dataindex_mrs.insert({offset, mr});
  }
  assert(!remote_dataindex_mrs.empty());
  for(auto i = 0; i< remote_filter_chunk_num; i++){
    //Todo: check whether the reinterpret_cast here make the correct value.
    ibv_mr* mr = new ibv_mr{context: reinterpret_cast<ibv_context*>(context_temp),
                            pd: reinterpret_cast<ibv_pd*>(pd_temp), addr: nullptr,
                            length: 0,
                            handle:handle_temp, lkey:0, rkey:0};
    uint32_t offset = 0;
    GetFixed32(&src, &offset);
    GetFixed64(&src, reinterpret_cast<uint64_t*>(&mr->addr));
    GetFixed64(&src, &mr->length);
    GetFixed32(&src, &mr->lkey);
    GetFixed32(&src, &mr->rkey);
    remote_filter_mrs.insert({offset, mr});
  }
  assert(!remote_filter_mrs.empty());
  return s;
}
void RemoteMemTableMetaData::mr_serialization(std::string* dst, ibv_mr* mr) const {
//  PutFixed64(dst, (uint64_t)mr->context);
//  PutFixed64(dst, (uint64_t)mr->pd);
  PutFixed64(dst, (uint64_t)mr->addr);
  PutFixed64(dst, mr->length);
//  PutFixed32(dst, mr->handle);
  PutFixed32(dst, mr->lkey);
  PutFixed32(dst, mr->rkey);

}

// Tag numbers for serialized VersionEdit.  These numbers are written to
// disk and should not be changed.
enum Tag {
  kComparator = 1,
  kLogNumber = 2,
  kNextFileNumber = 3,
  kLastSequence = 4,
  kCompactPointer = 5,
  kDeletedFile = 6,
  kNewFile = 7,
  // 8 was used for large value refs
  kPrevLogNumber = 9
};

void VersionEdit::Clear() {
  comparator_.clear();
  log_number_ = 0;
  prev_log_number_ = 0;
  last_sequence_ = 0;
  next_file_number_ = 0;
  has_comparator_ = false;
  has_log_number_ = false;
  has_prev_log_number_ = false;
  has_next_file_number_ = false;
  has_last_sequence_ = false;
  deleted_files_.clear();
  new_files_.clear();
}

void VersionEdit::EncodeTo(std::string* dst) const {
  if (has_comparator_) {
    PutVarint32(dst, kComparator);
    PutLengthPrefixedSlice(dst, comparator_);
  }
  if (has_log_number_) {
    PutVarint32(dst, kLogNumber);
    PutVarint64(dst, log_number_);
  }
  if (has_prev_log_number_) {
    PutVarint32(dst, kPrevLogNumber);
    PutVarint64(dst, prev_log_number_);
  }
  if (has_next_file_number_) {
    PutVarint32(dst, kNextFileNumber);
    PutVarint64(dst, next_file_number_);
  }
  if (has_last_sequence_) {
    PutVarint32(dst, kLastSequence);
    PutVarint64(dst, last_sequence_);
  }

  for (size_t i = 0; i < compact_pointers_.size(); i++) {
    PutVarint32(dst, kCompactPointer);
    PutVarint32(dst, compact_pointers_[i].first);  // level
    PutLengthPrefixedSlice(dst, compact_pointers_[i].second.Encode());
  }

  for (const auto& deleted_file_kvp : deleted_files_) {
    PutVarint32(dst, kDeletedFile);
    PutVarint32(dst, std::get<0>(deleted_file_kvp));   // level
    PutVarint64(dst, std::get<1>(deleted_file_kvp));  // file number
    dst->append(reinterpret_cast<const char*>(&std::get<2>(deleted_file_kvp)), sizeof(uint8_t));
//    printf("level is %d, number is %lu, node_id is %u", std::get<0>(deleted_file_kvp),
//        std::get<1>(deleted_file_kvp), std::get<2>(deleted_file_kvp));

  }

  for (size_t i = 0; i < new_files_.size(); i++) {
    const std::shared_ptr<RemoteMemTableMetaData> f = new_files_[i].second;
    PutVarint32(dst, kNewFile);
    PutVarint32(dst, new_files_[i].first);  // level
    f->EncodeTo(dst);

  }
//  assert(dst->size() < new_files_[0].second->rdma_mg->name_to_size["version_edit"]);
}
void VersionEdit::EncodeToDiskFormat(std::string* dst) const {
  if (has_comparator_) {
    PutVarint32(dst, kComparator);
    PutLengthPrefixedSlice(dst, comparator_);
  }
  if (has_log_number_) {
    PutVarint32(dst, kLogNumber);
    PutVarint64(dst, log_number_);
  }
  if (has_prev_log_number_) {
    PutVarint32(dst, kPrevLogNumber);
    PutVarint64(dst, prev_log_number_);
  }
  if (has_next_file_number_) {
    PutVarint32(dst, kNextFileNumber);
    PutVarint64(dst, next_file_number_);
  }
  if (has_last_sequence_) {
    PutVarint32(dst, kLastSequence);
    PutVarint64(dst, last_sequence_);
  }

  for (size_t i = 0; i < compact_pointers_.size(); i++) {
    PutVarint32(dst, kCompactPointer);
    PutVarint32(dst, compact_pointers_[i].first);  // level
    PutLengthPrefixedSlice(dst, compact_pointers_[i].second.Encode());
  }

  for (const auto& deleted_file_kvp : deleted_files_) {
    PutVarint32(dst, kDeletedFile);
    PutVarint32(dst, std::get<0>(deleted_file_kvp));   // level
    PutVarint64(dst, std::get<1>(deleted_file_kvp));  // file number
    PutVarint64(dst, std::get<2>(deleted_file_kvp));  // creator node id
  }

  for (size_t i = 0; i < new_files_.size(); i++) {
    std::shared_ptr<RemoteMemTableMetaData> f = new_files_[i].second;
    PutVarint32(dst, kNewFile);
    PutVarint32(dst, new_files_[i].first);  // level
    PutVarint64(dst, f->number);
    dst->append(reinterpret_cast<const char*>(&f->creator_node_id), sizeof(f->creator_node_id));

    PutVarint64(dst, f->file_size);
    PutLengthPrefixedSlice(dst, f->smallest.Encode());
    PutLengthPrefixedSlice(dst, f->largest.Encode());
  }
}

static bool GetInternalKey(Slice* input, InternalKey* dst) {
  Slice str;
  if (GetLengthPrefixedSlice(input, &str)) {
    return dst->DecodeFrom(str);
  } else {
    return false;
  }
}

static bool GetLevel(Slice* input, int* level) {
  uint32_t v;
  if (GetVarint32(input, &v) && v < config::kNumLevels) {
    *level = v;
    return true;
  } else {
    return false;
  }
}
// this machine type: 0 means compute node, 1 means memory node.
Status VersionEdit::DecodeFrom(const Slice src, int this_machine_type,
                               TableCache* cache) {
  Clear();
  Slice input = src;
  const char* msg = nullptr;
  uint32_t tag;

  // Temporary storage for parsing
  int level;
  uint64_t number;
  uint8_t node_id;
  Slice str;
  InternalKey key;
  size_t counter = 0;
  while (msg == nullptr && GetVarint32(&input, &tag)) {
    switch (tag) {
      case kComparator:
        if (GetLengthPrefixedSlice(&input, &str)) {
          comparator_ = str.ToString();
          has_comparator_ = true;
        } else {
          msg = "comparator name";
        }
        break;

      case kLogNumber:
        if (GetVarint64(&input, &log_number_)) {
          has_log_number_ = true;
        } else {
          msg = "log number";
        }
        break;

      case kPrevLogNumber:
        if (GetVarint64(&input, &prev_log_number_)) {
          has_prev_log_number_ = true;
        } else {
          msg = "previous log number";
        }
        break;

      case kNextFileNumber:
        if (GetVarint64(&input, &next_file_number_)) {
          has_next_file_number_ = true;
        } else {
          msg = "next file number";
        }
        break;

      case kLastSequence:
        if (GetVarint64(&input, &last_sequence_)) {
          has_last_sequence_ = true;
        } else {
          msg = "last sequence number";
        }
        break;

      case kCompactPointer:
        if (GetLevel(&input, &level) && GetInternalKey(&input, &key)) {
          compact_pointers_.push_back(std::make_pair(level, key));
        } else {
          msg = "compaction pointer";
        }
        break;

      case kDeletedFile:
        if (GetLevel(&input, &level) && GetVarint64(&input, &number)) {

          memcpy(&node_id, input.data(), sizeof(node_id));
//          assert(node_id < 2);
          input.remove_prefix(sizeof(node_id));
          deleted_files_.insert(std::make_tuple(level, number, node_id));
//          printf("level is %d, number is %lu, node_id is %u", level, number, node_id);
        } else {
          msg = "deleted file";
        }
        break;

      case kNewFile:
        if (GetLevel(&input, &level)) {
          std::shared_ptr<RemoteMemTableMetaData> f = std::make_shared<RemoteMemTableMetaData>(this_machine_type,cache, 999);
          f->DecodeFrom(input);
//          assert(level == 0);
          new_files_.push_back(std::make_pair(level, f));
        } else {
          msg = "new-file entry";
        }
        break;

      default:
        msg = "unknown tag";
        break;
    }
    counter++;
    if(counter>100000){
      printf("corrupted version edit decode\n");
      exit(0);
    }
  }

  if (msg == nullptr && !input.empty()) {
    msg = "invalid tag";
  }

  Status result;
  if (msg != nullptr) {
    result = Status::Corruption("VersionEdit", msg);
  }
  return result;
}

std::string VersionEdit::DebugString() const {
  std::string r;
  r.append("VersionEdit {");
  if (has_comparator_) {
    r.append("\n  Comparator: ");
    r.append(comparator_);
  }
  if (has_log_number_) {
    r.append("\n  LogNumber: ");
    AppendNumberTo(&r, log_number_);
  }
  if (has_prev_log_number_) {
    r.append("\n  PrevLogNumber: ");
    AppendNumberTo(&r, prev_log_number_);
  }
  if (has_next_file_number_) {
    r.append("\n  NextFile: ");
    AppendNumberTo(&r, next_file_number_);
  }
  if (has_last_sequence_) {
    r.append("\n  LastSeq: ");
    AppendNumberTo(&r, last_sequence_);
  }
  for (size_t i = 0; i < compact_pointers_.size(); i++) {
    r.append("\n  CompactPointer: ");
    AppendNumberTo(&r, compact_pointers_[i].first);
    r.append(" ");
    r.append(compact_pointers_[i].second.DebugString());
  }
  for (const auto& deleted_files_kvp : deleted_files_) {
    r.append("\n  RemoveFile: ");
    AppendNumberTo(&r, std::get<0>(deleted_files_kvp));
    r.append(" ");
    AppendNumberTo(&r, std::get<1>(deleted_files_kvp));
  }
  for (size_t i = 0; i < new_files_.size(); i++) {
    const std::shared_ptr<RemoteMemTableMetaData> f = new_files_[i].second;
    r.append("\n  AddFile: ");
    AppendNumberTo(&r, new_files_[i].first);
    r.append(" ");
    AppendNumberTo(&r, f->number);
    r.append(" ");
    AppendNumberTo(&r, f->file_size);
    r.append(" ");
    r.append(f->smallest.DebugString());
    r.append(" .. ");
    r.append(f->largest.DebugString());
  }
  r.append("\n}\n");
  return r;
}

void VersionEdit_Merger::merge_one_edit(VersionEdit* edit) {

  for (auto iter : *edit->GetDeletedFiles()){
    if (std::get<1>(iter)==5){
      printf("break here");
    }
    //TODO: we need to consider when the version edit is out of order.
    if (new_files_.erase(std::get<1>(iter)) == 0) {
      deleted_files_.insert(iter);
      //if there is no file already and it is a trival change then this file do not
      // need to be unpin.
      if (edit->IsTrival()){
        only_trival_change.insert(std::get<1>(iter));
      }
    }else{
      DEBUG_arg("delete a file %lu to", std::get<1>(iter));
      //if this is a trival edit, the delted file will not trigger its unpin.
      if (!edit->IsTrival()){
//        assert(std::find(merged_file_numbers.begin(), merged_file_numbers.end(),
//                         std::get<1>(iter)) == merged_file_numbers.end());

        // if the merged file is a result from the element in the only_trival_change.
        if (only_trival_change.find(std::get<1>(iter)) != only_trival_change.end()){
          // if we just delete a sstable which only have trival change according to
          // the merge edit. Then we need to remove it from the only trival change list.
          // if we do not do this, the fucntion PersistSSTables will create more threads than expected.
          // That may cause system crash.
          only_trival_change.erase(std::get<1>(iter));
        }else{
          assert(debug_map.find(std::get<1>(iter)) == debug_map.end());
          DEBUG_arg("The merged file is %lu\n", std::get<1>(iter));
          merged_file_numbers.push_back(std::get<1>(iter));
          if (merged_file_numbers.size() >= UNPIN_GRANULARITY){
            ready_to_upin_merged_file = true;
          }
        }


      }
      //if the edit is trival and the file creation is within this merging,
      // do not unpin here, unpin during the consistent check point. we do nothing
      // here
    }
  }
  for (auto iter : *edit->GetNewFiles()) {
    DEBUG_arg("insert a file %lu to", iter.second->number);
    new_files_.insert({iter.second->number, iter.second});
  }
//  ve_counter++;
//  if (ve_counter >= EDIT_MERGER_COUNT){
//    ve_counter = 0;
//    return true;
//  }else{
//    return false;
//  }
//  return true;
}
void VersionEdit_Merger::EncodeToDiskFormat(std::string* dst) const {
  for (const auto& deleted_file_kvp : deleted_files_) {
    PutVarint32(dst, kDeletedFile);
    PutVarint32(dst, std::get<0>(deleted_file_kvp));   // level
    PutVarint64(dst, std::get<1>(deleted_file_kvp));  // file number
    PutVarint64(dst, std::get<2>(deleted_file_kvp));  // creator node id
  }

  for (auto iter : new_files_) {
    std::shared_ptr<RemoteMemTableMetaData> f = iter.second;
    PutVarint32(dst, kNewFile);
    PutVarint32(dst, f->level);  // level
    PutVarint64(dst, f->number);
    dst->append(reinterpret_cast<const char*>(&f->creator_node_id), sizeof(f->creator_node_id));

    PutVarint64(dst, f->file_size);
    PutLengthPrefixedSlice(dst, f->smallest.Encode());
    PutLengthPrefixedSlice(dst, f->largest.Encode());
  }
}

}  // namespace TimberSaw
