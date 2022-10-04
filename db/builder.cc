// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/builder.h"

#include <table/table_builder_computeside.h>

namespace TimberSaw {

Status BuildTable(const std::string& dbname, Env* env, const Options& options,
                  TableCache* table_cache, Iterator* iter,
                  std::shared_ptr<RemoteMemTableMetaData> meta, IO_type type) {
  Status s;
//  meta->file_size = 0;
  iter->SeekToFirst();

  std::string fname = TableFileName(dbname, meta->number);
  if (iter->Valid()) {
    TableBuilder_ComputeSide* builder =
        new TableBuilder_ComputeSide(options, type, 0);
    meta->smallest.DecodeFrom(iter->key());
    Slice key;
    for (; iter->Valid(); iter->Next()) {
      key = iter->key();
      builder->Add(key, iter->value());
    }
    if (!key.empty()) {
      meta->largest.DecodeFrom(key);
    }

    // Finish and check for builder errors

    s = builder->Finish();
    builder->get_datablocks_map(meta->remote_data_mrs);
    builder->get_dataindexblocks_map(meta->remote_dataindex_mrs);
    builder->get_filter_map(meta->remote_filter_mrs);


    meta->file_size = 0;
    for(auto iter : meta->remote_data_mrs){
      meta->file_size += iter.second->length;
    }
    assert(builder->FileSize() == meta->file_size);
    delete builder;
//TOFIX: temporarily disable the verification of index block.

    if (s.ok()) {
      // Verify that the table is usable
      Iterator* it = table_cache->NewIterator(ReadOptions(), meta);
      s = it->status();
#ifndef NDEBUG
      it->SeekToFirst();
      while(it->Valid()){
        it->Next();
      }
#endif
      delete it;
    }
  }

  // Check for input iterator errors
  if (!iter->status().ok()) {
    s = iter->status();
  }

  if (s.ok() && !meta->remote_data_mrs.empty()) {
    // Keep it
  } else {
    env->RemoveFile(fname);
  }
  return s;
}

}  // namespace TimberSaw
