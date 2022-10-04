// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_TimberSaw_INCLUDE_TABLE_H_
#define STORAGE_TimberSaw_INCLUDE_TABLE_H_

#include <cstdint>
#include <memory>

#include "TimberSaw/export.h"
#include "TimberSaw/iterator.h"
#include "db/version_edit.h"
#include "table/full_filter_block.h"
#include "table/format.h"
#include "table/block.h"
namespace TimberSaw {

class Block;
class BlockHandle;
class Footer;
struct Options;
class RandomAccessFile;
struct ReadOptions;
class TableCache;

// A Table is a sorted map from strings to strings.  Tables are
// immutable and persistent.  A Table may be safely accessed from
// multiple threads without external synchronization.
class TimberSaw_EXPORT Table {
 public:
  struct Rep {
    Rep(const Options& options) : options(options) {

    }
    ~Rep() {
      delete filter;
      //    delete[] filter_data;
      delete index_block;
    }

    Options options;
    Status status;
    // weak_ptr because if there is cached value in the table table_cache then the obsoleted SST
    // will never be garbage collected.
    std::weak_ptr<RemoteMemTableMetaData> remote_table;
    uint64_t cache_id;
    FullFilterBlockReader* filter;
    //  const char* filter_data;

    BlockHandle metaindex_handle;  // Handle to metaindex_block: saved from footer
    Block* index_block;
#ifdef BYTEADDRESSABLE
//    Iterator* index_iter;
//    ThreadLocalPtr* mr_addr;
#endif
  };
  // Attempt to open the table that is stored in bytes [0..file_size)
  // of "file", and read the metadata entries necessary to allow
  // retrieving data from the table.
  //
  // If successful, returns ok and sets "*table" to the newly opened
  // table.  The client should delete "*table" when no longer needed.
  // If there was an error while initializing the table, sets "*table"
  // to nullptr and returns a non-ok status.  Does not take ownership of
  // "*source", but the client must ensure that "source" remains live
  // for the duration of the returned table's lifetime.
  //
  // *file must remain live while this Table is in use.
  static Status Open(const Options& options, Table** table,
                     const std::shared_ptr<RemoteMemTableMetaData>& Remote_table_meta);

  Table(const Table&) = delete;
  Table& operator=(const Table&) = delete;

  ~Table();

  // Returns a new iterator over the table contents.
  // The result of NewIterator() is initially invalid (caller must
  // call one of the Seek methods on the iterator before using it).
  Iterator* NewIterator(const ReadOptions&) const;
#ifdef BYTEADDRESSABLE
  Iterator* NewSEQIterator(const ReadOptions&) const;
//  void GetKV(Iterator* iiter);
#endif
  // Given a key, return an approximate byte offset in the file where
  // the data for that key begins (or would begin if the key were
  // present in the file).  The returned value is in terms of file
  // bytes, and so includes effects like compression of the underlying data.
  // E.g., the approximate offset of the last key in the table will
  // be close to the file length.
  uint64_t ApproximateOffsetOf(const Slice& key) const;
  Rep* const rep;

  static Slice KVReader(void*, const ReadOptions&, const Slice&);

 private:
  friend class TableCache;
//  struct Rep;

  static Iterator* BlockReader(void*, const ReadOptions&, const Slice&);
  explicit Table(Rep* rep) : rep(rep) {}

  // Calls (*handle_result)(arg, ...) with the entry found after a call
  // to Seek(key).  May not make such a call if filter policy says
  // that key is not present.
  Status InternalGet(const ReadOptions&, const Slice& key, void* arg,
                     void (*handle_result)(void* arg, const Slice& k,
                                           const Slice& v));

  void ReadMeta(const Footer& footer);
  void ReadFilter();

};

}  // namespace TimberSaw

#endif  // STORAGE_TimberSaw_INCLUDE_TABLE_H_
