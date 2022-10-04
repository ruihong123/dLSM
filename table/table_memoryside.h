//
// Created by ruihong on 8/7/21.
//

#ifndef TimberSaw_TABLE_MEMORYSIDE_H
#define TimberSaw_TABLE_MEMORYSIDE_H
#include <cstdint>
#include <memory>

#include "TimberSaw/export.h"
#include "TimberSaw/iterator.h"
#include "db/version_edit.h"
namespace TimberSaw {
class Block;
class BlockHandle;
class Footer;
struct Options;
class RandomAccessFile;
struct ReadOptions;
class TableCache;
class TimberSaw_EXPORT Table_Memory_Side {
 public:
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
  static Status Open(const Options& options, Table_Memory_Side** table,
                     const std::shared_ptr<RemoteMemTableMetaData>& Remote_table_meta);

  Table_Memory_Side(const Table_Memory_Side&) = delete;
  Table_Memory_Side& operator=(const Table_Memory_Side&) = delete;

  ~Table_Memory_Side();

  // Returns a new iterator over the table contents.
  // The result of NewIterator() is initially invalid (caller must
  // call one of the Seek methods on the iterator before using it).
  Iterator* NewIterator(const ReadOptions&) const;

  // Given a key, return an approximate byte offset in the file where
  // the data for that key begins (or would begin if the key were
  // present in the file).  The returned value is in terms of file
  // bytes, and so includes effects like compression of the underlying data.
  // E.g., the approximate offset of the last key in the table will
  // be close to the file length.
  uint64_t ApproximateOffsetOf(const Slice& key) const;

  static Slice KVReader(void* arg, const ReadOptions& options, const Slice& index_value);

 private:
  friend class TableCache;
  struct Rep;

  static Iterator* BlockReader(void* arg, const ReadOptions&, const Slice&);
  explicit Table_Memory_Side(Rep* rep) : rep(rep) {}

  // Calls (*handle_result)(arg, ...) with the entry found after a call
  // to Seek(key).  May not make such a call if filter policy says
  // that key is not present.
  Status InternalGet(const ReadOptions&, const Slice& key, void* arg,
                     void (*handle_result)(void* arg, const Slice& k,
                         const Slice& v));
  //
  //  void ReadMeta(const Footer& footer);
  void ReadFilter();
  void* Get_remote_table_ptr();
  Rep* const rep;
  //  static std::atomic<int> table
};
}

#endif  // TimberSaw_TABLE_MEMORYSIDE_H
