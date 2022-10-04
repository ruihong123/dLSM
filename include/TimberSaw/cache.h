// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// A Cache is an interface that maps keys to values.  It has internal
// synchronization and may be safely accessed concurrently from
// multiple threads.  It may automatically evict entries to make room
// for new entries.  Values have a specified charge against the table_cache
// capacity.  For example, a table_cache where the values are variable
// length strings, may use the length of the string as the charge for
// the string.
//
// A builtin table_cache implementation with a least-recently-used eviction
// policy is provided.  Clients may use their own implementations if
// they want something more sophisticated (like scan-resistance, a
// custom eviction policy, variable table_cache sizing, etc.)

#ifndef STORAGE_TimberSaw_INCLUDE_CACHE_H_
#define STORAGE_TimberSaw_INCLUDE_CACHE_H_

#include <cstdint>

#include "TimberSaw/export.h"
#include "TimberSaw/slice.h"

namespace TimberSaw {

class TimberSaw_EXPORT Cache;

// Create a new table_cache with a fixed size capacity.  This implementation
// of Cache uses a least-recently-used eviction policy.
TimberSaw_EXPORT Cache* NewLRUCache(size_t capacity);

class TimberSaw_EXPORT Cache {
 public:
  Cache() = default;

  Cache(const Cache&) = delete;
  Cache& operator=(const Cache&) = delete;

  // Destroys all existing entries by calling the "deleter"
  // function that was passed to the constructor.
  virtual ~Cache();

  // Opaque handle to an entry stored in the table_cache.
  struct Handle {};
  virtual size_t GetCapacity() = 0;
  // Insert a mapping from key->value into the table_cache and assign it
  // the specified charge against the total table_cache capacity.
  //
  // Returns a handle that corresponds to the mapping.  The caller
  // must call this->Release(handle) when the returned mapping is no
  // longer needed.
  //
  // When the inserted entry is no longer needed, the key and
  // value will be passed to "deleter".
  virtual Handle* Insert(const Slice& key, void* value, size_t charge,
                         void (*deleter)(const Slice& key, void* value)) = 0;

  // If the table_cache has no mapping for "key", returns nullptr.
  //
  // Else return a handle that corresponds to the mapping.  The caller
  // must call this->Release(handle) when the returned mapping is no
  // longer needed.
  virtual Handle* Lookup(const Slice& key) = 0;

  // Release a mapping returned by a previous Lookup().
  // REQUIRES: handle must not have been released yet.
  // REQUIRES: handle must have been returned by a method on *this.
  virtual void Release(Handle* handle) = 0;

  // Return the value encapsulated in a handle returned by a
  // successful Lookup().
  // REQUIRES: handle must not have been released yet.
  // REQUIRES: handle must have been returned by a method on *this.
  virtual void* Value(Handle* handle) = 0;

  // If the table_cache contains entry for key, erase it.  Note that the
  // underlying entry will be kept around until all existing handles
  // to it have been released.
  virtual void Erase(const Slice& key) = 0;

  // Return a new numeric id.  May be used by multiple clients who are
  // sharing the same table_cache to partition the key space.  Typically the
  // client will allocate a new id at startup and prepend the id to
  // its table_cache keys.
  virtual uint64_t NewId() = 0;

  // Remove all table_cache entries that are not actively in use.  Memory-constrained
  // applications may wish to call this method to reduce memory usage.
  // Default implementation of Prune() does nothing.  Subclasses are strongly
  // encouraged to override the default implementation.  A future release of
  // TimberSaw may change Prune() to a pure abstract method.
  virtual void Prune() {}

  // Return an estimate of the combined charges of all elements stored in the
  // table_cache.
  virtual size_t TotalCharge() const = 0;

 private:
  void LRU_Remove(Handle* e);
  void LRU_Append(Handle* e);
  void Unref(Handle* e);

  struct Rep;
  Rep* rep_;
};

}  // namespace TimberSaw

#endif  // STORAGE_TimberSaw_INCLUDE_CACHE_H_
