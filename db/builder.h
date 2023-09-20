// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_TimberSaw_DB_BUILDER_H_
#define STORAGE_TimberSaw_DB_BUILDER_H_

#include "db/dbformat.h"
#include "db/filename.h"
#include "db/table_cache.h"
#include "db/version_edit.h"
#include <include/TimberSaw/table_builder.h>
#include <memory>

#include "TimberSaw/db.h"
#include "TimberSaw/env.h"
#include "TimberSaw/iterator.h"
#include "TimberSaw/status.h"
namespace TimberSaw {

struct Options;
struct RemoteMemTableMetaData;

class Env;
class Iterator;
class TableCache;
class VersionEdit;

// Build a Table file from the contents of *iter.  The generated file
// will be named according to meta->number.  On success, the rest of
// *meta will be filled with metadata about the generated table.
// If no data is present in *iter, meta->file_size will be set to
// zero, and no Table file will be produced.
Status BuildTable(const std::string& dbname, Env* env, const Options& options,
                  TableCache* table_cache, Iterator* iter,
                  std::shared_ptr<RemoteMemTableMetaData> meta, IO_type type);

}  // namespace TimberSaw

#endif  // STORAGE_TimberSaw_DB_BUILDER_H_
