// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Simple hash function used for internal data structures

#ifndef STORAGE_TimberSaw_UTIL_HASH_H_
#define STORAGE_TimberSaw_UTIL_HASH_H_

#include <cstddef>
#include <cstdint>

namespace TimberSaw {

uint32_t Hash(const char* data, size_t n, uint32_t seed);

}  // namespace TimberSaw

#endif  // STORAGE_TimberSaw_UTIL_HASH_H_
