// Copyright (c) 2017 The TimberSaw Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_TimberSaw_INCLUDE_EXPORT_H_
#define STORAGE_TimberSaw_INCLUDE_EXPORT_H_

#if !defined(TimberSaw_EXPORT)

#if defined(TimberSaw_SHARED_LIBRARY)
#if defined(_WIN32)

#if defined(TimberSaw_COMPILE_LIBRARY)
#define TimberSaw_EXPORT __declspec(dllexport)
#else
#define TimberSaw_EXPORT __declspec(dllimport)
#endif  // defined(TimberSaw_COMPILE_LIBRARY)

#else  // defined(_WIN32)
#if defined(TimberSaw_COMPILE_LIBRARY)
#define TimberSaw_EXPORT __attribute__((visibility("default")))
#else
#define TimberSaw_EXPORT
#endif
#endif  // defined(_WIN32)

#else  // defined(TimberSaw_SHARED_LIBRARY)
#define TimberSaw_EXPORT
#endif

#endif  // !defined(TimberSaw_EXPORT)

#endif  // STORAGE_TimberSaw_INCLUDE_EXPORT_H_
