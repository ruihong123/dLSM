/* Copyright (c) 2011 The TimberSaw Authors. All rights reserved.
  Use of this source code is governed by a BSD-style license that can be
  found in the LICENSE file. See the AUTHORS file for names of contributors.

  C bindings for TimberSaw.  May be useful as a stable ABI that can be
  used by programs that keep TimberSaw in a shared library, or for
  a JNI api.

  Does not support:
  . getters for the option types
  . custom comparators that implement key shortening
  . custom iter, db, env, table_cache implementations using just the C bindings

  Some conventions:

  (1) We expose just opaque struct pointers and functions to clients.
  This allows us to change internal representations without having to
  recompile clients.

  (2) For simplicity, there is no equivalent to the Slice type.  Instead,
  the caller has to pass the pointer and length as separate
  arguments.

  (3) Errors are represented by a null-terminated c string.  NULL
  means no error.  All operations that can raise an error are passed
  a "char** errptr" as the last argument.  One of the following must
  be true on entry:
     *errptr == NULL
     *errptr points to a malloc()ed null-terminated error message
       (On Windows, *errptr must have been malloc()-ed by this library.)
  On success, a TimberSaw routine leaves *errptr unchanged.
  On failure, TimberSaw frees the old value of *errptr and
  set *errptr to a malloc()ed error message.

  (4) Bools have the type uint8_t (0 == false; rest == true)

  (5) All of the pointer arguments must be non-NULL.
*/

#ifndef STORAGE_TimberSaw_INCLUDE_C_H_
#define STORAGE_TimberSaw_INCLUDE_C_H_

#ifdef __cplusplus
extern "C" {
#endif

#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>

#include "TimberSaw/export.h"

/* Exported types */

typedef struct TimberSaw_t TimberSaw_t;
typedef struct TimberSaw_cache_t TimberSaw_cache_t;
typedef struct TimberSaw_comparator_t TimberSaw_comparator_t;
typedef struct TimberSaw_env_t TimberSaw_env_t;
typedef struct TimberSaw_filelock_t TimberSaw_filelock_t;
typedef struct TimberSaw_filterpolicy_t TimberSaw_filterpolicy_t;
typedef struct TimberSaw_iterator_t TimberSaw_iterator_t;
typedef struct TimberSaw_logger_t TimberSaw_logger_t;
typedef struct TimberSaw_options_t TimberSaw_options_t;
typedef struct TimberSaw_randomfile_t TimberSaw_randomfile_t;
typedef struct TimberSaw_readoptions_t TimberSaw_readoptions_t;
typedef struct TimberSaw_seqfile_t TimberSaw_seqfile_t;
typedef struct TimberSaw_snapshot_t TimberSaw_snapshot_t;
typedef struct TimberSaw_writablefile_t TimberSaw_writablefile_t;
typedef struct TimberSaw_writebatch_t TimberSaw_writebatch_t;
typedef struct TimberSaw_writeoptions_t TimberSaw_writeoptions_t;

/* DB operations */

TimberSaw_EXPORT TimberSaw_t* TimberSaw_open(const TimberSaw_options_t* options,
                                       const char* name, char** errptr);

TimberSaw_EXPORT void TimberSaw_close(TimberSaw_t* db);

TimberSaw_EXPORT void TimberSaw_put(TimberSaw_t* db,
                                const TimberSaw_writeoptions_t* options,
                                const char* key, size_t keylen, const char* val,
                                size_t vallen, char** errptr);

TimberSaw_EXPORT void TimberSaw_delete(TimberSaw_t* db,
                                   const TimberSaw_writeoptions_t* options,
                                   const char* key, size_t keylen,
                                   char** errptr);

TimberSaw_EXPORT void TimberSaw_write(TimberSaw_t* db,
                                  const TimberSaw_writeoptions_t* options,
                                  TimberSaw_writebatch_t* batch, char** errptr);

/* Returns NULL if not found.  A malloc()ed array otherwise.
   Stores the length of the array in *vallen. */
TimberSaw_EXPORT char* TimberSaw_get(TimberSaw_t* db,
                                 const TimberSaw_readoptions_t* options,
                                 const char* key, size_t keylen, size_t* vallen,
                                 char** errptr);

TimberSaw_EXPORT TimberSaw_iterator_t* TimberSaw_create_iterator(
    TimberSaw_t* db, const TimberSaw_readoptions_t* options);

TimberSaw_EXPORT const TimberSaw_snapshot_t* TimberSaw_create_snapshot(TimberSaw_t* db);

TimberSaw_EXPORT void TimberSaw_release_snapshot(
    TimberSaw_t* db, const TimberSaw_snapshot_t* snapshot);

/* Returns NULL if property name is unknown.
   Else returns a pointer to a malloc()-ed null-terminated value. */
TimberSaw_EXPORT char* TimberSaw_property_value(TimberSaw_t* db,
                                            const char* propname);

TimberSaw_EXPORT void TimberSaw_approximate_sizes(
    TimberSaw_t* db, int num_ranges, const char* const* range_start_key,
    const size_t* range_start_key_len, const char* const* range_limit_key,
    const size_t* range_limit_key_len, uint64_t* sizes);

TimberSaw_EXPORT void TimberSaw_compact_range(TimberSaw_t* db, const char* start_key,
                                          size_t start_key_len,
                                          const char* limit_key,
                                          size_t limit_key_len);

/* Management operations */

TimberSaw_EXPORT void TimberSaw_destroy_db(const TimberSaw_options_t* options,
                                       const char* name, char** errptr);

TimberSaw_EXPORT void TimberSaw_repair_db(const TimberSaw_options_t* options,
                                      const char* name, char** errptr);

/* Iterator */

TimberSaw_EXPORT void TimberSaw_iter_destroy(TimberSaw_iterator_t*);
TimberSaw_EXPORT uint8_t TimberSaw_iter_valid(const TimberSaw_iterator_t*);
TimberSaw_EXPORT void TimberSaw_iter_seek_to_first(TimberSaw_iterator_t*);
TimberSaw_EXPORT void TimberSaw_iter_seek_to_last(TimberSaw_iterator_t*);
TimberSaw_EXPORT void TimberSaw_iter_seek(TimberSaw_iterator_t*, const char* k,
                                      size_t klen);
TimberSaw_EXPORT void TimberSaw_iter_next(TimberSaw_iterator_t*);
TimberSaw_EXPORT void TimberSaw_iter_prev(TimberSaw_iterator_t*);
TimberSaw_EXPORT const char* TimberSaw_iter_key(const TimberSaw_iterator_t*,
                                            size_t* klen);
TimberSaw_EXPORT const char* TimberSaw_iter_value(const TimberSaw_iterator_t*,
                                              size_t* vlen);
TimberSaw_EXPORT void TimberSaw_iter_get_error(const TimberSaw_iterator_t*,
                                           char** errptr);

/* Write batch */

TimberSaw_EXPORT TimberSaw_writebatch_t* TimberSaw_writebatch_create(void);
TimberSaw_EXPORT void TimberSaw_writebatch_destroy(TimberSaw_writebatch_t*);
TimberSaw_EXPORT void TimberSaw_writebatch_clear(TimberSaw_writebatch_t*);
TimberSaw_EXPORT void TimberSaw_writebatch_put(TimberSaw_writebatch_t*,
                                           const char* key, size_t klen,
                                           const char* val, size_t vlen);
TimberSaw_EXPORT void TimberSaw_writebatch_delete(TimberSaw_writebatch_t*,
                                              const char* key, size_t klen);
TimberSaw_EXPORT void TimberSaw_writebatch_iterate(
    const TimberSaw_writebatch_t*, void* state,
    void (*put)(void*, const char* k, size_t klen, const char* v, size_t vlen),
    void (*deleted)(void*, const char* k, size_t klen));
TimberSaw_EXPORT void TimberSaw_writebatch_append(
    TimberSaw_writebatch_t* destination, const TimberSaw_writebatch_t* source);

/* Options */

TimberSaw_EXPORT TimberSaw_options_t* TimberSaw_options_create(void);
TimberSaw_EXPORT void TimberSaw_options_destroy(TimberSaw_options_t*);
TimberSaw_EXPORT void TimberSaw_options_set_comparator(TimberSaw_options_t*,
                                                   TimberSaw_comparator_t*);
TimberSaw_EXPORT void TimberSaw_options_set_filter_policy(TimberSaw_options_t*,
                                                      TimberSaw_filterpolicy_t*);
TimberSaw_EXPORT void TimberSaw_options_set_create_if_missing(TimberSaw_options_t*,
                                                          uint8_t);
TimberSaw_EXPORT void TimberSaw_options_set_error_if_exists(TimberSaw_options_t*,
                                                        uint8_t);
TimberSaw_EXPORT void TimberSaw_options_set_paranoid_checks(TimberSaw_options_t*,
                                                        uint8_t);
TimberSaw_EXPORT void TimberSaw_options_set_env(TimberSaw_options_t*, TimberSaw_env_t*);
TimberSaw_EXPORT void TimberSaw_options_set_info_log(TimberSaw_options_t*,
                                                 TimberSaw_logger_t*);
TimberSaw_EXPORT void TimberSaw_options_set_write_buffer_size(TimberSaw_options_t*,
                                                          size_t);
TimberSaw_EXPORT void TimberSaw_options_set_max_open_files(TimberSaw_options_t*, int);
TimberSaw_EXPORT void TimberSaw_options_set_cache(TimberSaw_options_t*,
                                              TimberSaw_cache_t*);
TimberSaw_EXPORT void TimberSaw_options_set_block_size(TimberSaw_options_t*, size_t);
TimberSaw_EXPORT void TimberSaw_options_set_block_restart_interval(
    TimberSaw_options_t*, int);
TimberSaw_EXPORT void TimberSaw_options_set_max_file_size(TimberSaw_options_t*,
                                                      size_t);

enum { TimberSaw_no_compression = 0, TimberSaw_snappy_compression = 1 };
TimberSaw_EXPORT void TimberSaw_options_set_compression(TimberSaw_options_t*, int);

/* Comparator */

TimberSaw_EXPORT TimberSaw_comparator_t* TimberSaw_comparator_create(
    void* state, void (*destructor)(void*),
    int (*compare)(void*, const char* a, size_t alen, const char* b,
                   size_t blen),
    const char* (*name)(void*));
TimberSaw_EXPORT void TimberSaw_comparator_destroy(TimberSaw_comparator_t*);

/* Filter policy */

TimberSaw_EXPORT TimberSaw_filterpolicy_t* TimberSaw_filterpolicy_create(
    void* state, void (*destructor)(void*),
    char* (*create_filter)(void*, const char* const* key_array,
                           const size_t* key_length_array, int num_keys,
                           size_t* filter_length),
    uint8_t (*key_may_match)(void*, const char* key, size_t length,
                             const char* filter, size_t filter_length),
    const char* (*name)(void*));
TimberSaw_EXPORT void TimberSaw_filterpolicy_destroy(TimberSaw_filterpolicy_t*);

TimberSaw_EXPORT TimberSaw_filterpolicy_t* TimberSaw_filterpolicy_create_bloom(
    int bits_per_key);

/* Read options */

TimberSaw_EXPORT TimberSaw_readoptions_t* TimberSaw_readoptions_create(void);
TimberSaw_EXPORT void TimberSaw_readoptions_destroy(TimberSaw_readoptions_t*);
TimberSaw_EXPORT void TimberSaw_readoptions_set_verify_checksums(
    TimberSaw_readoptions_t*, uint8_t);
TimberSaw_EXPORT void TimberSaw_readoptions_set_fill_cache(TimberSaw_readoptions_t*,
                                                       uint8_t);
TimberSaw_EXPORT void TimberSaw_readoptions_set_snapshot(TimberSaw_readoptions_t*,
                                                     const TimberSaw_snapshot_t*);

/* Write options */

TimberSaw_EXPORT TimberSaw_writeoptions_t* TimberSaw_writeoptions_create(void);
TimberSaw_EXPORT void TimberSaw_writeoptions_destroy(TimberSaw_writeoptions_t*);
TimberSaw_EXPORT void TimberSaw_writeoptions_set_sync(TimberSaw_writeoptions_t*,
                                                  uint8_t);

/* Cache */

TimberSaw_EXPORT TimberSaw_cache_t* TimberSaw_cache_create_lru(size_t capacity);
TimberSaw_EXPORT void TimberSaw_cache_destroy(TimberSaw_cache_t* cache);

/* Env */

TimberSaw_EXPORT TimberSaw_env_t* TimberSaw_create_default_env(void);
TimberSaw_EXPORT void TimberSaw_env_destroy(TimberSaw_env_t*);

/* If not NULL, the returned buffer must be released using TimberSaw_free(). */
TimberSaw_EXPORT char* TimberSaw_env_get_test_directory(TimberSaw_env_t*);

/* Utility */

/* Calls free(ptr).
   REQUIRES: ptr was malloc()-ed and returned by one of the routines
   in this file.  Note that in certain cases (typically on Windows), you
   may need to call this routine instead of free(ptr) to dispose of
   malloc()-ed memory returned by this library. */
TimberSaw_EXPORT void TimberSaw_free(void* ptr);

/* Return the major version number for this release. */
TimberSaw_EXPORT int TimberSaw_major_version(void);

/* Return the minor version number for this release. */
TimberSaw_EXPORT int TimberSaw_minor_version(void);

#ifdef __cplusplus
} /* end extern "C" */
#endif

#endif /* STORAGE_TimberSaw_INCLUDE_C_H_ */
