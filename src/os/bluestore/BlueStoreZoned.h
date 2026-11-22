// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_OS_BLUESTORE_BLUESTOREZONED_H
#define CEPH_OS_BLUESTORE_BLUESTOREZONED_H

// BlueStore on zoned devices (HM-SMR, ZNS): what BlueStore.cc and
// BlueStoreZoned.cc share.  BlueStore.h includes this file at its end.

#include <string>

#include "BlueStore.h"

// per-zone cleaner metadata: (zone, offset, object) -> ""
extern const std::string PREFIX_ZONED_CL_INFO;

#ifdef HAVE_LIBZBD

void get_zone_offset_object_key(
  uint32_t zone,
  uint64_t offset,
  ghobject_t oid,
  std::string *key);

int get_key_zone_offset_object(
  const std::string& key,
  uint32_t *zone,
  uint64_t *offset,
  ghobject_t *oid);

#else // HAVE_LIBZBD

// Zoned devices need libzbd.  Without it no block device reports is_smr(),
// so the zoned member functions of BlueStore are never called.  They are
// inline stubs here, and BlueStoreZoned.cc is not built.
#define BLUESTORE_ZONED_STUB \
  ceph_abort_msg("BlueStore was built without zoned (libzbd) support")

inline int BlueStore::_zoned_create_shared_alloc(uint64_t alloc_size)
{
  BLUESTORE_ZONED_STUB;
}

inline int BlueStore::_zoned_init_alloc(std::map<uint64_t, uint64_t> *zone_adjustments)
{
  BLUESTORE_ZONED_STUB;
}

inline void BlueStore::_zoned_post_init_alloc(const std::map<uint64_t, uint64_t>& zone_adjustments)
{
  BLUESTORE_ZONED_STUB;
}

inline void BlueStore::_zoned_mkfs_super(KeyValueDB::Transaction& t)
{
  BLUESTORE_ZONED_STUB;
}

inline void BlueStore::_zoned_fsck_note_blob(
  const bluestore_blob_t& blob,
  std::map<uint32_t, uint64_t>& zone_first_offsets)
{
  BLUESTORE_ZONED_STUB;
}

inline void BlueStore::_zoned_fsck_check_refs(
  const ghobject_t& oid,
  const std::map<uint32_t, uint64_t>& zone_first_offsets,
  const FSCK_ObjectCtx& ctx,
  int64_t& errors)
{
  BLUESTORE_ZONED_STUB;
}

inline void BlueStore::_zoned_fsck_load(
  FSCKDepth depth,
  bool repair,
  BlueStoreRepairer& repairer,
  std::vector<std::unordered_map<ghobject_t, uint64_t>>& zone_refs,
  int64_t& errors)
{
  BLUESTORE_ZONED_STUB;
}

inline void BlueStore::_zoned_fsck_check_zones(
  mempool_dynamic_bitset& used_blocks,
  uint64_t alloc_size,
  int64_t& errors)
{
  BLUESTORE_ZONED_STUB;
}

inline void BlueStore::_zoned_txc_finalize_kv(TransContext *txc)
{
  BLUESTORE_ZONED_STUB;
}

inline void BlueStore::_zoned_cleaner_start()
{
  BLUESTORE_ZONED_STUB;
}

inline void BlueStore::_zoned_cleaner_stop()
{
  BLUESTORE_ZONED_STUB;
}

inline void BlueStore::_zoned_cleaner_thread()
{
  BLUESTORE_ZONED_STUB;
}

inline void BlueStore::_zoned_clean_zone(
  uint64_t zone,
  ZonedAllocator *a,
  ZonedFreelistManager *f
  )
{
  BLUESTORE_ZONED_STUB;
}

inline void BlueStore::_clean_some(ghobject_t oid, uint32_t zone)
{
  BLUESTORE_ZONED_STUB;
}

inline void BlueStore::_zoned_do_write_small(
  CollectionRef& c,
  OnodeRef& o,
  uint64_t offset, uint64_t length,
  uint64_t alloc_len,
  ceph::buffer::list& bl,
  WriteContext *wctx)
{
  BLUESTORE_ZONED_STUB;
}

inline void BlueStore::_zoned_wctx_note_writes(
  TransContext *txc,
  OnodeRef& o,
  WriteContext *wctx)
{
  BLUESTORE_ZONED_STUB;
}

inline void BlueStore::_zoned_wctx_drop_refs(
  TransContext *txc,
  OnodeRef& o,
  std::set<uint32_t>& zones_with_releases)
{
  BLUESTORE_ZONED_STUB;
}

inline void BlueStore::_zoned_clone_range_refs(
  TransContext *txc,
  OnodeRef& newo,
  uint64_t dstoff, uint64_t length)
{
  BLUESTORE_ZONED_STUB;
}

inline void BlueStore::_zoned_rename_refs(
  TransContext *txc,
  OnodeRef& oldo,
  OnodeRef& newo)
{
  BLUESTORE_ZONED_STUB;
}

#undef BLUESTORE_ZONED_STUB

#endif // HAVE_LIBZBD

#endif
