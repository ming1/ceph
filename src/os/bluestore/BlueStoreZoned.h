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

// BlueStore on zoned devices (HM-SMR, ZNS): class BlueStoreZoned, and what
// BlueStore.cc and BlueStoreZoned.cc share.
//
// BlueStore.h includes this file before class BlueStore, which has a
// BlueStoreZoned as a data member.  So nothing here can use a type that is
// nested in BlueStore.

#include <map>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

#include <boost/dynamic_bitset.hpp>

#include "include/ceph_assert.h"
#include "include/mempool.h"
#include "common/ceph_mutex.h"
#include "common/Thread.h"
#include "common/hobject.h"
#include "kv/KeyValueDB.h"

#include "bluestore_types.h"
#include "BlueStore_fwd.h"

class BlueStore;
class BlueStoreRepairer;
class ZonedAllocator;
class ZonedFreelistManager;

// per-zone cleaner metadata: (zone, offset, object) -> ""
extern const std::string PREFIX_ZONED_CL_INFO;

// BlueStore on a zoned device: the state, and the larger pieces of code.
//
// This is not a layer under BlueStore.  The member functions are steps of
// BlueStore's own code paths (open, mkfs, fsck, the write path, clone,
// rename) that only a zoned device needs, plus the zone cleaner.  They work
// on BlueStore's own data through "bstore", so the class is a friend of
// BlueStore.  BlueStore calls them only when bdev->is_smr() is true.
class BlueStoreZoned {
  BlueStore *bstore;

  using Collection = bluestore::Collection;
  using CollectionRef = bluestore::CollectionRef;
  using BlobRef = bluestore::BlobRef;
  using OnodeRef = bluestore::OnodeRef;
  using Extent = bluestore::Extent;
  using TransContext = bluestore::TransContext;
  using OpSequencer = bluestore::OpSequencer;
  using WriteContext = bluestore::WriteContext;
  // the same type as BlueStore::mempool_dynamic_bitset
  using mempool_dynamic_bitset =
    boost::dynamic_bitset<uint64_t,
			  mempool::bluestore_fsck::pool_allocator<uint64_t>>;

  // the zone cleaner: its thread, and what only that thread runs
  struct CleanerThread : public Thread {
    BlueStoreZoned *zoned;
    explicit CleanerThread(BlueStoreZoned *z) : zoned(z) {}
    void *entry() override {
      zoned->cleaner_thread();
      return nullptr;
    }
  };
  CleanerThread zoned_cleaner_thread;
  ceph::mutex zoned_cleaner_lock = ceph::make_mutex("BlueStoreZoned::zoned_cleaner_lock");
  ceph::condition_variable zoned_cleaner_cond;
  bool zoned_cleaner_started = false;
  bool zoned_cleaner_stop = false;

  void cleaner_thread();
  void clean_zone(
    uint64_t zone,
    ZonedAllocator *a,
    ZonedFreelistManager *f
    );
  void clean_some(ghobject_t oid, uint32_t zone);

  /// zones that init_alloc() found reset on the device but not in the
  /// freelist; post_init_alloc() marks them empty in the freelist
  std::vector<uint64_t> interrupted_cleans;
  /// The rests of allocation units that a write cut off by a crash left
  /// unwritten, as device offset and length.  Found by init_alloc() and
  /// filled by post_init_alloc().
  std::vector<std::pair<uint64_t, uint64_t>> torn_tails;

public:
  explicit BlueStoreZoned(BlueStore *bstore)
    : bstore(bstore), zoned_cleaner_thread(this) {}

  uint64_t zone_size = 0;              ///< size of a zone in bytes
  uint64_t first_sequential_zone = 0;  ///< first SMR zone that is sequential-only

  /// Held from the allocation of a write to its submit, so that the writes
  /// to a sequential zone reach the device in the order of their offsets.
  ceph::mutex atomic_alloc_and_submit_lock =
      ceph::make_mutex("BlueStoreZoned::atomic_alloc_and_submit_lock");

  int create_shared_alloc(uint64_t alloc_size);
  int init_alloc(std::map<uint64_t, uint64_t> *zone_adjustments);
  void post_init_alloc(const std::map<uint64_t, uint64_t>& zone_adjustments);
  /// mkfs: read the zone layout of the device and reset the zones (unless
  /// bluestore_assume_clean_zones is set)
  void mkfs_prepare();
  /// mkfs: give BlueFS the conventional region
  void mkfs_init_main_device(uint64_t reserved);
  void mkfs_super(KeyValueDB::Transaction& t);
  void fsck_note_blob(
    const bluestore_blob_t& blob,
    std::map<uint32_t, uint64_t>& zone_first_offsets);
  void fsck_check_refs(
    OnodeRef& o,
    const std::map<uint32_t, uint64_t>& zone_first_offsets,
    std::vector<std::unordered_map<ghobject_t, uint64_t>> *zone_refs,
    BlueStoreRepairer *repairer,
    int64_t& errors);
  void fsck_check_stray_refs(
    const std::vector<std::unordered_map<ghobject_t, uint64_t>>& zone_refs,
    BlueStoreRepairer* repairer,
    int64_t& errors);
  void fsck_load(
    bool shallow,
    bool repair,
    BlueStoreRepairer& repairer,
    std::vector<std::unordered_map<ghobject_t, uint64_t>>& zone_refs,
    int64_t& errors);
  void fsck_check_zones(
    mempool_dynamic_bitset& used_blocks,
    uint64_t alloc_size,
    BlueStoreRepairer* repairer,
    int64_t& errors);
  void txc_finalize_kv(TransContext *txc);
  void cleaner_start();
  void cleaner_stop();
  void gc_whole_units(
    OnodeRef& o,
    const interval_set<uint64_t>& extents,
    interval_set<uint64_t> *units);
  void do_write_small(
    CollectionRef& c,
    OnodeRef& o,
    uint64_t offset, uint64_t length,
    uint64_t alloc_len,
    ceph::buffer::list& bl,
    WriteContext *wctx);
  void wctx_note_writes(
    TransContext *txc,
    OnodeRef& o,
    WriteContext *wctx);
  void wctx_drop_refs(
    TransContext *txc,
    OnodeRef& o,
    std::set<uint32_t>& zones_with_releases);
  void update_zone_refs(
    TransContext *txc, OnodeRef& o,
    const PExtentVector& allocated,
    const PExtentVector& released);
  void clone_range_refs(
    TransContext *txc,
    OnodeRef& newo,
    uint64_t dstoff, uint64_t length);
  void rename_refs(
    TransContext *txc,
    OnodeRef& oldo,
    OnodeRef& newo);
};

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
// so the member functions of BlueStoreZoned are never called.  They are
// inline stubs here, and BlueStoreZoned.cc is not built.
#define BLUESTORE_ZONED_STUB \
  ceph_abort_msg("BlueStore was built without zoned (libzbd) support")

inline int BlueStoreZoned::create_shared_alloc(uint64_t alloc_size)
{
  BLUESTORE_ZONED_STUB;
}

inline int BlueStoreZoned::init_alloc(std::map<uint64_t, uint64_t> *zone_adjustments)
{
  BLUESTORE_ZONED_STUB;
}

inline void BlueStoreZoned::post_init_alloc(const std::map<uint64_t, uint64_t>& zone_adjustments)
{
  BLUESTORE_ZONED_STUB;
}

inline void BlueStoreZoned::mkfs_prepare()
{
  BLUESTORE_ZONED_STUB;
}

inline void BlueStoreZoned::mkfs_init_main_device(uint64_t reserved)
{
  BLUESTORE_ZONED_STUB;
}

inline void BlueStoreZoned::mkfs_super(KeyValueDB::Transaction& t)
{
  BLUESTORE_ZONED_STUB;
}

inline void BlueStoreZoned::fsck_note_blob(
  const bluestore_blob_t& blob,
  std::map<uint32_t, uint64_t>& zone_first_offsets)
{
  BLUESTORE_ZONED_STUB;
}

inline void BlueStoreZoned::fsck_check_refs(
  OnodeRef& o,
  const std::map<uint32_t, uint64_t>& zone_first_offsets,
  std::vector<std::unordered_map<ghobject_t, uint64_t>> *zone_refs,
  BlueStoreRepairer *repairer,
  int64_t& errors)
{
  BLUESTORE_ZONED_STUB;
}

inline void BlueStoreZoned::fsck_check_stray_refs(
  const std::vector<std::unordered_map<ghobject_t, uint64_t>>& zone_refs,
  BlueStoreRepairer* repairer,
  int64_t& errors)
{
  BLUESTORE_ZONED_STUB;
}

inline void BlueStoreZoned::fsck_load(
  bool shallow,
  bool repair,
  BlueStoreRepairer& repairer,
  std::vector<std::unordered_map<ghobject_t, uint64_t>>& zone_refs,
  int64_t& errors)
{
  BLUESTORE_ZONED_STUB;
}

inline void BlueStoreZoned::fsck_check_zones(
  mempool_dynamic_bitset& used_blocks,
  uint64_t alloc_size,
  BlueStoreRepairer* repairer,
  int64_t& errors)
{
  BLUESTORE_ZONED_STUB;
}

inline void BlueStoreZoned::txc_finalize_kv(TransContext *txc)
{
  BLUESTORE_ZONED_STUB;
}

inline void BlueStoreZoned::cleaner_start()
{
  BLUESTORE_ZONED_STUB;
}

inline void BlueStoreZoned::cleaner_stop()
{
  BLUESTORE_ZONED_STUB;
}

inline void BlueStoreZoned::cleaner_thread()
{
  BLUESTORE_ZONED_STUB;
}

inline void BlueStoreZoned::clean_zone(
  uint64_t zone,
  ZonedAllocator *a,
  ZonedFreelistManager *f
  )
{
  BLUESTORE_ZONED_STUB;
}

inline void BlueStoreZoned::clean_some(ghobject_t oid, uint32_t zone)
{
  BLUESTORE_ZONED_STUB;
}

inline void BlueStoreZoned::gc_whole_units(
  OnodeRef& o,
  const interval_set<uint64_t>& extents,
  interval_set<uint64_t> *units)
{
  BLUESTORE_ZONED_STUB;
}

inline void BlueStoreZoned::do_write_small(
  CollectionRef& c,
  OnodeRef& o,
  uint64_t offset, uint64_t length,
  uint64_t alloc_len,
  ceph::buffer::list& bl,
  WriteContext *wctx)
{
  BLUESTORE_ZONED_STUB;
}

inline void BlueStoreZoned::wctx_note_writes(
  TransContext *txc,
  OnodeRef& o,
  WriteContext *wctx)
{
  BLUESTORE_ZONED_STUB;
}

inline void BlueStoreZoned::wctx_drop_refs(
  TransContext *txc,
  OnodeRef& o,
  std::set<uint32_t>& zones_with_releases)
{
  BLUESTORE_ZONED_STUB;
}

inline void BlueStoreZoned::update_zone_refs(TransContext *txc, OnodeRef& o,
				       const PExtentVector& allocated,
				       const PExtentVector& released)
{
  BLUESTORE_ZONED_STUB;
}

inline void BlueStoreZoned::clone_range_refs(
  TransContext *txc,
  OnodeRef& newo,
  uint64_t dstoff, uint64_t length)
{
  BLUESTORE_ZONED_STUB;
}

inline void BlueStoreZoned::rename_refs(
  TransContext *txc,
  OnodeRef& oldo,
  OnodeRef& newo)
{
  BLUESTORE_ZONED_STUB;
}

#undef BLUESTORE_ZONED_STUB

#endif // HAVE_LIBZBD

#endif
