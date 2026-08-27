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

// BlueStore on zoned devices (HM-SMR, ZNS).
//
// The member functions of BlueStoreZoned.  BlueStore calls them only when
// bdev->is_smr() is true.  They live here so that BlueStore.cc needs almost
// no "#ifdef HAVE_LIBZBD".  This file is built only with libzbd (WITH_ZBD).
// Without it no block device reports is_smr(), and BlueStoreZoned.h has
// inline stubs instead.

#include "BlueStore.h"
#include "BlueStoreZoned.h"
#include "BlueStore_objects.h"
#include "BlueStore_inlines.h"
#include "BlueStore_components.h"
#include "bluestore_common.h"
#include "Allocator.h"
#include "FreelistManager.h"
#include "ZonedAllocator.h"
#include "ZonedFreelistManager.h"
#include "common/debug.h"
#include "common/errno.h"
#include "common/pretty_binary.h"
#include "include/intarith.h"

#define dout_context bstore->cct
#define dout_subsys ceph_subsys_bluestore
#undef dout_prefix
#define dout_prefix *_dout << "bluestore(" << bstore->path << ") "

using std::map;
using std::set;
using std::string;
using std::vector;
using ceph::bufferlist;
using ceph::encode;

int BlueStoreZoned::create_shared_alloc(uint64_t alloc_size)
{
  // The conventional region of a zoned device is randomly writable and
  // hosts BlueFS/metadata, so it must use a regular allocator -- never the
  // sequential-only "zoned" allocator (which would hand out the reserved
  // bdev label/superblock region at offset 0).
  std::string conventional_allocator_type = bstore->cct->_conf->bluestore_allocator;
  if (conventional_allocator_type == "zoned") {
    conventional_allocator_type = "hybrid";
  }
  Allocator *a = Allocator::create(
    bstore->cct, conventional_allocator_type,
    bstore->bdev->get_conventional_region_size(),
    alloc_size,
    zone_size, 0,
    "zoned_block");
  if (!a) {
    lderr(bstore->cct) << __func__ << " failed to create " << conventional_allocator_type
	       << " allocator" << dendl;
    delete bstore->alloc;
    return -EINVAL;
  }
  bstore->shared_alloc.set(a, alloc_size);
  return 0;
}

int BlueStoreZoned::init_alloc(std::map<uint64_t, uint64_t> *zone_adjustments)
{
  auto a = dynamic_cast<ZonedAllocator*>(bstore->alloc);
  ceph_assert(a);
  auto f = dynamic_cast<ZonedFreelistManager*>(bstore->fm);
  ceph_assert(f);
  vector<uint64_t> wp = bstore->bdev->get_zones();
  vector<zone_state_t> zones = f->get_zone_states(bstore->db);
  ceph_assert(wp.size() == zones.size());

  // reconcile zone state
  auto num_zones = bstore->bdev->get_size() / zone_size;
  for (unsigned i = first_sequential_zone; i < num_zones; ++i) {
    ceph_assert(wp[i] >= i * zone_size);
    ceph_assert(wp[i] <= (i + 1) * zone_size); // pos might be at start of next zone
    uint64_t p = wp[i] - i * zone_size;
    if (zones[i].write_pointer > p) {
      derr << __func__ << " zone 0x" << std::hex << i
	   << " bluestore write pointer 0x" << zones[i].write_pointer
	   << " > device write pointer 0x" << p
	   << std::dec << " -- VERY SUSPICIOUS!" << dendl;
    } else if (zones[i].write_pointer < p) {
      // this is "normal" in that it can happen after any crash (if we have a
      // write in flight but did not manage to commit the transaction)
      auto delta = p - zones[i].write_pointer;
      dout(1) << __func__ << " zone 0x" << std::hex << i
	       << " device write pointer 0x" << p
	       << " > bluestore pointer 0x" << zones[i].write_pointer
	       << ", advancing 0x" << delta << std::dec << dendl;
      (*zone_adjustments)[zones[i].write_pointer] = delta;
      zones[i].num_dead_bytes += delta;
      zones[i].write_pointer = p;
    }
  }

  // start with conventional zone "free" (bluefs may adjust this when it starts up)
  auto reserved = bstore->_get_ondisk_reserved();
  // for now we require a conventional zone
  ceph_assert(bstore->bdev->get_conventional_region_size());
  ceph_assert(bstore->shared_alloc.a != bstore->alloc);  // zoned allocator doesn't use conventional region
  bstore->shared_alloc.a->init_add_free(
    reserved,
    p2align(bstore->bdev->get_conventional_region_size(), bstore->min_alloc_size) - reserved);

  // init sequential zone based on the device's write pointers
  a->init_from_zone_pointers(std::move(zones));
  dout(1) << __func__
	  << " loaded zone pointers: "
	  << std::hex
	  << ", allocator type " << bstore->alloc->get_type()
	  << ", capacity 0x" << bstore->alloc->get_capacity()
	  << ", block size 0x" << bstore->alloc->get_block_size()
	  << ", free 0x" << bstore->alloc->get_free()
	  << ", fragmentation " << bstore->alloc->get_fragmentation()
	  << std::dec << dendl;

  return 0;
}

void BlueStoreZoned::post_init_alloc(const std::map<uint64_t, uint64_t>& zone_adjustments)
{
  int r = 0;
  if (zone_adjustments.empty()) {
    return;
  }
  dout(1) << __func__ << " adjusting freelist based on device write pointers" << dendl;
  auto f = dynamic_cast<ZonedFreelistManager*>(bstore->fm);
  ceph_assert(f);
  KeyValueDB::Transaction t = bstore->db->get_transaction();
  for (auto& i : zone_adjustments) {
    // allocate AND release since this gap is now dead space
    // note that the offset is imprecise, but only need to select the zone
    f->allocate(i.first, i.second, t);
    f->release(i.first, i.second, t);
  }
  r = bstore->db->submit_transaction_sync(t);
  ceph_assert(r >= 0);
}

void BlueStoreZoned::mkfs_prepare()
{
  auto bdev = bstore->bdev;
  zone_size = bdev->get_zone_size();
  first_sequential_zone = bdev->get_conventional_region_size() / zone_size;
  bdev->reset_all_zones();
}

void BlueStoreZoned::mkfs_init_main_device(uint64_t reserved)
{
  auto bdev = bstore->bdev;
  if (bstore->alloc != bstore->shared_alloc.a) {
    bstore->shared_alloc.a->init_add_free(
      reserved,
      p2align(bdev->get_conventional_region_size(),
	      bstore->min_alloc_size) - reserved);
  }
}

void BlueStoreZoned::mkfs_super(KeyValueDB::Transaction& t)
{
  {
    bufferlist bl;
    encode((uint64_t)zone_size, bl);
    t->set(PREFIX_SUPER, "zone_size", bl);
  }
  {
    bufferlist bl;
    encode((uint64_t)first_sequential_zone, bl);
    t->set(PREFIX_SUPER, "first_sequential_zone", bl);
  }
}

void BlueStoreZoned::fsck_note_blob(
  const bluestore_blob_t& blob,
  std::map<uint32_t, uint64_t>& zone_first_offsets)
{
  for (auto& e : blob.get_extents()) {
    if (e.is_valid()) {
      uint32_t zone = e.offset / zone_size;
      uint64_t offset = e.offset % zone_size;
      auto p = zone_first_offsets.find(zone);
      if (p == zone_first_offsets.end() || p->second > offset) {
	// FIXME: use interator for guided insert?
	zone_first_offsets[zone] = offset;
      }
    }
  }
}

void BlueStoreZoned::fsck_check_refs(
  const ghobject_t& oid,
  const std::map<uint32_t, uint64_t>& zone_first_offsets,
  std::vector<std::unordered_map<ghobject_t, uint64_t>> *zone_refs,
  int64_t& errors)
{
  for (auto& [zone, first_offset] : zone_first_offsets) {
    auto p = (*zone_refs)[zone].find(oid);
    if (p != (*zone_refs)[zone].end()) {
      if (first_offset < p->second) {
	dout(20) << " slightly wonky zone ref 0x" << std::hex << zone
	     << " offset 0x" << p->second
	     << " but first offset is 0x" << first_offset
	     << "; this can happen due to clone_range"
	     << dendl;
      } else {
	dout(20) << " good zone ref 0x" << std::hex << zone << " offset 0x" << p->second
		 << " <= first offset 0x" << first_offset
		 << std::dec << dendl;
      }
      (*zone_refs)[zone].erase(p);
    } else {
      derr << "fsck error: " << oid << " references zone 0x" << std::hex << zone
	   << " but there is no zone ref" << std::dec << dendl;
      // FIXME: add repair
      ++errors;
    }
  }
}

void BlueStoreZoned::fsck_load(
  bool shallow,
  bool repair,
  BlueStoreRepairer& repairer,
  std::vector<std::unordered_map<ghobject_t, uint64_t>>& zone_refs,
  int64_t& errors)
{
  KeyValueDB::Iterator it;
  auto a = dynamic_cast<ZonedAllocator*>(bstore->alloc);
  ceph_assert(a);
  auto f = dynamic_cast<ZonedFreelistManager*>(bstore->fm);
  ceph_assert(f);
  vector<uint64_t> wp = bstore->bdev->get_zones();
  vector<zone_state_t> zones = f->get_zone_states(bstore->db);
  ceph_assert(wp.size() == zones.size());
  auto num_zones = bstore->bdev->get_size() / zone_size;
  for (unsigned i = first_sequential_zone; i < num_zones; ++i) {
    uint64_t p = wp[i] == (i + 1) * zone_size ? zone_size : wp[i] % zone_size;
    if (zones[i].write_pointer > p &&
	zones[i].num_dead_bytes < zones[i].write_pointer) {
      derr << "fsck error: zone 0x" << std::hex << i
	   << " bluestore write pointer 0x" << zones[i].write_pointer
	   << " > device write pointer 0x" << p
	   << " (with only 0x" << zones[i].num_dead_bytes << " dead bytes)"
	   << std::dec << dendl;
      ++errors;
    }
  }

  if (!shallow) {
    // load zone refs
    zone_refs.resize(bstore->bdev->get_size() / zone_size);
    it = bstore->db->get_iterator(PREFIX_ZONED_CL_INFO, KeyValueDB::ITERATOR_NOCACHE);
    if (it) {
      for (it->lower_bound(string());
	   it->valid();
	   it->next()) {
	uint32_t zone = 0;
	uint64_t offset = 0;
	ghobject_t oid;
	string key = it->key();
	int r = get_key_zone_offset_object(key, &zone, &offset, &oid);
	if (r < 0) {
	  derr << "fsck error: invalid zone ref key " << pretty_binary_string(key)
	       << dendl;
	  if (repair) {
	    repairer.remove_key(bstore->db, PREFIX_ZONED_CL_INFO, key);
	  }
	  ++errors;
	  continue;
	}
	dout(30) << " zone ref 0x" << std::hex << zone << " offset 0x" << offset
		 << " -> " << std::dec << oid << dendl;
	if (zone_refs[zone].count(oid)) {
	  derr << "fsck error: second zone ref in zone 0x" << std::hex << zone
	       << " offset 0x" << offset << std::dec << " for " << oid << dendl;
	  if (repair) {
	    repairer.remove_key(bstore->db, PREFIX_ZONED_CL_INFO, key);
	  }
	  ++errors;
	  continue;
	}
	zone_refs[zone][oid] = offset;
      }
    }
  }
}

void BlueStoreZoned::fsck_check_zones(
  mempool_dynamic_bitset& used_blocks,
  uint64_t alloc_size,
  int64_t& errors)
{
  // verify per-zone state
  //  - verify no allocations beyond write pointer
  //  - verify num_dead_bytes count (neither allocated nor
  //    free space past the write pointer)
  auto a = dynamic_cast<ZonedAllocator*>(bstore->alloc);
  auto num_zones = bstore->bdev->get_size() / zone_size;

  // mark the free space past the write pointer
  for (uint32_t zone = first_sequential_zone; zone < num_zones; ++zone) {
    auto wp = a->get_write_pointer(zone);
    uint64_t offset = zone_size * zone + wp;
    uint64_t length = zone_size - wp;
    if (!length) {
      continue;
    }
    bool intersects = false;
    dout(10) << "  marking zone 0x" << std::hex << zone
	     << " region after wp 0x" << offset << "~" << length
	     << std::dec << dendl;
    apply_for_bitset_range(
      offset, length, alloc_size, used_blocks,
      [&](uint64_t pos, mempool_dynamic_bitset &bs) {
	if (bs.test(pos)) {
	  derr << "fsck error: zone 0x" << std::hex << zone
	       << " has used space at 0x" << pos * alloc_size
	       << " beyond write pointer 0x" << wp
	       << std::dec << dendl;
	  intersects = true;
	} else {
	  bs.set(pos);
	}
      }
      );
    if (intersects) {
      ++errors;
    }
  }

  used_blocks.flip();

  // skip conventional zones
  uint64_t pos = (first_sequential_zone * zone_size) / bstore->min_alloc_size - 1;
  pos = used_blocks.find_next(pos);

  uint64_t zone_dead = 0;
  for (uint32_t zone = first_sequential_zone;
       zone < num_zones;
       ++zone, zone_dead = 0) {
    while (pos != mempool_dynamic_bitset::npos &&
	   (pos * bstore->min_alloc_size) / zone_size == zone) {
      dout(40) << " zone 0x" << std::hex << zone
	       << " dead 0x" << (pos * bstore->min_alloc_size) << "~" << bstore->min_alloc_size
	       << std::dec << dendl;
      zone_dead += bstore->min_alloc_size;
      pos = used_blocks.find_next(pos);
    }
    dout(20) << " zone 0x" << std::hex << zone << " dead is 0x" << zone_dead
	     << std::dec << dendl;
    // cross-check dead bytes against zone state
    if (a->get_dead_bytes(zone) != zone_dead) {
      derr << "fsck error: zone 0x" << std::hex << zone << " has 0x" << zone_dead
	   << " dead bytes but freelist says 0x" << a->get_dead_bytes(zone)
	   << dendl;
      ++errors;
      // TODO: repair
    }
  }
  used_blocks.flip();
}

void BlueStoreZoned::txc_finalize_kv(TransContext *txc)
{
  for (auto& i : txc->old_zone_offset_refs) {
    dout(20) << __func__ << " rm ref zone 0x" << std::hex << i.first.second
	     << " offset 0x" << i.second << std::dec
	     << " -> " << i.first.first->oid << dendl;
    string key;
    get_zone_offset_object_key(i.first.second, i.second, i.first.first->oid, &key);
    txc->t->rmkey(PREFIX_ZONED_CL_INFO, key);
  }
  for (auto& i : txc->new_zone_offset_refs) {
    // (zone, offset) -> oid
    dout(20) << __func__ << " add ref zone 0x" << std::hex << i.first.second
	     << " offset 0x" << i.second << std::dec
	     << " -> " << i.first.first->oid << dendl;
    string key;
    get_zone_offset_object_key(i.first.second, i.second, i.first.first->oid, &key);
    bufferlist v;
    txc->t->set(PREFIX_ZONED_CL_INFO, key, v);
  }
}

void BlueStoreZoned::cleaner_start()
{
  dout(10) << __func__ << dendl;
  zoned_cleaner_thread.create("bstore_zcleaner");
}

void BlueStoreZoned::cleaner_stop()
{
  dout(10) << __func__ << dendl;
  {
    std::unique_lock l{zoned_cleaner_lock};
    while (!zoned_cleaner_started) {
      zoned_cleaner_cond.wait(l);
    }
    zoned_cleaner_stop = true;
    zoned_cleaner_cond.notify_all();
  }
  zoned_cleaner_thread.join();
  {
    std::lock_guard l{zoned_cleaner_lock};
    zoned_cleaner_stop = false;
  }
  dout(10) << __func__ << " done" << dendl;
}

void BlueStoreZoned::cleaner_thread()
{
  dout(10) << __func__ << " start" << dendl;
  std::unique_lock l{zoned_cleaner_lock};
  ceph_assert(!zoned_cleaner_started);
  zoned_cleaner_started = true;
  zoned_cleaner_cond.notify_all();
  auto a = dynamic_cast<ZonedAllocator*>(bstore->alloc);
  ceph_assert(a);
  auto f = dynamic_cast<ZonedFreelistManager*>(bstore->fm);
  ceph_assert(f);
  while (true) {
    // Exit promptly on shutdown, even when there is always a zone to clean
    // (otherwise the loop below would keep cleaning and never observe the stop
    // request, hanging umount).
    if (zoned_cleaner_stop) {
      break;
    }

    // Only garbage-collect under space pressure.  Otherwise the cleaner would
    // relocate still-live data on a mostly-empty device (huge write
    // amplification that steals device bandwidth from client writes) instead
    // of using the abundant free space as over-provisioning.
    int64_t zone_to_clean = -1;
    if (a->low_on_space()) {
      // thresholds to trigger cleaning
      // FIXME: fixed values, they should be options
      float min_score = .05;                // score: bytes saved / bytes moved
      uint64_t min_saved = zone_size / 32;  // min bytes saved to consider cleaning
      zone_to_clean = a->pick_zone_to_clean(min_score, min_saved);
    }
    if (zone_to_clean < 0) {
      if (zoned_cleaner_stop) {
	break;
      }
      auto period = ceph::make_timespan(bstore->cct->_conf->bluestore_cleaner_sleep_interval);
      dout(20) << __func__ << " sleep for " << period << dendl;
      zoned_cleaner_cond.wait_for(l, period);
      dout(20) << __func__ << " wake" << dendl;
    } else {
      l.unlock();
      a->set_cleaning_zone(zone_to_clean);
      clean_zone(zone_to_clean, a, f);
      a->clear_cleaning_zone(zone_to_clean);
      l.lock();
    }
  }
  dout(10) << __func__ << " finish" << dendl;
  zoned_cleaner_started = false;
}

void BlueStoreZoned::clean_zone(
  uint64_t zone,
  ZonedAllocator *a,
  ZonedFreelistManager *f
  )
{
  dout(10) << __func__ << " cleaning zone 0x" << std::hex << zone << std::dec << dendl;

  KeyValueDB::Iterator it = bstore->db->get_iterator(PREFIX_ZONED_CL_INFO);
  std::string zone_start;
  get_zone_offset_object_key(zone, 0, ghobject_t(), &zone_start);
  for (it->lower_bound(zone_start); it->valid(); it->next()) {
    uint32_t z;
    uint64_t offset;
    ghobject_t oid;
    string k = it->key();
    int r = get_key_zone_offset_object(k, &z, &offset, &oid);
    if (r < 0) {
      derr << __func__ << " failed to decode zone ref " << pretty_binary_string(k)
	   << dendl;
      continue;
    }
    if (zone != z) {
      dout(10) << __func__ << " reached end of zone refs" << dendl;
      break;
    }
    dout(10) << __func__ << " zone 0x" << std::hex << zone << " offset 0x" << offset
	     << std::dec << " " << oid << dendl;
    clean_some(oid, zone);
  }

  if (a->get_live_bytes(zone) > 0) {
    derr << "zone 0x" << std::hex << zone << " still has 0x" << a->get_live_bytes(zone)
	 << " live bytes" << std::dec << dendl;
    // should we do something else here to avoid a live-lock in the event of a problem?
    return;
  }

  // make sure transactions flush/drain/commit (and data is all rewritten
  // safely elsewhere) before we blow away the cleaned zone
  bstore->_osr_drain_all();

  // reset the device zone
  dout(10) << __func__ << " resetting zone 0x" << std::hex << zone << std::dec << dendl;
  bstore->bdev->reset_zone(zone);

  // record that we can now write there
  f->mark_zone_to_clean_free(zone, bstore->db);
  bstore->bdev->flush();

  // then allow ourselves to start allocating there
  dout(10) << __func__ << " done cleaning zone 0x" << std::hex << zone << std::dec
	   << dendl;
  a->reset_zone(zone);
}

void BlueStoreZoned::clean_some(ghobject_t oid, uint32_t zone)
{
  dout(10) << __func__ << " " << oid << " from zone 0x" << std::hex << zone << std::dec
	   << dendl;

  CollectionRef cref = bstore->_get_collection_by_oid(oid);
  if (!cref) {
    dout(10) << __func__ << " can't find collection for " << oid << dendl;
    return;
  }
  Collection *c = cref.get();

  // serialize io dispatch vs other transactions
  std::lock_guard l(atomic_alloc_and_submit_lock);
  std::unique_lock l2(c->lock);

  auto o = c->get_onode(oid, false);
  if (!o) {
    dout(10) << __func__ << " can't find " << oid << dendl;
    return;
  }

  o->extent_map.fault_range(bstore->db, 0, BlueStore::OBJECT_MAX_SIZE);
  _dump_onode<30>(bstore->cct, *o);

  // NOTE: This is a naive rewrite strategy.  If any blobs are
  // shared, they will be duplicated for each object that references
  // them.  That means any cloned/snapshotted objects will explode
  // their utilization.  This won't matter for RGW workloads, but
  // for RBD and CephFS it is completely unacceptable, and it's
  // entirely reasonable to have "archival" data workloads on SMR
  // for CephFS and (possibly/probably) RBD.
  //
  // At some point we need to replace this with something more
  // sophisticated that ensures that a shared blob gets moved once
  // and all referencing objects get updated to point to the new
  // location.

  map<uint32_t, uint32_t> to_move;
  for (auto& e : o->extent_map.extent_map) {
    bool touches_zone = false;
    for (auto& be : e.blob->get_blob().get_extents()) {
      if (be.is_valid()) {
	uint32_t z = be.offset / zone_size;
	if (z == zone) {
	  touches_zone = true;
	  break;
	}
      }
    }
    if (touches_zone) {
      to_move[e.logical_offset] = e.length;
    }
  }
  if (to_move.empty()) {
    dout(10) << __func__ << " no references to zone 0x" << std::hex << zone
	     << std::dec << " from " << oid << dendl;
    return;
  }

  dout(10) << __func__ << " rewriting object extents 0x" << std::hex << to_move
	   << std::dec << dendl;
  OpSequencer *osr = c->osr.get();
  TransContext *txc = bstore->_txc_create(c, osr, nullptr);

  spg_t pgid;
  if (c->cid.is_pg(&pgid)) {
    txc->osd_pool_id = pgid.pool();
  }

  for (auto& [offset, length] : to_move) {
    bufferlist bl;
    int r = bstore->_do_read(c, o, offset, length, bl, 0);
    ceph_assert(r == (int)length);

    r = bstore->_do_write(txc, cref, o, offset, length, bl, 0);
    ceph_assert(r >= 0);
  }
  txc->write_onode(o);

  bstore->_txc_write_nodes(txc, txc->t);
  bstore->_txc_finalize_kv(txc, txc->t);
  bstore->_txc_state_proc(txc);
}

void BlueStoreZoned::do_write_small(
  CollectionRef& c,
  OnodeRef& o,
  uint64_t offset, uint64_t length,
  uint64_t alloc_len,
  ceph::buffer::list& bl,
  WriteContext *wctx)
{
  uint64_t b_off = p2phase<uint64_t>(offset, alloc_len);
  uint64_t b_off0 = b_off;
  o->extent_map.punch_hole(c, offset, length, &wctx->old_extents);

  // Zero detection -- small block
  if (!bstore->cct->_conf->bluestore_zero_block_detection || !bl.is_zero()) {
    BlobRef b = c->new_blob();
    bstore->_pad_zeros(&bl, &b_off0, bstore->min_alloc_size);
    wctx->write(offset, b, alloc_len, b_off0, bl, b_off, length, false, true);
  } else { // if (bl.is_zero())
    dout(20) << __func__ << " skip small zero block " << std::hex
      << " (0x" << b_off0 << "~" << bl.length() << ")"
      << " (0x" << b_off << "~" << length << ")"
      << std::dec << dendl;
    bstore->logger->inc(l_bluestore_write_small_skipped);
    bstore->logger->inc(l_bluestore_write_small_skipped_bytes, length);
  }

  return;
}

void BlueStoreZoned::wctx_note_writes(
  TransContext *txc,
  OnodeRef& o,
  WriteContext *wctx)
{
  for (auto& w : wctx->writes) {
    for (auto& e : w.b->get_blob().get_extents()) {
      if (!e.is_valid()) {
	continue;
      }
      uint32_t zone = e.offset / zone_size;
      if (!o->onode.zone_offset_refs.count(zone)) {
	uint64_t zoff = e.offset % zone_size;
	dout(20) << __func__ << " add ref zone 0x" << std::hex << zone
		 << " offset 0x" << zoff << std::dec << dendl;
	txc->note_write_zone_offset(o, zone, zoff);
      }
    }
  }
}

void BlueStoreZoned::wctx_drop_refs(
  TransContext *txc,
  OnodeRef& o,
  std::set<uint32_t>& zones_with_releases)
{
  // we need to fault the entire extent range in here to determine if we've dropped
  // all refs to a zone.
  o->extent_map.fault_range(bstore->db, 0, BlueStore::OBJECT_MAX_SIZE);
  for (auto& b : o->extent_map.extent_map) {
    for (auto& e : b.blob->get_blob().get_extents()) {
      if (e.is_valid()) {
	zones_with_releases.erase(e.offset / zone_size);
      }
    }
  }
  for (auto zone : zones_with_releases) {
    auto p = o->onode.zone_offset_refs.find(zone);
    if (p != o->onode.zone_offset_refs.end()) {
      dout(20) << __func__ << " rm ref zone 0x" << std::hex << zone
	       << " offset 0x" << p->second << std::dec << dendl;
      txc->note_release_zone_offset(o, zone, p->second);
    }
  }
}

void BlueStoreZoned::clone_range_refs(
  TransContext *txc,
  OnodeRef& newo,
  uint64_t dstoff, uint64_t length)
{
  // duplicate the refs for the shared region.
  Extent dummy(dstoff);
  for (auto e = newo->extent_map.extent_map.lower_bound(dummy);
       e != newo->extent_map.extent_map.end();
       ++e) {
    if (e->logical_offset >= dstoff + length) {
      break;
    }
    for (auto& ex : e->blob->get_blob().get_extents()) {
      // note that we may introduce a new extent reference that is
      // earlier than the first zone ref.  we allow this since it is
      // a lot of work to avoid and has marginal impact on cleaning
      // performance.
      if (!ex.is_valid()) {
	continue;
      }
      uint32_t zone = ex.offset / zone_size;
      if (!newo->onode.zone_offset_refs.count(zone)) {
	uint64_t zoff = ex.offset % zone_size;
	dout(20) << __func__ << " add ref zone 0x" << std::hex << zone
		 << " offset 0x" << zoff << std::dec
		 << " -> " << newo->oid << dendl;
	txc->note_write_zone_offset(newo, zone, zoff);
      }
    }
  }
}

void BlueStoreZoned::rename_refs(
  TransContext *txc,
  OnodeRef& oldo,
  OnodeRef& newo)
{
  // adjust zone refs
  for (auto& [zone, offset] : newo->onode.zone_offset_refs) {
    dout(20) << __func__ << " rm ref zone 0x" << std::hex << zone
	     << " offset 0x" << offset << std::dec
	     << " -> " << oldo->oid << dendl;
    string key;
    get_zone_offset_object_key(zone, offset, oldo->oid, &key);
    txc->t->rmkey(PREFIX_ZONED_CL_INFO, key);

    dout(20) << __func__ << " add ref zone 0x" << std::hex << zone
	     << " offset 0x" << offset << std::dec
	     << " -> " << newo->oid << dendl;
    get_zone_offset_object_key(zone, offset, newo->oid, &key);
    bufferlist v;
    txc->t->set(PREFIX_ZONED_CL_INFO, key, v);
  }
}
