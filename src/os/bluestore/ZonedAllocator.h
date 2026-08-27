// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

//
// A simple allocator that just hands out space from the next empty zone.  This
// is temporary, just to get the simplest append-only write workload to work.
//
// Copyright (C) 2020 Abutalib Aghayev
//

#ifndef CEPH_OS_BLUESTORE_ZONEDALLOCATOR_H
#define CEPH_OS_BLUESTORE_ZONEDALLOCATOR_H

#include <mutex>
#include <set>
#include <vector>

#include "AllocatorBase.h"
#include "common/ceph_mutex.h"
#include "include/btree_map.h"
#include "include/interval_set.h"
#include "include/mempool.h"
#include "bluestore_types.h"
#include "zoned_types.h"

class ZonedAllocator : public AllocatorBase {
  CephContext* cct;

  // Currently only one thread at a time calls into ZonedAllocator due to
  // BlueStoreZoned::atomic_alloc_and_submit_lock, but we do locking anyway
  // because eventually ZONE_APPEND support will land and
  // atomic_alloc_and_submit_lock will be removed.
  ceph::mutex lock = ceph::make_mutex("ZonedAllocator::lock");

  uint64_t size;
  uint64_t sequential_size;
  std::atomic<int64_t> num_sequential_free;  ///< total bytes in freelist
  uint64_t block_size;
  uint64_t zone_size;
  uint64_t zone_capacity;  ///< usable bytes per zone (<= zone_size on ZNS)
  uint64_t first_seq_zone_num;
  uint64_t starting_zone_num;
  uint64_t num_zones;
  std::atomic<uint32_t> cleaning_zone = -1;
  std::vector<zone_state_t> zone_states;
  // Zones the allocator has just filled and that need a device zone-finish.
  // The write to the last allocated bytes may still be on its way, and a
  // finished zone rejects it, so these are not ready yet.  The transaction
  // that filled them takes them with take_filled_zones().
  std::set<uint64_t> zones_filled;
  // Zones that are filled and written and should be transitioned to the
  // device FULL state (so they stop counting against max_active_zones).
  // Drained by BlueStore, which issues the actual device zone-finish.
  std::set<uint64_t> zones_to_finish;
  // Zones the cleaner failed to fully evacuate (residual live bytes it could
  // not relocate).  Excluded from future cleaning so the cleaner does not
  // live-lock re-selecting the same un-cleanable zone.  Cleared when the zone
  // is reset, when more space is released in it, or when the cleaner looks
  // for a zone and this one has no live bytes.
  std::set<uint32_t> non_cleanable_zones;

  inline uint64_t get_offset(uint64_t zone_num) const {
    return zone_num * zone_size + get_write_pointer(zone_num);
  }

public:
  inline uint64_t get_write_pointer(uint64_t zone_num) const {
    return zone_states[zone_num].get_write_pointer();
  }
private:
  inline uint64_t get_remaining_space(uint64_t zone_num) const {
    // Only the first |zone_capacity| bytes of a zone are usable; the tail
    // [zone_capacity, zone_size) is an unwritable gap on ZNS devices.
    return zone_capacity - get_write_pointer(zone_num);
  }

  inline void increment_write_pointer(uint64_t zone_num, uint64_t want_size) {
    zone_states[zone_num].increment_write_pointer(want_size);
  }

  inline void increment_num_dead_bytes(uint64_t zone_num, uint64_t length) {
    zone_states[zone_num].increment_num_dead_bytes(length);
  }

public:
  ZonedAllocator(CephContext* cct, int64_t size, int64_t block_size,
		 int64_t _zone_size,
		 int64_t _zone_capacity,
		 int64_t _first_sequential_zone,
		 std::string_view name);
  ~ZonedAllocator() override;

  const char *get_type() const override {
    return "zoned";
  }

  uint64_t get_dead_bytes(uint32_t zone) {
    return zone_states[zone].num_dead_bytes;
  }
  uint64_t get_live_bytes(uint32_t zone) {
    std::scoped_lock l(lock);
    return zone_states[zone].write_pointer - zone_states[zone].num_dead_bytes;
  }

  int64_t allocate(
    uint64_t want_size, uint64_t alloc_unit, uint64_t max_alloc_size,
    int64_t hint, PExtentVector *extents) override;

  void release(const interval_set<uint64_t>& release_set) override;

  uint64_t get_free() override;

  void dump() override;
  void foreach(
    std::function<void(uint64_t offset, uint64_t length)> notify) override;
  uint64_t get_free_extents(
    uint64_t range_begin,
    uint64_t range_end,
    size_t max_count,
    free_extent_vector_t* out) override;

  // Move out the zones that were filled since the last call.  Allocations
  // are serialized with the submission of their writes, so these are the
  // zones the calling transaction filled.
  void take_filled_zones(std::vector<uint64_t> *zones) {
    std::scoped_lock l(lock);
    zones->assign(zones_filled.begin(), zones_filled.end());
    zones_filled.clear();
  }
  // The writes of the transaction that filled these zones are done.
  void queue_zones_to_finish(const std::vector<uint64_t>& zones) {
    std::scoped_lock l(lock);
    zones_to_finish.insert(zones.begin(), zones.end());
  }

  bool has_zones_to_finish() {
    std::scoped_lock l(lock);
    return !zones_to_finish.empty();
  }
  // Move out the set of zones that have been filled and written, and need a
  // device zone-finish to transition them to FULL.  Clears the internal set.
  void get_zones_to_finish(std::vector<uint64_t> *zones) {
    std::scoped_lock l(lock);
    zones->assign(zones_to_finish.begin(), zones_to_finish.end());
    zones_to_finish.clear();
  }

  int64_t pick_zone_to_clean(float min_score, uint64_t min_saved);
  // Mark a zone the cleaner could not fully evacuate so it is not re-selected
  // (avoids a cleaner live-lock that would otherwise spin and flood the log).
  void mark_zone_non_cleanable(uint32_t zone) {
    std::scoped_lock l(lock);
    non_cleanable_zones.insert(zone);
  }
  // True once free space drops below the over-provisioning threshold; used to
  // gate the cleaner so garbage collection only runs under space pressure
  // (and not wastefully on a mostly-empty device).
  bool low_on_space(void);
  void set_cleaning_zone(uint32_t zone) {
    cleaning_zone = zone;
  }
  void clear_cleaning_zone(uint32_t zone) {
    cleaning_zone = -1;
  }
  void reset_zone(uint32_t zone);

  void init_from_zone_pointers(
    std::vector<zone_state_t> &&_zone_states);
  // Called once at mount, after init_from_zone_pointers().  The requests to
  // finish a zone live only in memory, so queue again the zones that are
  // filled and that the device does not report as full.  |device_wp| has the
  // device's write pointer of each zone, as a device offset.
  void init_zones_to_finish(const std::vector<uint64_t>& device_wp);
  void init_add_free(uint64_t offset, uint64_t length) override {}
  void init_rm_free(uint64_t offset, uint64_t length) override {}

  void shutdown() override;
};

#endif
