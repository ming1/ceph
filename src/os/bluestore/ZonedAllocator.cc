// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

//
// A simple allocator that just hands out space from the next empty zone.  This
// is temporary, just to get the simplest append-only write workload to work.
//
// Copyright (C) 2020 Abutalib Aghayev
//

#include "ZonedAllocator.h"
#include "bluestore_types.h"
#include "zoned_types.h"
#include "common/debug.h"

#define dout_context cct
#define dout_subsys ceph_subsys_bluestore
#undef dout_prefix
#define dout_prefix *_dout << "ZonedAllocator(" << this << ") " << __func__ << " "

ZonedAllocator::ZonedAllocator(CephContext* cct,
			       int64_t size,
			       int64_t blk_size,
			       int64_t _zone_size,
			       int64_t _first_sequential_zone,
			       std::string_view name)
    : AllocatorBase(name, size, blk_size),
      cct(cct),
      size(size),
      conventional_size(_first_sequential_zone * _zone_size),
      sequential_size(size - conventional_size),
      num_sequential_free(0),
      block_size(blk_size),
      zone_size(_zone_size),
      first_seq_zone_num(_first_sequential_zone),
      starting_zone_num(first_seq_zone_num),
      num_zones(size / zone_size)
{
  ldout(cct, 10) << " size 0x" << std::hex << size
		 << ", zone size 0x" << zone_size << std::dec
		 << ", number of zones 0x" << num_zones
		 << ", first sequential zone 0x" << starting_zone_num
		 << ", sequential size 0x" << sequential_size
		 << std::dec
		 << dendl;
  ceph_assert(size % zone_size == 0);

  zone_states.resize(num_zones);
}

ZonedAllocator::~ZonedAllocator()
{
}

int64_t ZonedAllocator::allocate(
  uint64_t want_size,
  uint64_t alloc_unit,
  uint64_t max_alloc_size,
  int64_t hint,
  PExtentVector *extents)
{
  std::lock_guard l(lock);

  ceph_assert(want_size % 4096 == 0);

  ceph_assert(alloc_unit > 0);

  ldout(cct, 10) << " trying to allocate 0x"
		 << std::hex << want_size << std::dec << dendl;

  // Fill zones completely, spilling the remainder of a request into subsequent
  // zones (as additional extents) instead of requiring the whole request to
  // fit one zone.  Otherwise, whenever the request size does not divide a
  // zone's remaining space, the trailing "want_size - remaining" bytes are
  // stranded: they stay counted as free but are never allocated, and the zone
  // is left partially filled.  Filling the tail also lets starting_zone_num
  // advance past full zones (avoiding an O(n^2) rescan of the stranded zones
  // on every allocation).
  const size_t extents_begin = extents->size();
  const uint64_t starting_zone_saved = starting_zone_num;
  int64_t allocated = 0;
  uint64_t to_alloc = want_size;
  uint64_t left = num_zones - first_seq_zone_num;
  uint64_t zone_num = starting_zone_num;
  for ( ; to_alloc > 0 && left > 0; ++zone_num, --left) {
    if (zone_num == num_zones) {
      zone_num = first_seq_zone_num;
    }
    if (zone_num == cleaning_zone) {
      ldout(cct, 10) << " skipping zone 0x" << std::hex << zone_num
		     << " because we are cleaning it" << std::dec << dendl;
      continue;
    }
    // Only alloc_unit-aligned space is allocatable.
    uint64_t avail = p2align(get_remaining_space(zone_num), alloc_unit);
    if (avail == 0) {
      continue;
    }
    uint64_t this_size = std::min(to_alloc, avail);
    uint64_t offset = get_offset(zone_num);
    increment_write_pointer(zone_num, this_size);
    num_sequential_free -= this_size;
    extents->emplace_back(bluestore_pextent_t(offset, this_size));
    allocated += this_size;
    to_alloc -= this_size;

    ldout(cct, 10) << " allocated 0x" << std::hex << offset << "~" << this_size
		   << " from zone 0x" << zone_num
		   << " and zone offset 0x" << (offset % zone_size)
		   << std::dec << dendl;

    if (p2align(get_remaining_space(zone_num), alloc_unit) == 0) {
      // No more allocatable space in this zone: never allocate from it again.
      if (zone_num == starting_zone_num) {
	starting_zone_num = zone_num + 1;
      }
    }
    if (to_alloc == 0) {
      break;
    }
  }

  if (to_alloc > 0) {
    ldout(cct, 10) << " failed to allocate 0x" << std::hex << want_size
		   << ", only got 0x" << allocated << std::dec << dendl;
    // roll back this call's allocations so we do not leak space
    for (size_t i = extents_begin; i < extents->size(); ++i) {
      const auto& e = (*extents)[i];
      uint64_t z = e.offset / zone_size;
      zone_states[z].write_pointer -= e.length;
      num_sequential_free += e.length;
    }
    extents->resize(extents_begin);
    // the zones we passed have space again, so start from them next time
    starting_zone_num = starting_zone_saved;
    return -ENOSPC;
  }

  return allocated;
}

void ZonedAllocator::release(const interval_set<uint64_t>& release_set)
{
  std::lock_guard l(lock);
  for (auto p = cbegin(release_set); p != cend(release_set); ++p) {
    auto offset = p.get_start();
    auto length = p.get_len();
    uint64_t zone_num = offset / zone_size;
    ldout(cct, 10) << " 0x" << std::hex << offset << "~" << length
		   << " from zone 0x" << zone_num << std::dec << dendl;
    uint64_t num_dead = std::min(zone_size - offset % zone_size, length);
    for ( ; length; ++zone_num) {
      increment_num_dead_bytes(zone_num, num_dead);
      // More of the zone is dead now, maybe all of what the cleaner could
      // not move: let it try this zone again.  This cannot make the cleaner
      // spin, because a new attempt needs a new release in the zone.
      non_cleanable_zones.erase(zone_num);
      length -= num_dead;
      num_dead = std::min(zone_size, length);
    }
  }
}

uint64_t ZonedAllocator::get_free()
{
  return num_sequential_free;
}

void ZonedAllocator::dump()
{
  std::lock_guard l(lock);
  ldout(cct, 0) << __func__ << " zones:" << dendl;
  for (uint64_t zone = first_seq_zone_num; zone < num_zones; ++zone) {
    ldout(cct, 0) << std::hex
		  << "zone 0x" << zone
		  << " write pointer 0x" << get_write_pointer(zone)
		  << " dead bytes 0x" << zone_states[zone].get_num_dead_bytes()
		  << std::dec << dendl;
  }
}

void ZonedAllocator::foreach(
  std::function<void(uint64_t offset, uint64_t length)> notify)
{
  std::lock_guard l(lock);
  // Free space in a sequential zone is the tail behind the write pointer, up to
  // the end of the zone.  Reporting it keeps the generic consumers honest: the
  // fragmentation score is computed from this walk, and scoring an empty set
  // yields a garbage value and a permanent bogus "high free space
  // fragmentation" health warning.
  for (uint64_t zone = first_seq_zone_num; zone < num_zones; ++zone) {
    uint64_t wp = get_write_pointer(zone);
    if (wp >= zone_size) {
      continue;
    }
    notify(zone * zone_size + wp, zone_size - wp);
  }
}

uint64_t ZonedAllocator::get_free_extents(
  uint64_t range_begin,
  uint64_t range_end,
  size_t max_count,
  free_extent_vector_t* out)
{
  ceph_assert(range_begin <= range_end);
  if (range_begin == range_end) {
    return range_end;
  }
  std::lock_guard l(lock);
  // Batched counterpart of foreach(): walk the zones overlapping the window and
  // emit each zone's free tail, clipped to the window.
  uint64_t zone = std::max(range_begin / zone_size, first_seq_zone_num);
  size_t n = 0;
  max_count--;  // 0 wraps to SIZE_MAX, i.e. unbounded, as in the other allocators
  for (; zone < num_zones && zone * zone_size < range_end; ++zone) {
    uint64_t wp = get_write_pointer(zone);
    if (wp >= zone_size) {
      continue;
    }
    uint64_t lo = std::max(zone * zone_size + wp, range_begin);
    uint64_t hi = std::min(zone * zone_size + zone_size, range_end);
    if (lo >= hi) {
      continue;
    }
    if (n > max_count) {
      // Stopped on the count cap: resume at the start of this zone.
      return zone * zone_size;
    }
    out->emplace_back(lo, hi - lo);
    ++n;
  }
  return range_end;
}

void ZonedAllocator::init_from_zone_pointers(
  std::vector<zone_state_t> &&_zone_states)
{
  // this is called once, based on the device's zone pointers
  std::lock_guard l(lock);
  ldout(cct, 10) << dendl;
  zone_states = std::move(_zone_states);
  num_sequential_free = 0;
  for (size_t i = first_seq_zone_num; i < num_zones; ++i) {
    // The write pointer is relative to the start of the zone and never exceeds
    // the zone size, so the free space is zone_size - write_pointer.
    num_sequential_free += zone_size - zone_states[i].write_pointer;
  }
  ldout(cct, 10) << "free 0x" << std::hex << num_sequential_free
		 << " / 0x" << sequential_size << std::dec
		 << dendl;
}

int64_t ZonedAllocator::pick_zone_to_clean(float min_score, uint64_t min_saved)
{
  std::lock_guard l(lock);
  int32_t best = -1;
  float best_score = 0.0;
  for (size_t i = first_seq_zone_num; i < num_zones; ++i) {
    // value (score) = benefit / cost
    //    benefit = how much net free space we'll get (dead bytes)
    //    cost = how many bytes we'll have to rewrite (live bytes)
    // avoid divide by zero on a zone with no live bytes
    float score =
      (float)zone_states[i].num_dead_bytes /
      (float)(zone_states[i].get_num_live_bytes() + 1);
    if (score > 0) {
      ldout(cct, 20) << " zone 0x" << std::hex << i
		     << " dead 0x" << zone_states[i].num_dead_bytes
		     << " score " << score
		     << dendl;
    }
    if (zone_states[i].num_dead_bytes < min_saved) {
      continue;
    }
    if (non_cleanable_zones.count(i)) {
      if (zone_states[i].get_num_live_bytes() > 0) {
	// a previous cleaning attempt could not fully evacuate this zone
	continue;
      }
      // Nothing is live here any more.  The cleaner looks at the live bytes
      // and marks the zone in two steps, and the last release came in
      // between, so release() found no mark to remove.  There is nothing
      // left to move: the next attempt resets the zone.
      ldout(cct, 10) << " zone 0x" << std::hex << i << std::dec
		     << " has no live bytes, can be cleaned again" << dendl;
      non_cleanable_zones.erase(i);
    }
    if (best < 0 || score > best_score) {
      best = i;
      best_score = score;
    }
  }
  if (best < 0) {
    ldout(cct, 10) << " no zones found that are good cleaning candidates" << dendl;
    return -1;
  }
  ldout(cct, 10) << " zone 0x" << std::hex << best << " with score " << best_score
		 << ": 0x" << zone_states[best].num_dead_bytes
		 << " dead and 0x" << zone_states[best].get_num_live_bytes()
		 << " live bytes" << std::dec << dendl;
  if (best_score < min_score) {
    ldout(cct, 10) << " the score is below min_score " << min_score << dendl;
    return -1;
  }
  return best;
}

void ZonedAllocator::reset_zone(uint32_t zone)
{
  std::scoped_lock l(lock);
  num_sequential_free += zone_states[zone].write_pointer;
  zone_states[zone].reset();
  non_cleanable_zones.erase(zone);
}

bool ZonedAllocator::low_on_space(void)
{
  std::lock_guard l(lock);
  double free_ratio = static_cast<double>(num_sequential_free) / sequential_size;

  ldout(cct, 10) << " free 0x" << std::hex << num_sequential_free
		 << "/ 0x" << sequential_size << std::dec
		 << ", free ratio is " << free_ratio << dendl;
  ceph_assert(num_sequential_free <= (int64_t)sequential_size);

  return free_ratio <= cct->_conf->bluestore_zoned_cleaner_min_free_ratio;
}

void ZonedAllocator::shutdown()
{
  ldout(cct, 1) << dendl;
}
