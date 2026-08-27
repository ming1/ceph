// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014 Red Hat
 * Copyright (C) 2020 Abutalib Aghayev
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "HMSMRDevice.h"
extern "C" {
#include <libzbd/zbd.h>
}
#include "common/debug.h"
#include "common/errno.h"

#define dout_context cct
#define dout_subsys ceph_subsys_bdev
#undef dout_prefix
#define dout_prefix *_dout << "smrbdev(" << this << " " << path << ") "

using namespace std;

HMSMRDevice::HMSMRDevice(CephContext* cct,
			 aio_callback_t cb,
			 void *cbpriv,
			 aio_callback_t d_cb,
			 void *d_cbpriv)
  : KernelDevice(cct, cb, cbpriv, d_cb, d_cbpriv)
{
}

bool HMSMRDevice::support(const std::string& path)
{
  return zbd_device_is_zoned(path.c_str()) == 1;
}

int HMSMRDevice::_post_open()
{
  dout(10) << __func__ << dendl;

  struct zbd_info info;
  zbd_fd = zbd_open(path.c_str(), O_RDWR | O_DIRECT | O_LARGEFILE, &info);
  int r;
  if (zbd_fd < 0) {
    r = errno;
    derr << __func__ << " zbd_open failed on " << path << ": "
	 << cpp_strerror(r) << dendl;
    return -r;
  }

  // Number of conventional (randomly-writable) zones.  These always form a
  // prefix of the device's LBA space and define the conventional region.  A
  // pure ZNS device has none, so we must not assume one exists.
  unsigned int nr_conv_zones = 0;
  // Reusable single-zone report buffer.
  unsigned int nr = 1;
  std::vector<zbd_zone> z(1);
  if (zbd_report_nr_zones(zbd_fd, 0, 0, ZBD_RO_NOT_WP, &nr_conv_zones) != 0) {
    r = -errno;
    derr << __func__ << " zbd_report_nr_zones failed on " << path << ": "
	 << cpp_strerror(r) << dendl;
    goto fail;
  }

  // Read the first zone (of any type) to determine the (uniform) zone size.
  // HM-SMR drives start with conventional zones, pure ZNS drives start with a
  // sequential zone, but in both cases the zone size is the same everywhere.
  nr = 1;
  if (zbd_report_zones(zbd_fd, 0, 0, ZBD_RO_ALL, z.data(), &nr) != 0) {
    r = -errno;
    derr << __func__ << " zbd_report_zones failed on " << path << ": "
	 << cpp_strerror(r) << dendl;
    goto fail;
  }
  if (nr < 1) {
    // the call succeeded, so errno is not set: do not return it
    r = -EINVAL;
    derr << __func__ << " " << path << " reports no zones" << dendl;
    goto fail;
  }
  zone_size = zbd_zone_len(&z[0]);
  conventional_region_size = nr_conv_zones * zone_size;

  // Determine the usable capacity of a sequential zone.  On HM-SMR drives the
  // capacity equals the zone size; ZNS SSDs may report a smaller capacity,
  // leaving an unwritable gap at the end of every sequential zone.  We read it
  // from the first zone at/after the conventional region (capacity is uniform
  // across sequential zones on every device we support).
  if (conventional_region_size / zone_size >= info.nr_zones) {
    // No sequential zone, so nothing uses the capacity.
    zone_capacity = zone_size;
  } else {
    // The capacity is stored at mkfs and decides what can be allocated: do
    // not guess it when the device does not tell.
    nr = 1;
    if (zbd_report_zones(zbd_fd, conventional_region_size, zone_size,
			 ZBD_RO_ALL, z.data(), &nr) != 0) {
      r = -errno;
      derr << __func__ << " zbd_report_zones failed on " << path << ": "
	   << cpp_strerror(r) << dendl;
      goto fail;
    }
    if (nr < 1) {
      r = -EINVAL;
      derr << __func__ << " " << path << " reports no zone at offset "
	   << conventional_region_size << dendl;
      goto fail;
    }
    zone_capacity = zbd_zone_capacity(&z[0]);
    if (zone_capacity == 0 || zone_capacity > zone_size) {
      r = -EINVAL;
      derr << __func__ << " " << path << " reports a zone capacity of "
	   << zone_capacity << " for a zone size of " << zone_size << dendl;
      goto fail;
    }
  }

  dout(10) << __func__ << " setting zone size to " << zone_size
	   << ", zone capacity to " << zone_capacity
	   << " and conventional region size to " << conventional_region_size
           << dendl;

  return 0;

fail:
  zbd_close(zbd_fd);
  zbd_fd = -1;
  return r;
}


void HMSMRDevice::_pre_close()
{
  if (zbd_fd >= 0) {
    zbd_close(zbd_fd);
    zbd_fd = -1;
  }
}

void HMSMRDevice::reset_all_zones()
{
  dout(10) << __func__ << dendl;
  zbd_reset_zones(zbd_fd, conventional_region_size, 0);
}

void HMSMRDevice::reset_zone(uint64_t zone)
{
  dout(10) << __func__ << " zone 0x" << std::hex << zone << std::dec << dendl;
  if (zbd_reset_zones(zbd_fd, zone * zone_size, zone_size) != 0) {
    derr << __func__ << " resetting zone failed for zone 0x" << std::hex
	 << zone << std::dec << dendl;
    ceph_abort("zbd_reset_zones failed");
  }
}

std::vector<uint64_t> HMSMRDevice::get_zones()
{
  std::vector<zbd_zone> zones;
  unsigned int num_zones = size / zone_size;
  zones.resize(num_zones);

  int r = zbd_report_zones(zbd_fd, 0, 0, ZBD_RO_ALL, zones.data(), &num_zones);
  if (r != 0) {
    derr << __func__ << " zbd_report_zones failed on " << path << ": "
	 << cpp_strerror(errno) << dendl;
    ceph_abort("zbd_report_zones failed");
  }

  std::vector<uint64_t> wp(num_zones);
  for (unsigned i = 0; i < num_zones; ++i) {
    // The write pointer of a full zone is not defined.  Some devices report
    // the end of the zone there, others an invalid value (zloop: all ones).
    // Callers expect the end of the zone.
    if (zbd_zone_full(&zones[i])) {
      wp[i] = zbd_zone_start(&zones[i]) + zbd_zone_len(&zones[i]);
    } else {
      wp[i] = zbd_zone_wp(&zones[i]);
    }
  }
  return wp;
}
