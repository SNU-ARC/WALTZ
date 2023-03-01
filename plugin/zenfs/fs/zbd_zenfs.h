// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
// Copyright (c) 2019-present, Western Digital Corporation
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#if !defined(ROCKSDB_LITE) && defined(OS_LINUX)

#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include <thread>
#include <queue>

#include "metrics.h"
#include "rocksdb/env.h"
#include "rocksdb/io_status.h"

#include "spdk/nvme.h"
#include "spdk/nvme_zns.h"

#define NUM_WORKER_THREAD (32)

// [JS] if this is defined, callback parameter will keep the command-related variables
#define CB_ARG_DEBUG

#if defined(CB_ARG_DEBUG)
struct cb_type {
  struct spdk_nvme_ns    *ns;
  struct spdk_nvme_qpair *qpair;
  char func_name[16];
  uint64_t slba;
  uint64_t nlb;
  uint32_t idx;
  uint64_t alba;
  volatile bool fail;
  volatile bool done;
};
#else
struct cb_type {
  uint64_t alba;
  volatile bool fail;
  volatile bool done;
};
#endif

extern int get_qpair_idx();
extern void append_completion(void *cb_arg, const struct spdk_nvme_cpl *cpl);
extern void sync_completion(void *cb_arg, const struct spdk_nvme_cpl *cpl);
extern void async_completion(void *cb_arg, const struct spdk_nvme_cpl *cpl);

struct zns_info {
  struct spdk_nvme_ctrlr *ctrlr;
  struct spdk_nvme_ns    *ns;
  struct spdk_nvme_qpair **qpair;
  const struct spdk_nvme_zns_ns_data *zns;
  uint32_t maxqd;
  bool append_support;
  bool valid;

  zns_info() :
    ctrlr(nullptr),
    ns(nullptr),
    qpair(nullptr),
    zns(nullptr),
    maxqd(0),
    append_support(false),
    valid(false) {
  }
};

extern zns_info *g_stInfo;

namespace ROCKSDB_NAMESPACE {
class ZonedBlockDevice;
class ZoneSnapshot;

class Zone {
  ZonedBlockDevice *zbd_;
  std::atomic_bool busy_;

 public:
  explicit Zone(ZonedBlockDevice *zbd, struct spdk_nvme_zns_zone_desc *z);

  uint64_t start_;
  uint64_t capacity_; /* remaining capacity */
  uint64_t max_capacity_;
  uint64_t wp_;
  Env::WriteLifeTimeHint lifetime_;
  std::atomic<long> used_capacity_;

  IOStatus Reset();
  IOStatus ResetAsync();
  IOStatus Finish();
  IOStatus Close();

  IOStatus Write(char *data, uint32_t size);
  IOStatus Append(char *data, uint32_t size, uint64_t *alba);
  bool IsUsed();
  bool IsFull();
  bool IsEmpty();
  uint64_t GetZoneNr();
  uint64_t GetCapacityLeft();
  bool IsBusy() { return this->busy_.load(std::memory_order_relaxed); }
  bool Acquire() {
    bool expected = false;
    return this->busy_.compare_exchange_strong(expected, true,
                                               std::memory_order_acq_rel);
  }
  bool Release() {
    bool expected = true;
    return this->busy_.compare_exchange_strong(expected, false,
                                               std::memory_order_acq_rel);
  }

  void EncodeJson(std::ostream &json_stream);

  IOStatus CloseWR(); /* Done writing */

  inline IOStatus CheckRelease();
};

class ZonedBlockDevice {
 private:
  std::string filename_;
  uint32_t blk_sft_;
  uint64_t blk_msk_;
  uint32_t block_sz_;
  uint64_t zone_sz_;
  uint32_t nr_zones_;
  std::vector<Zone *> io_zones;
  std::mutex io_zones_mtx;
  std::vector<Zone *> meta_zones;

  time_t start_time_;
  std::shared_ptr<Logger> logger_;
  uint32_t finish_threshold_ = 0;

  std::atomic<long> active_io_zones_;
  std::atomic<long> open_io_zones_;
  /* Protects zone_resuorces_  condition variable, used
     for notifying changes in open_io_zones_ */
  std::mutex zone_resources_mtx_;
  std::condition_variable zone_resources_;
  std::mutex zone_deferred_status_mutex_;
  IOStatus zone_deferred_status_;

  unsigned int max_nr_active_io_zones_;
  unsigned int max_nr_open_io_zones_;

  std::shared_ptr<ZenFSMetrics> metrics_;

  uint32_t max_data_xfer_size_;

  std::thread *allocation_thread_;
  const int wal_zone_reserve_count_ = 2;
  const int wal_allocation_threshold_ = 5; // 5%
  std::queue<Zone *> wal_zone_queue_;
  std::queue<Zone *> wal_finish_zone_queue_;
  std::atomic<int> wal_queue_cnt_;
  std::atomic<int> wal_finish_queue_cnt_;
  char *wal_update_extent_buffer_;

  void EncodeJsonZone(std::ostream &json_stream,
                      const std::vector<Zone *> zones);

 public:
  explicit ZonedBlockDevice(std::string bdevname,
                            std::shared_ptr<Logger> logger,
                            std::shared_ptr<ZenFSMetrics> metrics =
                                std::make_shared<NoZenFSMetrics>());
  virtual ~ZonedBlockDevice();

  IOStatus Open(bool readonly, bool exclusive);
  IOStatus CheckScheduler();
  Status Format();

  Zone *GetIOZone(uint64_t offset);

  IOStatus AllocateZone(Env::WriteLifeTimeHint file_lifetime, Zone **out_zone);
  IOStatus AllocateMetaZone(Zone **out_meta_zone);

  uint64_t GetFreeSpace();
  uint64_t GetUsedSpace();
  uint64_t GetReclaimableSpace();

  std::string GetFilename();
  uint32_t GetBlockShift();
  uint64_t GetBlockMask();
  uint32_t GetBlockSize();

  Status ResetUnusedIOZones();
  void LogZoneStats();
  void LogZoneUsage();

  uint64_t GetZoneSize() { return zone_sz_; }
  uint32_t GetNrZones() { return nr_zones_; }
  std::vector<Zone *> GetMetaZones() { return meta_zones; }

  void SetFinishTreshold(uint32_t threshold) { finish_threshold_ = threshold; }

  void NotifyIOZoneFull();
  void NotifyIOZoneClosed();

  void EncodeJson(std::ostream &json_stream);

  void SetZoneDeferredStatus(IOStatus status);

  std::shared_ptr<ZenFSMetrics> GetMetrics() { return metrics_; }

  void GetZoneSnapshot(std::vector<ZoneSnapshot> &snapshot);

  uint32_t GetMDTS(void) { return max_data_xfer_size_; }

  void BackgroundJobForWAL(void);
  Zone *RetrieveWalZone(void);
  bool CheckNewWalZoneNeeded(Zone *req_zone, uint64_t alba) {
    //req_zone->max_capacity_ * wal_allocation_threshold_ / 100
    if ((req_zone->start_ + req_zone->max_capacity_ - alba * 4096) < 1*1024*1024) { // 1MB fixed threshold
      return true;
    } else {
      return false;
    }
  }
  void FinishWalZone(Zone *req_zone) {
    wal_finish_zone_queue_.push(req_zone);
    wal_finish_queue_cnt_.fetch_add(1);
  }
  char *GetWalExtentBuffer(void) {
    return wal_update_extent_buffer_;
  }

 private:
  std::string ErrorToString(int err);
  IOStatus GetZoneDeferredStatus();
};

}  // namespace ROCKSDB_NAMESPACE

#endif  // !defined(ROCKSDB_LITE) && defined(OS_LINUX)
