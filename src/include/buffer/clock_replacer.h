//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// clock_replacer.h
//
// Identification: src/include/buffer/clock_replacer.h
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <algorithm>
#include <list>
#include <mutex>  // NOLINT
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "buffer/replacer.h"
#include "common/config.h"

namespace bustub {

/**
 * ClockReplacer implements the clock replacement policy, which approximates the Least Recently Used policy.
 */
class ClockReplacer : public Replacer {
 public:
  /**
   * Create a new ClockReplacer.
   * @param num_pages the maximum number of pages the ClockReplacer will be required to store
   */
  explicit ClockReplacer(size_t num_pages);

  /**
   * Destroys the ClockReplacer.
   */
  ~ClockReplacer() override;

  bool Victim(frame_id_t *frame_id) override;

  void Pin(frame_id_t frame_id) override;

  void Unpin(frame_id_t frame_id) override;

  size_t Size() override;

 private:
  // TODO(student): implement me!
  int capacity;
  int cur_size;
  int clock_hand;
  std::vector<frame_id_t> buckets;
  std::vector<int> ref_bits;  // it represent whether this frame changed from pinned to unpinned recently
  std::unordered_map<frame_id_t, int> map; // key=frame_id, value=butck id in clockReplacer
  std::unordered_set<frame_id_t> recent_pinned; // a set of recent pinned frame_id

  auto find_next_available(int start) -> int;
};

}  // namespace bustub
