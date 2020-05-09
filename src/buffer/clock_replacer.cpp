//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// clock_replacer.cpp
//
// Identification: src/buffer/clock_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/clock_replacer.h"

namespace bustub {

ClockReplacer::ClockReplacer(size_t num_pages) {
  this->capacity = num_pages;
  this->cur_size = 0;
  this->clock_hand = 0;
  this->buckets.resize(num_pages, -1);
  this->ref_bits.resize(num_pages, -1);
}

ClockReplacer::~ClockReplacer() = default;

bool ClockReplacer::Victim(frame_id_t *frame_id) {
  if (cur_size == 0) { return false; }

  int sweep_cnt = 0;
  while (sweep_cnt < cur_size) {
    clock_hand %= capacity;

    // empty slot or been recently pinned
    if (buckets[clock_hand] == -1 || recent_pinned.count(buckets[clock_hand])) {
      clock_hand++;
      continue;
    }

    // otherwise verify this clock hand position
    if (ref_bits[clock_hand] == 1) {  // this slot still got ref_bit == 1
      ref_bits[clock_hand] = 0;
      clock_hand++;
      sweep_cnt++;
    } else {  // find the victim
      int ans = buckets[clock_hand];
      *frame_id = ans;
      buckets[clock_hand] = -1;
      clock_hand++;
      auto it = map.find(ans);
      map.erase(it);
      cur_size--;
      return true;
    }
  }

  // no victim after whole sweep, then find the smallest frame_id as victim
  int smallest_frame_id = INT_MAX;
  for (int i = 0; i < capacity; i++) {
    if (buckets[i] != -1 && buckets[i] < smallest_frame_id) {
      smallest_frame_id = buckets[i];
    }
  }
  *frame_id = smallest_frame_id;
  auto it = map.find(smallest_frame_id);
  map.erase(it);
  cur_size--;
  return true;
}

void ClockReplacer::Pin(frame_id_t frame_id) {
  if (!map.count(frame_id)) { return; } // this frame_id has not been added into clockReplacer

  recent_pinned.insert(frame_id);
  cur_size--;
}

void ClockReplacer::Unpin(frame_id_t frame_id) {
  // case 1: if not in the clockReplacer, add <frame_id, 0>
  // case 2: if in the clockReplacer
  //    case 2.1: if in the recent_pinned set, set ref_bit = 1
  //    case 2.2: else, do nothing

  if (frame_id > capacity) { return; }

  if (!map.count(frame_id)) {
    int slot = find_next_available(clock_hand);
    buckets[slot] = frame_id;
    ref_bits[slot] = 0;
    map[frame_id] = slot;
    cur_size++;
  } else if (recent_pinned.count(frame_id)) {
    auto it = recent_pinned.find(frame_id);
    recent_pinned.erase(it);
    ref_bits[map[frame_id]] = 1;
    cur_size++;
  }
}

size_t ClockReplacer::Size() { return cur_size; }

auto ClockReplacer::find_next_available(int start) -> int {
  for (int i = 0; i < capacity; i++) {
    int cur_slot = (i + start) % capacity;
    if (buckets[cur_slot] == -1) {
      return cur_slot;
    } else if (recent_pinned.count(buckets[cur_slot])) {
      return cur_slot;
    }
  }
  return -1;
}

}  // namespace bustub
