//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  *frame_id = -1;
  auto judge = [&](const frame_id_t &s, const frame_id_t &t) -> bool {
    if (frame_map_[s].access_history_.size() < k_ && frame_map_[t].access_history_.size() == k_) {
      return true;
    }
    if (frame_map_[s].access_history_.size() == k_ && frame_map_[t].access_history_.size() < k_) {
      return false;
    }
    return frame_map_[s].access_history_.front() < frame_map_[t].access_history_.front();
  };
  for (const auto &[key, value] : frame_map_) {
    if (value.evictable_) {
      if (*frame_id == -1 || judge(key, *frame_id)) {
        *frame_id = key;
      }
    }
  }
  if (*frame_id != -1) {
    frame_map_.erase(*frame_id);
    curr_size_--;
    return true;
  }
  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  if (frame_map_.find(frame_id) == frame_map_.end() && frame_map_.size() == replacer_size_) {
    return;
  }
  if (frame_map_[frame_id].access_history_.size() == k_) {
    frame_map_[frame_id].access_history_.pop();
  }
  frame_map_[frame_id].access_history_.emplace(current_timestamp_++);
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::scoped_lock<std::mutex> lock(latch_);
  if (frame_map_.find(frame_id) == frame_map_.end()) {
    return;
  }
  bool old_evictable = frame_map_[frame_id].evictable_;
  frame_map_[frame_id].evictable_ = set_evictable;
  if (!old_evictable && set_evictable) {
    ++curr_size_;
  } else if (old_evictable && !set_evictable) {
    --curr_size_;
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  if (frame_map_.find(frame_id) == frame_map_.end() || !frame_map_[frame_id].evictable_) {
    return;
  }
  frame_map_.erase(frame_id);
  --curr_size_;
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

}  // namespace bustub
