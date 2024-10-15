//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "container/hash/extendible_hash_table.h"

#include <cassert>
#include <cstdlib>
#include <functional>
#include <list>
#include <utility>

#include "storage/page/page.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size)
    : global_depth_(0), bucket_size_(bucket_size), num_buckets_(1) {
  dir_.emplace_back(std::make_shared<Bucket>(bucket_size_));
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  int mask = (1 << global_depth_) - 1;
  return std::hash<K>()(key) & mask;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetGlobalDepthInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  return global_depth_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetLocalDepthInternal(dir_index);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  return dir_[dir_index]->GetDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetNumBucketsInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  return num_buckets_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  size_t index = IndexOf(key);
  return dir_[index]->Find(key, value);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  size_t index = IndexOf(key);
  return dir_[index]->Remove(key);
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  std::scoped_lock<std::mutex> lock(latch_);
  while (true) {
    size_t index = this->IndexOf(key);
    bool flag = dir_[index]->Insert(key, value);
    if (flag) {
      return;
    }
    int local_depth = dir_[index]->GetDepth();
    int global_depth = GetGlobalDepthInternal();
    if (local_depth < global_depth) {
      RedistributeBucket(dir_[index]);
    } else {
      ++global_depth_;
      size_t dir_size = dir_.size();
      dir_.resize(dir_size * 2);
      std::copy(dir_.begin(), dir_.begin() + dir_size, dir_.begin() + dir_size);
      index = IndexOf(key);
    }
  }
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::RedistributeBucket(std::shared_ptr<Bucket> bucket) -> void {
  num_buckets_++;
  bucket->IncrementDepth();
  int depth = bucket->GetDepth();
  int high_bit = 1 << (depth - 1);
  size_t old_index = std::hash<K>()(bucket->GetItems().front().first) & (high_bit - 1);
  auto p0 = std::make_shared<Bucket>(bucket_size_, depth);
  auto p1 = std::make_shared<Bucket>(bucket_size_, depth);
  auto items = bucket->GetItems();
  for (const auto &[k, v] : items) {
    size_t hash_value = std::hash<K>()(k);
    if ((hash_value & high_bit) != 0) {
      p1->Insert(k, v);
    } else {
      p0->Insert(k, v);
    }
  }
  for (size_t i = old_index; i < dir_.size(); i += high_bit) {
    dir_[i] = (i & high_bit) != 0 ? p1 : p0;
  }
}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  for (const auto &[k, v] : list_) {
    if (k == key) {
      value = v;
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  for (auto it = list_.begin(); it != list_.end(); it++) {
    if (it->first == key) {
      list_.erase(it);
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  for (auto &[k, v] : list_) {
    if (k == key) {
      v = value;
      return true;
    }
  }
  if (!IsFull()) {
    list_.emplace_back(key, value);
    return true;
  }
  return false;
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
