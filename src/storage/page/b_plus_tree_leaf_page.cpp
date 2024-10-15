//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetMaxSize(max_size);
  SetPageType(IndexPageType::LEAF_PAGE);
  SetSize(0);
  SetNextPageId(INVALID_PAGE_ID);
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t { return next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // replace with your own code
  return array_[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) -> void {
  // replace with your own code
  array_[index].first = key;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::ValueAt(int index) const -> ValueType {
  // replace with your own code
  return array_[index].second;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::SetValueAt(int index, const ValueType &value) -> void {
  // replace with your own code
  array_[index].second = value;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetAt(int index) -> MappingType { return array_[index]; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetAt(int index, const MappingType &value) { array_[index] = value; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetAtIter(int index) -> MappingType & { return array_[index]; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(MappingType value, int index, const KeyComparator &keyComparator) -> bool {
  if (index < GetSize() && keyComparator(value.first, array_[index].first) == 0) {  // 如果key相等
    return false;
  }
  for (int i = GetSize() - 1; i >= index; --i) {
    array_[i + 1] = array_[i];  // 移动数据
  }
  array_[index] = value;  // 插入数据
  IncreaseSize(1);        // 增加大小
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyIndex(const KeyType &key, const KeyComparator &keyComparator) -> int {
  int l = 0;
  int r = GetSize();  // 左右指针
  while (l < r) {
    int mid = (l + r) / 2;
    if (keyComparator(array_[mid].first, key) < 0) {
      l = mid + 1;
    } else {
      r = mid;
    }
  }
  return l;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Split(Page *bother_page) -> void {
  int mid = GetSize() / 2;                                                                         // 中间位置
  auto leaf_bother_page = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(bother_page->GetData());  // 转换为叶子节点
  for (int i = mid, j = 0; i < GetMaxSize(); ++i, ++j) {
    leaf_bother_page->array_[j] = array_[i];  // 移动数据
    IncreaseSize(-1);                         // 减少大小
    leaf_bother_page->IncreaseSize(1);        // 增加大小
  }
  leaf_bother_page->next_page_id_ = next_page_id_;  // 设置下一个节点
  SetNextPageId(bother_page->GetPageId());          // 设置下一个节点
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Remove(const KeyType &key, int index, const KeyComparator &keyComparator) -> bool {
  if (keyComparator(array_[index].first, key) != 0) {  // 如果key不相等
    return false;
  }
  for (int i = index; i < GetSize() - 1; ++i) {
    array_[i] = array_[i + 1];  // 移动数据
  }
  IncreaseSize(-1);  // 减少大小
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Delete(const KeyType &key, const KeyComparator &keyComparator) -> bool {
  int index = KeyIndex(key, keyComparator);                           // 获取索引
  if (index >= GetSize() || keyComparator(KeyAt(index), key) != 0) {  // 如果索引大于等于大小或者key不相等
    return false;
  }
  for (int i = index + 1; i < GetSize(); ++i) {  // 移动数据
    array_[i - 1] = array_[i];                   // 移动数据
  }
  IncreaseSize(-1);  // 减少大小
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Merge(Page *right_page, BufferPoolManager *buffer_pool_manager_) -> void {
  auto right = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(right_page->GetData());  // 转换为叶子节点
  for (int i = GetSize(), j = 0; j < right->GetSize(); ++i, ++j) {
    array_[i] = std::make_pair(right->KeyAt(j), right->ValueAt(j));  // 插入数据
    IncreaseSize(1);                                                 // 增加大小
  }
  right->SetSize(0);                                               // 设置大小
  buffer_pool_manager_->UnpinPage(right_page->GetPageId(), true);  // 释放
  buffer_pool_manager_->DeletePage(right_page->GetPageId());       // 删除
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::InsertFirst(const KeyType &key, const ValueType &value) -> void {
  for (int i = GetSize(); i > 0; --i) {
    array_[i] = array_[i - 1];  // 移动数据
  }
  array_[0] = std::make_pair(key, value);  // 插入数据
  IncreaseSize(1);                         // 增加大小
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::InsertLast(const KeyType &key, const ValueType &value) -> void {
  array_[GetSize()] = std::make_pair(key, value);  // 插入数据
  IncreaseSize(1);                                 // 增加大小
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
