//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetMaxSize(max_size);
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetSize(0);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // replace with your own code
  return array_[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) { array_[index].first = key; }

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType { return array_[index].second; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetValueAt(int index, const ValueType &value) { array_[index].second = value; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::GetAt(int index) const -> MappingType { return array_[index]; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetAt(int index, const MappingType &value) { array_[index] = value; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(const KeyType &key, const KeyComparator &keyComparator) -> ValueType {
  int l = 1;
  int r = GetSize();
  while (l < r) {
    int mid = (l + r) / 2;
    if (keyComparator(array_[mid].first, key) <= 0) {
      l = mid + 1;
    } else {
      r = mid;
    }
  }
  return array_[r - 1].second;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Insert(const MappingType &value, const KeyComparator &keyComparator) -> void {
  for (int i = GetSize() - 1; i > 0; --i) {
    if (keyComparator(array_[i].first, value.first) > 0) {
      array_[i + 1] = array_[i];
    } else {
      array_[i + 1] = value;
      IncreaseSize(1);
      return;
    }
  }
  SetAt(1, value);
  IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Split(const KeyType &key, Page *page_bother, Page *page_parent_page,
                                           const KeyComparator &keyComparator, BufferPoolManager *buffer_pool_manager) {
  auto *tmp = static_cast<MappingType *>(malloc(sizeof(MappingType) * (GetMaxSize() + 1)));
  bool flag = true;    // 是否已经插入
  tmp[0] = array_[0];  // 0 是无效的
  for (int i = 1; i < GetMaxSize(); ++i) {
    if (keyComparator(array_[i].first, key) < 0) {  // 如果key小于分裂key
      tmp[i] = array_[i];
    } else if (flag && keyComparator(array_[i].first, key) > 0) {  // 如果key大于分裂key
      flag = false;                                                // 重置flag
      tmp[i] = std::make_pair(key, page_bother->GetPageId());
      tmp[i + 1] = array_[i];
    } else {  // 如果key等于分裂key
      tmp[i + 1] = array_[i];
    }
  }
  if (flag) {                                                           // 没有插入
    tmp[GetMaxSize()] = std::make_pair(key, page_bother->GetPageId());  // 在最后插入
  }
  auto page_bother_node = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_TYPE *>(page_bother->GetData());  // 转换为内部节点
  page_bother_node->SetParentPageId(GetPageId());  // 将分裂节点的父节点设置为当前节点
  IncreaseSize(1);                                 // 增加大小
  int mid = (GetMaxSize() + 1) / 2;                // 中间位置
  auto page_parent_node =
      reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_TYPE *>(page_parent_page->GetData());  // 转换为内部节点
  for (int i = 0; i < mid; ++i) {
    array_[i] = tmp[i];  // 复制数据
  }
  int i = 0;  // 重置i
  while (mid <= GetMaxSize()) {
    Page *child = buffer_pool_manager->FetchPage(tmp[mid].second);                           // 获取子节点
    auto child_node = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_TYPE *>(child->GetData());  // 转换为内部节点
    child_node->SetParentPageId(page_parent_node->GetPageId());                              // 设置父节点
    page_parent_node->array_[i++] = tmp[mid++];                                              // 复制数据
    page_parent_node->IncreaseSize(1);                                                       // 增加大小
    IncreaseSize(-1);                                                                        // 减少大小
    buffer_pool_manager->UnpinPage(child->GetPageId(), true);                                // 释放
  }
  free(tmp);  // 释放
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Delete(const KeyType &key, const KeyComparator &keyComparator) -> bool {
  int index = KeyIndex(key, keyComparator);                                  // 获取索引
  if (index >= GetSize() || keyComparator(array_[index].first, key) != 0) {  // 如果索引大于等于大小或者key不相等
    return false;
  }
  for (int i = index + 1; i < GetSize(); ++i) {  // 删除数据
    array_[i - 1] = array_[i];
  }
  IncreaseSize(-1);  // 减少大小
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyIndex(const KeyType &key, const KeyComparator &keyComparator) -> int {
  int l = 1;
  int r = GetSize();
  while (l < r) {
    int mid = (l + r) / 2;
    if (keyComparator(array_[mid].first, key) < 0) {
      l = mid + 1;
    } else {
      r = mid;
    }
  }
  return r;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::GetBotherPage(page_id_t child_page_id, Page *&bother_page, KeyType &key,
                                                   bool &ispre, BufferPoolManager *buffer_pool_manager) -> void {
  int i = 0;
  for (i = 0; i < GetSize(); ++i) {
    if (array_[i].second == child_page_id) {  // 如果子节点id等于child_page_id
      break;                                  // 说明找到了
    }
  }
  if (i >= 1) {                                                          // 如果大于等于1
    bother_page = buffer_pool_manager->FetchPage(array_[i - 1].second);  // 获取左兄弟节点
    key = array_[i - 1].first;                                           // 设置key
    ispre = true;                                                        // 设置为true
    return;
  }
  bother_page = buffer_pool_manager->FetchPage(array_[i + 1].second);  // 获取右兄弟节点
  key = array_[i + 1].first;                                           // 设置key
  ispre = false;                                                       // 设置为false
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Merge(const KeyType &key, Page *right_page, BufferPoolManager *buffer_pool_manager)
    -> void {
  auto right = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_TYPE *>(right_page->GetData());  // 转换为内部节点
  int size = GetSize();                                                                    // 获取大小
  array_[GetSize()] = std::make_pair(key, right->array_[0].second);                        // 插入数据
  IncreaseSize(1);                                                                         // 增加大小
  for (int i = GetSize(), j = 1; j < right->GetSize(); ++i, ++j) {
    array_[i] = std::make_pair(right->array_[j].first, right->array_[j].second);  // 插入数据
    IncreaseSize(1);                                                              // 增加大小
  }
  buffer_pool_manager->UnpinPage(right->GetPageId(), true);  // 释放
  buffer_pool_manager->DeletePage(right->GetPageId());       // 删除页
  for (int i = size; i < GetSize(); ++i) {
    page_id_t child_page_id = array_[i].second;                                                   // 获取子节点id
    auto child_page = buffer_pool_manager->FetchPage(child_page_id);                              // 获取子节点
    auto child_node = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_TYPE *>(child_page->GetData());  // 转换为内部节点
    child_node->SetParentPageId(GetPageId());                                                     // 设置父节点
    buffer_pool_manager->UnpinPage(child_page_id, true);                                          // 释放
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertFirst(const KeyType &key, const ValueType &value) -> void {
  for (int i = GetSize(); i > 0; --i) {
    array_[i] = array_[i - 1];
  }
  SetValueAt(0, value);  // 插入数据
  SetKeyAt(1, key);      // 插入数据
  IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::DeleteFirst() -> void {
  for (int i = 1; i < GetSize(); ++i) {
    array_[i - 1] = array_[i];
  }
  IncreaseSize(-1);
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
