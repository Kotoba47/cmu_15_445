#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"

namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool { return root_page_id_ == INVALID_PAGE_ID; }
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) -> bool {
  if (IsEmpty()) {  // 如果为空
    return false;   // 返回false
  }
  auto page = FindLeafPage(key);  // 查找叶子节点
  if (page == nullptr) {          // 如果为空
    return false;                 // 返回false
  }
  auto leaf_page = reinterpret_cast<LeafPage *>(page->GetData());                        // 转换为叶子节点
  int index = leaf_page->KeyIndex(key, comparator_);                                     // 获取索引
  if (index < leaf_page->GetSize() && comparator_(leaf_page->KeyAt(index), key) == 0) {  // 如果索引小于大小且key相等
    result->emplace_back(leaf_page->ValueAt(index));                                     // 插入数据
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);                      // 释放
    return true;                                                                         // 返回true
  }
  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);  // 释放
  return false;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeafPage(const KeyType &key) -> Page * {
  if (IsEmpty()) {  // 如果为空
    return nullptr;
  }
  Page *page = buffer_pool_manager_->FetchPage(root_page_id_);                           // 获取根节点
  auto internal_page = reinterpret_cast<InternalPage *>(page->GetData());                // 转换为内部节点
  while (!internal_page->IsLeafPage()) {                                                 // 如果不是叶子节点
    page_id_t child_page_id = internal_page->Lookup(key, comparator_);                   // 查找key
    Page *child_page = buffer_pool_manager_->FetchPage(child_page_id);                   // 获取子节点
    auto child_internal_page = reinterpret_cast<InternalPage *>(child_page->GetData());  // 转换为内部节点
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);                           // 释放
    page = child_page;                                                                   // 设置为子节点
    internal_page = child_internal_page;                                                 // 设置为子节点
  }
  return page;  // 返回
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {
  Page *page = FindLeafPage(key);                                          // 获取叶子节点
  while (page == nullptr) {                                                // 如果为空
    if (IsEmpty()) {                                                       // 如果为空
      page_id_t page_id;                                                   // 页id
      Page *new_page = buffer_pool_manager_->NewPage(&page_id);            // 创建新页
      auto leaf_page = reinterpret_cast<LeafPage *>(new_page->GetData());  // 转换为叶子节点
      leaf_page->Init(page_id, INVALID_PAGE_ID, leaf_max_size_);           // 初始化
      root_page_id_ = page_id;                                             // 设置根节点
      UpdateRootPageId(1);                                                 // 更新根节点
      buffer_pool_manager_->UnpinPage(page_id, true);                      // 释放
    }
    page = FindLeafPage(key);  // 查找叶子节点
  }
  auto leaf_page = reinterpret_cast<LeafPage *>(page->GetData());                      // 转换为叶子节点
  int index = leaf_page->KeyIndex(key, comparator_);                                   // 获取索引
  bool is_insert = leaf_page->Insert(std::make_pair(key, value), index, comparator_);  // 插入数据
  if (!is_insert) {                                                                    // 如果插入失败
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);                         // 释放
    return false;                                                                      // 返回false
  }
  // 如果插入成功
  if (leaf_page->GetSize() == leaf_max_size_) {                                    // 如果大小等于最大大小
    page_id_t page_bother_id;                                                      // 兄弟节点id
    Page *page_bother = buffer_pool_manager_->NewPage(&page_bother_id);            // 创建新页
    auto leaf_bother_page = reinterpret_cast<LeafPage *>(page_bother->GetData());  // 转换为叶子节点
    leaf_bother_page->Init(page_bother_id, INVALID_PAGE_ID, leaf_max_size_);       // 初始化
    leaf_page->Split(page_bother);                                                 // 分裂
    InsertInParent(page, leaf_bother_page->KeyAt(0), page_bother);                 // 插入父节点
    buffer_pool_manager_->UnpinPage(page_bother->GetPageId(), true);               // 释放
  }
  buffer_pool_manager_->UnpinPage(page->GetPageId(), true);  // 释放
  return true;                                               // 返回true
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertInParent(Page *page_leaf, const KeyType &key, Page *page_bother) -> void {
  auto tree_page = reinterpret_cast<BPlusTreePage *>(page_leaf->GetData());             // 转换为b+树节点
  if (tree_page->GetPageId() == root_page_id_) {                                        // 如果是根节点
    page_id_t new_page_id;                                                              // 页id
    Page *new_page = buffer_pool_manager_->NewPage(&new_page_id);                       // 创建新页
    auto new_root = reinterpret_cast<InternalPage *>(new_page->GetData());              // 转换为内部节点
    new_root->Init(new_page_id, INVALID_PAGE_ID, internal_max_size_);                   // 初始化
    new_root->SetValueAt(0, page_leaf->GetPageId());                                    // 设置值
    new_root->SetKeyAt(1, key);                                                         // 设置key
    new_root->SetValueAt(1, page_bother->GetPageId());                                  // 设置值
    new_root->IncreaseSize(2);                                                          // 增加大小
    auto page_leaf_node = reinterpret_cast<BPlusTreePage *>(page_leaf->GetData());      // 转换为b+树节点
    page_leaf_node->SetParentPageId(new_page_id);                                       // 设置父节点
    auto page_bother_node = reinterpret_cast<BPlusTreePage *>(page_bother->GetData());  // 转换为b+树节点
    page_bother_node->SetParentPageId(new_page_id);                                     // 设置父节点
    root_page_id_ = new_page_id;                                                        // 设置根节点
    UpdateRootPageId(0);                                                                // 更新根节点
    buffer_pool_manager_->UnpinPage(new_page_id, true);                                 // 释放
    return;
  }
  page_id_t parent_id = tree_page->GetParentPageId();                                 // 获取父节点id
  Page *parent_page = buffer_pool_manager_->FetchPage(parent_id);                     // 获取父节点
  auto parent_node = reinterpret_cast<InternalPage *>(parent_page->GetData());        // 转换为内部节点
  auto page_bother_node = reinterpret_cast<InternalPage *>(page_bother->GetData());   // 转换为内部节点
  if (parent_node->GetSize() < parent_node->GetMaxSize()) {                           // 如果大小小于最大大小
    parent_node->Insert(std::make_pair(key, page_bother->GetPageId()), comparator_);  // 插入数据
    page_bother_node->SetParentPageId(parent_id);                                     // 设置父节点
    buffer_pool_manager_->UnpinPage(parent_id, true);                                 // 释放
    return;
  }
  page_id_t page_parent_bother_id;                                                              // 父兄弟节点id
  Page *page_parent_bother = buffer_pool_manager_->NewPage(&page_parent_bother_id);             // 创建新页
  auto parent_bother_node = reinterpret_cast<InternalPage *>(page_parent_bother->GetData());    // 转换为内部节点
  parent_bother_node->Init(page_parent_bother_id, INVALID_PAGE_ID, internal_max_size_);         // 初始化
  parent_node->Split(key, page_bother, page_parent_bother, comparator_, buffer_pool_manager_);  // 分裂
  InsertInParent(parent_page, parent_bother_node->KeyAt(0), page_parent_bother);                // 插入父节点
  buffer_pool_manager_->UnpinPage(page_parent_bother_id, true);                                 // 释放
  buffer_pool_manager_->UnpinPage(parent_id, true);                                             // 释放
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  if (IsEmpty()) {  // 如果为空
    return;         // 返回
  }
  auto leaf_page = FindLeafPage(key);  // 查找叶子节点
  if (leaf_page == nullptr) {
    return;
  }
  DeleteEntry(leaf_page, key);  // 删除数据
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::DeleteEntry(Page *&page, const KeyType &key) -> void {
  auto b_node = reinterpret_cast<BPlusTreePage *>(page->GetData());  // 转换为b+树节点
  if (b_node->IsLeafPage()) {                                        // 如果是叶子节点
    auto leaf_node = reinterpret_cast<LeafPage *>(page->GetData());  // 转换为叶子节点
    if (!leaf_node->Delete(key, comparator_)) {                      // 删除失败，说明没有这个key
      buffer_pool_manager_->UnpinPage(page->GetPageId(), false);     // 释放
      return;
    }
  } else {                                                                   // 如果不是叶子节点
    auto internal_node = reinterpret_cast<InternalPage *>(page->GetData());  // 转换为内部节点
    if (!internal_node->Delete(key, comparator_)) {                          // 删除失败
      buffer_pool_manager_->UnpinPage(page->GetPageId(), false);             // 释放
      return;
    }
  }
  if (b_node->GetPageId() == root_page_id_) {  // 如果是根节点
    AdjustRootPage(b_node);                    // 调整根节点
    return;
  }
  if (b_node->GetSize() < b_node->GetMinSize()) {  // 如果大小小于最小大小
    Page *bother_page;
    KeyType parent_key{};
    bool is_pre;
    auto inter_node = reinterpret_cast<InternalPage *>(page->GetData());          // 转换为内部节点
    auto parent_page_id = inter_node->GetParentPageId();                          // 获取父节点id
    auto parent_page = buffer_pool_manager_->FetchPage(parent_page_id);           // 获取父节点
    auto parent_node = reinterpret_cast<InternalPage *>(parent_page->GetData());  // 转换为内部节点
    parent_node->GetBotherPage(page->GetPageId(), bother_page, parent_key, is_pre,
                               buffer_pool_manager_);                              // 获取兄弟节点
    auto bother_node = reinterpret_cast<BPlusTreePage *>(bother_page->GetData());  // 转换为b+树节点
    if (b_node->GetSize() + bother_node->GetSize() <= b_node->GetMaxSize()) {  // 如果大小小于等于最大大小
      if (!is_pre) {
        std::swap(page, bother_page);
      }
      Coalesce(page, bother_page, parent_key);                           // 合并
      DeleteEntry(parent_page, parent_key);                              // 删除数据
    } else {                                                             // 如果大小大于最大大小
      Redistribute(page, bother_page, parent_page, parent_key, is_pre);  // 重分配
    }
  }
  buffer_pool_manager_->UnpinPage(page->GetPageId(), true);  // 释放
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::AdjustRootPage(BPlusTreePage *b_node) {
  if (b_node->IsLeafPage() && b_node->GetSize() == 0) {          // 如果是叶子节点且大小为0
    root_page_id_ = INVALID_PAGE_ID;                             // 设置根节点
    UpdateRootPageId(0);                                         // 更新根节点
    buffer_pool_manager_->UnpinPage(b_node->GetPageId(), true);  // 释放
    buffer_pool_manager_->DeletePage(b_node->GetPageId());       // 删除
    return;
  }
  if (!b_node->IsLeafPage() && b_node->GetSize() == 1) {
    auto inter_node = reinterpret_cast<InternalPage *>(b_node);  // 转换为内部节点
    root_page_id_ = inter_node->ValueAt(0);                      // 设置根节点
    UpdateRootPageId(0);                                         // 更新根节点
    buffer_pool_manager_->UnpinPage(b_node->GetPageId(), true);  // 释放
    buffer_pool_manager_->DeletePage(b_node->GetPageId());       // 删除
    return;
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Redistribute(Page *page, Page *bother_page, Page *parent_page, const KeyType &parent_key,
                                  bool ispre) {
  auto bother_node = reinterpret_cast<BPlusTreePage *>(bother_page->GetData());             // 转换为b+树节点
  if (bother_node->IsRootPage()) {                                                          // 如果是根节点
    auto inter_bother_node = reinterpret_cast<InternalPage *>(bother_page->GetData());      // 转换为内部节点
    auto inter_b_node = reinterpret_cast<InternalPage *>(page->GetData());                  // 转换为内部节点
    Page *child_page;                                                                       // 子节点
    KeyType key;                                                                            // key
    if (ispre) {                                                                            // 如果是前一个
      page_id_t last_value = inter_bother_node->ValueAt(inter_bother_node->GetSize() - 1);  // 获取最后一个值
      KeyType last_key = inter_bother_node->KeyAt(inter_bother_node->GetSize() - 1);        // 获取最后一个key
      inter_bother_node->Delete(last_key, comparator_);                                     // 删除数据
      inter_b_node->InsertFirst(parent_key, last_value);                                    // 插入数据
      child_page = buffer_pool_manager_->FetchPage(last_value);                             // 获取子节点
      key = last_key;                                                                       // 设置key
    } else {                                                                                // 如果是后一个
      page_id_t first_value = inter_bother_node->ValueAt(0);                                // 获取第一个值
      KeyType first_key = inter_bother_node->KeyAt(1);                                      // 获取第一个key
      inter_bother_node->DeleteFirst();                                                     // 删除第一个
      inter_b_node->Insert(std::make_pair(parent_key, first_value), comparator_);           // 插入数据
      child_page = buffer_pool_manager_->FetchPage(first_value);                            // 获取子节点
      key = first_key;                                                                      // 设置key
    }
    auto child_node = reinterpret_cast<BPlusTreePage *>(child_page->GetData());    // 转换为b+树节点
    if (child_node->IsLeafPage()) {                                                // 如果是叶子节点
      auto leaf_child_node = reinterpret_cast<LeafPage *>(child_page->GetData());  // 转换为叶子节点
      leaf_child_node->SetParentPageId(inter_b_node->GetPageId());                 // 设置父节点
    } else {
      auto inter_child_node = reinterpret_cast<InternalPage *>(child_page->GetData());  // 转换为内部节点
      inter_child_node->SetParentPageId(inter_b_node->GetPageId());                     // 设置父节点
    }
    buffer_pool_manager_->UnpinPage(child_page->GetPageId(), true);                       // 释放
    auto inter_parent_node = reinterpret_cast<InternalPage *>(parent_page->GetData());    // 转换为内部节点
    int index = inter_parent_node->KeyIndex(parent_key, comparator_);                     // 获取索引
    inter_parent_node->SetKeyAt(index, key);                                              // 设置key
    buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);                      // 释放
    buffer_pool_manager_->UnpinPage(page->GetPageId(), true);                             // 释放
    buffer_pool_manager_->UnpinPage(bother_page->GetPageId(), true);                      // 释放
  } else {                                                                                // 如果是叶子节点
    auto leaf_bother_node = reinterpret_cast<LeafPage *>(bother_page->GetData());         // 转换为叶子节点
    auto leaf_b_node = reinterpret_cast<LeafPage *>(page->GetData());                     // 转换为叶子节点
    KeyType key;                                                                          // key
    if (ispre) {                                                                          // 如果是前一个
      ValueType last_value = leaf_bother_node->ValueAt(leaf_bother_node->GetSize() - 1);  // 获取最后一个值
      KeyType last_key = leaf_bother_node->KeyAt(leaf_bother_node->GetSize() - 1);        // 获取最后一个key
      leaf_bother_node->Delete(last_key, comparator_);                                    // 删除数据
      leaf_b_node->InsertFirst(last_key, last_value);                                     // 插入数据
      key = last_key;                                                                     // 设置key
    } else {                                                                              // 如果是后一个
      ValueType first_value = leaf_bother_node->ValueAt(0);                               // 获取第一个值
      KeyType first_key = leaf_bother_node->KeyAt(0);                                     // 获取第一个key
      leaf_bother_node->Delete(first_key, comparator_);                                   // 删除数据
      leaf_b_node->InsertLast(first_key, first_value);                                    // 插入数据
      key = leaf_bother_node->KeyAt(0);                                                   // 设置key
    }
    auto inter_parent_node = reinterpret_cast<InternalPage *>(parent_page->GetData());  // 转换为内部节点
    int index = inter_parent_node->KeyIndex(parent_key, comparator_);                   // 获取索引
    inter_parent_node->SetKeyAt(index, key);                                            // 设置key
    buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);                    // 释放
    buffer_pool_manager_->UnpinPage(page->GetPageId(), true);                           // 释放
    buffer_pool_manager_->UnpinPage(bother_page->GetPageId(), true);                    // 释放
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Coalesce(Page *page, Page *bother_page, const KeyType &parent_key) {
  auto b_node = reinterpret_cast<BPlusTreePage *>(page->GetData());                     // 转换为b+树节点
  if (b_node->IsLeafPage()) {                                                           // 如果是叶子节点
    auto leaf_bother_node = reinterpret_cast<LeafPage *>(bother_page->GetData());       // 转换为叶子节点
    auto leaf_b_node = reinterpret_cast<LeafPage *>(page->GetData());                   // 转换为叶子节点
    leaf_bother_node->Merge(page, buffer_pool_manager_);                                // 合并
    leaf_bother_node->SetNextPageId(leaf_b_node->GetNextPageId());                      // 设置下一个节点
  } else {                                                                              // 如果内部节点
    auto inter_bother_node = reinterpret_cast<InternalPage *>(bother_page->GetData());  // 转换为内部节点
    inter_bother_node->Merge(parent_key, page, buffer_pool_manager_);                   // 合并
  }
  buffer_pool_manager_->UnpinPage(page->GetPageId(), true);         // 释放
  buffer_pool_manager_->DeletePage(page->GetPageId());              // 删除
  buffer_pool_manager_->UnpinPage(bother_page->GetPageId(), true);  // 释放
}
/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  if (IsEmpty()) {
    return INDEXITERATOR_TYPE();
  }
  Page *curr_page = buffer_pool_manager_->FetchPage(root_page_id_);
  auto curr_page_inter = reinterpret_cast<InternalPage *>(curr_page->GetData());
  while (!curr_page_inter->IsLeafPage()) {
    Page *next_page = buffer_pool_manager_->FetchPage(curr_page_inter->ValueAt(0));
    auto next_page_inter = reinterpret_cast<InternalPage *>(next_page->GetData());
    buffer_pool_manager_->UnpinPage(curr_page->GetPageId(), false);
    curr_page = next_page;
    curr_page_inter = next_page_inter;
  }
  return INDEXITERATOR_TYPE(curr_page->GetPageId(), curr_page, 0, buffer_pool_manager_);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  if (IsEmpty()) {
    return INDEXITERATOR_TYPE();
  }
  auto leaf_page = FindLeafPage(key);
  auto leaf_node = reinterpret_cast<LeafPage *>(leaf_page->GetData());
  int index;
  for (index = 0; index < leaf_node->GetSize(); index++) {
    if (comparator_(leaf_node->KeyAt(index), key) == 0) {
      break;
    }
  }
  if (index == leaf_node->GetSize()) {
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
    return End();
  }
  return INDEXITERATOR_TYPE(leaf_page->GetPageId(), leaf_page, index, buffer_pool_manager_);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE {
  if (IsEmpty()) {
    return INDEXITERATOR_TYPE();
  }
  Page *curr_page = buffer_pool_manager_->FetchPage(root_page_id_);
  auto curr_page_inter = reinterpret_cast<InternalPage *>(curr_page->GetData());
  while (!curr_page_inter->IsLeafPage()) {
    Page *next_page = buffer_pool_manager_->FetchPage(curr_page_inter->ValueAt(curr_page_inter->GetSize() - 1));
    auto next_page_inter = reinterpret_cast<InternalPage *>(next_page->GetData());
    buffer_pool_manager_->UnpinPage(curr_page->GetPageId(), false);
    curr_page = next_page;
    curr_page_inter = next_page_inter;
  }
  auto curr_node = reinterpret_cast<LeafPage *>(curr_page->GetData());
  page_id_t page_id = curr_page->GetPageId();
  buffer_pool_manager_->UnpinPage(curr_page->GetPageId(), false);
  return INDEXITERATOR_TYPE(page_id, curr_page, curr_node->GetSize(), buffer_pool_manager_);
}

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t { return root_page_id_; }

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  auto *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Draw an empty tree");
    return;
  }
  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  ToGraph(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm, out);
  out << "}" << std::endl;
  out.flush();
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  if (IsEmpty()) {
    LOG_WARN("Print an empty tree");
    return;
  }
  ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm);
}

/**
 * This method is used for debug only, You don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
