/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator() = default;

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(page_id_t page_id, Page *page, int index, BufferPoolManager *buffer_pool_manager)
    : page_id_(page_id), page_(page), index_(index), buffer_pool_manager_(buffer_pool_manager) {}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() = default;  // NOLINT

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool {
  auto tree_page = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(page_->GetData());
  return static_cast<bool>(index_ == tree_page->GetSize() && tree_page->GetNextPageId() == INVALID_PAGE_ID);
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & {
  auto tree_page = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(page_->GetData());
  return tree_page->GetAtIter(index_);
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  ++index_;
  auto tree_page = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(page_->GetData());
  if (index_ == tree_page->GetSize() && tree_page->GetNextPageId() != INVALID_PAGE_ID) {
    auto next_page = buffer_pool_manager_->FetchPage(tree_page->GetNextPageId());
    buffer_pool_manager_->UnpinPage(tree_page->GetPageId(), false);
    page_ = next_page;
    page_id_ = page_->GetPageId();
    index_ = 0;
  }
  if (index_ == tree_page->GetSize() && tree_page->GetNextPageId() == INVALID_PAGE_ID) {
    buffer_pool_manager_->UnpinPage(tree_page->GetPageId(), false);
  }
  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
