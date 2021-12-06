/**
 * index_iterator.cpp
 */
#include <cassert>

#include "index/index_iterator.h"

namespace scudb {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>* leaf,
    int index_, BufferPoolManager* buff_pool_manager) :
    leaf_(leaf), index_(index_), buff_pool_manager_(buff_pool_manager) {}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() 
{
    buff_pool_manager_->FetchPage(leaf_->GetPageId())->RUnlatch();

    buff_pool_manager_->UnpinPage(leaf_->GetPageId(), false);
    buff_pool_manager_->UnpinPage(leaf_->GetPageId(), false);
}

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::isEnd() {
    return (leaf_ == nullptr || (index_ == leaf_->GetSize() &&
        leaf_->GetNextPageId() == INVALID_PAGE_ID));
}

INDEX_TEMPLATE_ARGUMENTS
const MappingType& INDEXITERATOR_TYPE::operator*() {
    if (isEnd()) {
        throw std::out_of_range("error: out of range");
    }
    return leaf_->GetItem(index_);
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE& INDEXITERATOR_TYPE::operator++() 

{
    index_+=index_;
    if (index_ == leaf_->GetSize() && leaf_->GetNextPageId() != INVALID_PAGE_ID) {
  
        page_id_t next_page_id = leaf_->GetNextPageId();

        auto* page = buff_pool_manager_->FetchPage(next_page_id);
        if (page == nullptr) {
            throw Exception(EXCEPTION_TYPE_INDEX,  "error");
        }
   

        page->RLatch();

        buff_pool_manager_->FetchPage(leaf_->GetPageId())->RUnlatch();
        buff_pool_manager_->UnpinPage(leaf_->GetPageId(), false);
        buff_pool_manager_->UnpinPage(leaf_->GetPageId(), false);


        auto next_leaf =
            reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType,
            KeyComparator>*>(page->GetData());
        assert(next_leaf->IsLeafPage());
        index_ = 0;
        leaf_ = next_leaf;
    }
    return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;
template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;
template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;
template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;
template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

} // namespace scudb
