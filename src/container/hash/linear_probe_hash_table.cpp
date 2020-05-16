//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// linear_probe_hash_table.cpp
//
// Identification: src/container/hash/linear_probe_hash_table.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "container/hash/linear_probe_hash_table.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_TYPE::LinearProbeHashTable(const std::string &name, BufferPoolManager *buffer_pool_manager,
                                      const KeyComparator &comparator, size_t num_buckets,
                                      HashFunction<KeyType> hash_fn)
    : buffer_pool_manager_(buffer_pool_manager), comparator_(comparator), hash_fn_(std::move(hash_fn)), num_blocks_(num_buckets) {
  // assume that the num_buckets is always no larger than 1020
  // 1. get a header page from the BufferPoolManager
  header_page_ = reinterpret_cast<HashTableHeaderPage *>(buffer_pool_manager_->NewPage(&header_page_id_, nullptr)->GetData());

  // 2. create block pages
  page_id_t block_page_id_tmp;
  for (size_t i = 0; i < num_buckets; i++) {
    buffer_pool_manager_->NewPage(&block_page_id_tmp, nullptr);
    header_page_->AddBlockPageId(block_page_id_tmp);
    buffer_pool_manager_->UnpinPage(block_page_id_tmp, true);
  }

  // 3. set header page metadata
  header_page_->SetPageId(header_page_id_);
  header_page_->SetSize(BLOCK_ARRAY_SIZE * num_buckets);
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) {
  uint64_t hash_res = hash_fn_.GetHash(key);
  page_id_t block_page_id = header_page_->GetBlockPageId(hash_res % num_blocks_);
  auto block_page = reinterpret_cast<HashTableBlockPage<KeyType, ValueType, KeyComparator> *>(buffer_pool_manager_->FetchPage(block_page_id)->GetData());

  slot_offset_t buck_ind = hash_res % BLOCK_ARRAY_SIZE;
  while (buck_ind < BLOCK_ARRAY_SIZE) {
    if (!block_page->IsOccupied(buck_ind)) { break; }
    if (block_page->IsReadable(buck_ind)) { result->push_back(block_page->ValueAt(buck_ind)); }
    buck_ind++;
  }

  return true;
}
/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  // only returns false if it tries to insert an existing key-value pair.

  uint64_t hash_res = hash_fn_.GetHash(key);
  page_id_t block_page_id = header_page_->GetBlockPageId(hash_res % num_blocks_);
  auto block_page = reinterpret_cast<HashTableBlockPage<KeyType, ValueType, KeyComparator> *>(buffer_pool_manager_->FetchPage(block_page_id)->GetData());

  slot_offset_t bucket_ind = hash_res % BLOCK_ARRAY_SIZE;
  while (bucket_ind < BLOCK_ARRAY_SIZE) {
    if (block_page->IsReadable(bucket_ind) && block_page->ValueAt(bucket_ind) == value) {
      std::cout << "Cannot insert duplicate values for the same key." << std::endl;
      return false;
    }
    if (block_page->Insert(bucket_ind, key, value)) { return true; }
    bucket_ind++;
  }

   std::cout << "Block page [" << block_page_id << "] is full and cannot insert any more kv pair." << std::endl;
   return false;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) {
  uint64_t hash_res = hash_fn_.GetHash(key);
  page_id_t block_page_id = header_page_->GetBlockPageId(hash_res % num_blocks_);
  auto block_page = reinterpret_cast<HashTableBlockPage<KeyType, ValueType, KeyComparator> *>(buffer_pool_manager_->FetchPage(block_page_id)->GetData());

  slot_offset_t bucket_ind = hash_res % BLOCK_ARRAY_SIZE;
  while (bucket_ind < BLOCK_ARRAY_SIZE) {
    if (block_page->IsReadable(bucket_ind) && block_page->ValueAt(bucket_ind) == value) {
      block_page->Remove(bucket_ind);
      return true;
    }
    if (!block_page->IsOccupied(bucket_ind)) { break; }
    bucket_ind++;
  }
  return false;
}

/*****************************************************************************
 * RESIZE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Resize(size_t initial_size) {}

/*****************************************************************************
 * GETSIZE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
size_t HASH_TABLE_TYPE::GetSize() {
  return 0;
}

template class LinearProbeHashTable<int, int, IntComparator>;

template class LinearProbeHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class LinearProbeHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class LinearProbeHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class LinearProbeHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class LinearProbeHashTable<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
