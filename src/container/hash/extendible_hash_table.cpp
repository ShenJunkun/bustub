//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "container/hash/extendible_hash_table.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_TYPE::ExtendibleHashTable(const std::string &name, BufferPoolManager *buffer_pool_manager,
                                     const KeyComparator &comparator, HashFunction<KeyType> hash_fn)
    : buffer_pool_manager_(buffer_pool_manager), comparator_(comparator), hash_fn_(std::move(hash_fn)) {
  //  implement me!
  Page* pg = buffer_pool_manager_->NewPage(&directory_page_id_);
  HashTableDirectoryPage* hashTableDirectoryPage = reinterpret_cast<HashTableDirectoryPage*>(pg->GetData());
  hashTableDirectoryPage->SetPageId(directory_page_id_);

  page_id_t bktPgId = INVALID_PAGE_ID;
  buffer_pool_manager_->NewPage(&bktPgId);
  // HASH_TABLE_BUCKET_TYPE* hashTableBucketPage = reinterpret_cast<HASH_TABLE_BUCKET_TYPE*> (bktPg->GetData());
  hashTableDirectoryPage->SetBucketPageId(0, bktPgId);
  buffer_pool_manager_->UnpinPage(bktPgId, true);
  buffer_pool_manager_->UnpinPage(directory_page_id_, true);
}

/*****************************************************************************
 * HELPERS
 *****************************************************************************/
/**
 * Hash - simple helper to downcast MurmurHash's 64-bit hash to 32-bit
 * for extendible hashing.
 *
 * @param key the key to hash
 * @return the downcasted 32-bit hash
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::Hash(KeyType key) {
  return static_cast<uint32_t>(hash_fn_.GetHash(key));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline uint32_t HASH_TABLE_TYPE::KeyToDirectoryIndex(KeyType key, HashTableDirectoryPage *dir_page) {
  return Hash(key) & dir_page->GetGlobalDepthMask();
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline uint32_t HASH_TABLE_TYPE::KeyToPageId(KeyType key, HashTableDirectoryPage *dir_page) {
  uint32_t idx =  KeyToDirectoryIndex(key, dir_page);
  return dir_page->GetBucketPageId(idx);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HashTableDirectoryPage *HASH_TABLE_TYPE::FetchDirectoryPage() {
  return reinterpret_cast<HashTableDirectoryPage*> (buffer_pool_manager_->FetchPage(directory_page_id_)->GetData());
} 

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_BUCKET_TYPE *HASH_TABLE_TYPE::FetchBucketPage(page_id_t bucket_page_id) {
  return reinterpret_cast<HASH_TABLE_BUCKET_TYPE*> (buffer_pool_manager_->FetchPage(bucket_page_id)->GetData());
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) {
  //TODO
  table_latch_.RLock();
  HashTableDirectoryPage * dirPg= FetchDirectoryPage();
  uint32_t bktPgId = KeyToPageId(key, dirPg);
  table_latch_.RUnlock();
  buffer_pool_manager_->UnpinPage(directory_page_id_, true);
  HASH_TABLE_BUCKET_TYPE * bktPg = FetchBucketPage(bktPgId);
  auto pg = reinterpret_cast<Page *> (bktPg);
  pg->RLatch();
  bool res = bktPg->GetValue(key, comparator_, result);
  pg->RUnlatch();
  buffer_pool_manager_->UnpinPage(bktPgId, true);
  return res;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  //TODO
  table_latch_.RLock();
  HashTableDirectoryPage * dirPg= FetchDirectoryPage();
  uint32_t bktPgId = KeyToPageId(key, dirPg);
  table_latch_.RUnlock();
  buffer_pool_manager_->UnpinPage(directory_page_id_, true);

  HASH_TABLE_BUCKET_TYPE * bktPg = FetchBucketPage(bktPgId);
  auto pg = reinterpret_cast<Page *> (bktPg);
  pg->WLatch();
  if (bktPg->IsFull()) {
    pg->WUnlatch();
    buffer_pool_manager_->UnpinPage(bktPgId, true);
    return SplitInsert(transaction, key, value);
  } else {
    bool res = bktPg->Insert(key, value, comparator_);
    pg->WUnlatch();
    buffer_pool_manager_->UnpinPage(bktPgId, true);
    return res;
  }
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::SplitInsert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.RLock();
  HashTableDirectoryPage * dirPg= FetchDirectoryPage();
  //根据key找到bucket, 判断bucket是否已满
  //   如果未满，这进行插入操作
  //   如果已满，查看loal depth
  //      如果local depth + 1后，大于global depth, global depth + 1,更新directory page中数据
  //      如果local depth + 1后，小于等于global depth, 则对该bucket中的数据进行分配即可

  uint32_t bktPgId = KeyToPageId(key, dirPg);

  HASH_TABLE_BUCKET_TYPE * bktPg = FetchBucketPage(bktPgId);

  auto pg = reinterpret_cast<Page *> (bktPg);
  pg->WLatch();

  if (bktPg->IsFull()) {
    table_latch_.RUnlock();
    table_latch_.WLock();

    auto idx = KeyToDirectoryIndex(key, dirPg);
    dirPg->IncrLocalDepth(idx);
    auto local_depth = dirPg->GetLocalDepth(idx);
    if (local_depth <= dirPg->GetGlobalDepth()) {
      //不需要增加global depth
      page_id_t newPid;
      auto newPg = buffer_pool_manager_->NewPage(&newPid);
      HASH_TABLE_BUCKET_TYPE* newBktPg = reinterpret_cast<HASH_TABLE_BUCKET_TYPE*>(newPg);
      newPg->WLatch();
      //写入数据
      //new page中插入和插入值一样的hash数值，原始页负责其他部分
      memcpy(newPg->GetData(), pg->GetData(), PAGE_SIZE);

      //将new page中和hash val不一样的设置为不可读
      auto hash_val = Hash(key) & ((1 << local_depth) -1 );

      for (size_t i = 0; i < newBktPg->NumReadable(); i++) {
        auto hash_tmp = Hash(newBktPg->KeyAt(i)) & ((1 << local_depth) -1);
        if (hash_tmp != hash_val) {
          newBktPg->SetUnreadable(i);
        }
      }

      // newBktPg->Insert(key, value, comparator_);
      newPg->WUnlatch();
      buffer_pool_manager_->UnpinPage(newPid, true);

      for (size_t i = 0; i < bktPg->NumReadable(); i++) {
        auto hash_tmp = Hash(bktPg->KeyAt(i)) & ((1 << local_depth) - 1);
        if (hash_tmp == hash_val) {
          newBktPg->SetUnreadable(i);
        }
      }
      pg->WUnlatch();
      buffer_pool_manager_->UnpinPage(bktPgId, true);

      //更新direct page
      for (size_t i = 0; i < dirPg->Size(); i++) {

        if (uint32_t(dirPg->GetBucketPageId(i)) == bktPgId) {
          if ( (i & ((1<<local_depth)-1) ) == hash_val) {
            dirPg->SetBucketPageId(i, newPid);
          } else {
            dirPg->SetBucketPageId(i, bktPgId);
          }
          dirPg->SetLocalDepth(i, local_depth);
        }
      }
      table_latch_.WUnlock();
      buffer_pool_manager_->UnpinPage(directory_page_id_, true);

    } else {
      //需要增加global depth

      //更新global depth
      //更新dic pg
      size_t oldDepth = dirPg->GetGlobalDepth();
      dirPg->IncrGlobalDepth();
 
      size_t base = 1 << oldDepth;

      for (size_t i = base; i < dirPg->Size(); i++) {
        dirPg->SetBucketPageId(i, dirPg->GetBucketPageId(i % base));
        dirPg->SetLocalDepth(i, dirPg->GetLocalDepth(i % base));
      }

      //新建一个page
      page_id_t newPid;
      auto newPg = buffer_pool_manager_->NewPage(&newPid);
      HASH_TABLE_BUCKET_TYPE* newBktPg = reinterpret_cast<HASH_TABLE_BUCKET_TYPE*>(newPg);
      newPg->WLatch();
      //写入数据
      //new page中插入和插入值一样的hash数值，原始页负责其他部分
      memcpy(newPg->GetData(), pg->GetData(), PAGE_SIZE);

      auto new_depth = dirPg->GetGlobalDepth();
      //将new page中和hash val不一样的设置为不可读
      auto hash_val = Hash(key) & ((1 << new_depth) -1 );
      for (size_t i = 0; i < newBktPg->NumReadable(); i++) {
        auto hash_tmp = Hash(newBktPg->KeyAt(i)) & ((1 << new_depth) -1);
        if (hash_tmp != hash_val) {
          newBktPg->SetUnreadable(i);
        }
      }
      // newBktPg->Insert(key, value, comparator_);
      newPg->WUnlatch();
      buffer_pool_manager_->UnpinPage(newPid, true);

      for (size_t i = 0; i < bktPg->NumReadable(); i++) {
        auto hash_tmp = Hash(bktPg->KeyAt(i)) & ((1 << new_depth) - 1);
        if (hash_tmp == hash_val) {
          bktPg->SetUnreadable(i);
        }
      }

      //更新direct page
      dirPg->SetBucketPageId(hash_val, newPid);
      dirPg->SetLocalDepth(hash_val, new_depth);
      auto hash_else = hash_val ^ (1 << (new_depth - 1));
      dirPg->SetBucketPageId(hash_else, bktPgId);
      dirPg->SetLocalDepth(hash_else, new_depth);
            
      pg->WUnlatch();
      table_latch_.WUnlock();
      buffer_pool_manager_->UnpinPage(bktPgId, true);
      buffer_pool_manager_->UnpinPage(directory_page_id_, true);

    }
    return SplitInsert(transaction, key, value);

  } else {
    //第二次检查，如果没有满，就执行插入
    bool res = bktPg->Insert(key, value, comparator_);
    pg->WUnlatch();
    table_latch_.RUnlock();
    buffer_pool_manager_->UnpinPage(bktPgId, true);
    buffer_pool_manager_->UnpinPage(directory_page_id_, true);
    return res;
  }

}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) {
  //TODO
  //将这个值标记为不可读的（删除）
  //  删除后，如果该页的size 为 0 ，则找到pair，查看local depth是否相同，如果相同，则进行和饼。
  table_latch_.RLock();
  HashTableDirectoryPage * dirPg= FetchDirectoryPage();
  uint32_t bktPgId = KeyToPageId(key, dirPg);

  HASH_TABLE_BUCKET_TYPE * bktPg = FetchBucketPage(bktPgId);

  auto pg = reinterpret_cast<Page *> (bktPg);
  pg->WLatch();
  bool res = bktPg->Remove(key, value, comparator_);
  bool isEmpty = bktPg->IsEmpty();
  pg->WUnlatch();
  table_latch_.RUnlock();
  buffer_pool_manager_->UnpinPage(bktPgId, true);
  buffer_pool_manager_->UnpinPage(directory_page_id_, true);
  if (res) {
    if (isEmpty) {
      Merge(transaction, key, value);
    }
    return true;
  } else {
    return false;
  }
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Merge(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.WLock();
  HashTableDirectoryPage * dirPg= FetchDirectoryPage();
  uint32_t bktPgId = KeyToPageId(key, dirPg);
  auto idx = KeyToDirectoryIndex(key, dirPg);
  HASH_TABLE_BUCKET_TYPE * bktPg = FetchBucketPage(bktPgId);

  auto pg = reinterpret_cast<Page *> (bktPg);
  pg->WLatch();
  if (bktPg->IsEmpty() && dirPg->GetLocalDepth(idx) > 0) {
    //
    auto local_depth = dirPg->GetLocalDepth(idx);
    auto hash_val = Hash(key) & ((1 << local_depth) -1 );
    auto hash_else = hash_val ^ (1 << (local_depth - 1));
    bool canMerge = true;
    page_id_t pgElseID = INVALID_PAGE_ID;
    for (size_t i = 0; i < dirPg->Size(); i++) {
      auto temp = i & ((1 <<local_depth) -1 );
      if (temp == hash_else && dirPg->GetLocalDepth(i) != local_depth) {
        canMerge = false; 
      } 
      if (temp == hash_else && dirPg->GetLocalDepth(i) == local_depth) {
        pgElseID = dirPg->GetBucketPageId(i);
      }
    } 

    if(canMerge) {
      //删除页面
      buffer_pool_manager_->UnpinPage(bktPgId, true);
      buffer_pool_manager_->DeletePage(bktPgId);
      //更新direct page
      for (size_t i = 0; i < dirPg->Size(); i++) {
        if (uint32_t(dirPg->GetBucketPageId(i)) == bktPgId || dirPg->GetBucketPageId(i) == pgElseID) {
          dirPg->DecrLocalDepth(i);
        }

        if ( uint32_t(dirPg->GetBucketPageId(i)) == bktPgId) {
          dirPg->SetBucketPageId(i, pgElseID);
        }

        if (dirPg->CanShrink()) {
          dirPg->DecrGlobalDepth();
        }
      }
    } 

  } else {
    pg->WUnlatch();
    table_latch_.WUnlock();
    buffer_pool_manager_->UnpinPage(bktPgId, true);
    buffer_pool_manager_->UnpinPage(directory_page_id_, true);
    return;  
  }

  pg->WUnlatch();
  table_latch_.WUnlock();
  buffer_pool_manager_->UnpinPage(bktPgId, true);
  buffer_pool_manager_->UnpinPage(directory_page_id_, true);  
  Merge(transaction, key, value);
}

/*****************************************************************************
 * GETGLOBALDEPTH - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::GetGlobalDepth() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t global_depth = dir_page->GetGlobalDepth();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
  return global_depth;
}

/*****************************************************************************
 * VERIFY INTEGRITY - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::VerifyIntegrity() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  dir_page->VerifyIntegrity();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
}

/*****************************************************************************
 * TEMPLATE DEFINITIONS - DO NOT TOUCH
 *****************************************************************************/
template class ExtendibleHashTable<int, int, IntComparator>;

template class ExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class ExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class ExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class ExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class ExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
