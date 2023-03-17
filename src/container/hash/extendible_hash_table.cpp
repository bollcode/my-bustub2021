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

#include <fstream>
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

/**
 * 返回key对应的再directory中的bucket_page_ids_的下标，也是很多函数传入的值
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
inline uint32_t HASH_TABLE_TYPE::KeyToDirectoryIndex(KeyType key, HashTableDirectoryPage *dir_page) {
  uint32_t index = Hash(key) & dir_page->GetGlobalDepthMask();
  return index;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline uint32_t HASH_TABLE_TYPE::KeyToPageId(KeyType key, HashTableDirectoryPage *dir_page) {
  uint32_t bucket_idx = KeyToDirectoryIndex(key, dir_page);
  uint32_t pageId = dir_page->GetBucketPageId(bucket_idx);
  return pageId;
}

/**
 * 申请一个页作为HashTableDirectoryPage，然后设置这个页的基本变量
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
HashTableDirectoryPage *HASH_TABLE_TYPE::FetchDirectoryPage() {
  HashTableDirectoryPage *directory_page  = nullptr;
  // // 因为directory_page_id_是一个公共变量，防止多个线程同时申请创建directory_page
  // my_lock_.lock();
  if (directory_page_id_ == INVALID_PAGE_ID) {
    // 从bufferPool中获取一个page用来充当这个directory_page
    LOG_DEBUG("create new directory");
    page_id_t pageid;
    // 将获取的新页强制转换成HashTableDirectoryPage类型的，
    // data才是页的真实数据，也就是HashTableDirectoryPage要存储的空间
    directory_page  = reinterpret_cast<HashTableDirectoryPage *>(AssertPage(buffer_pool_manager_->NewPage(&pageid))->GetData());
    directory_page_id_ = pageid;
    directory_page ->SetPageId(directory_page_id_);
    assert(directory_page_id_ != INVALID_PAGE_ID);
    // 创建完成一个HashTableDirectoryPage 即directory后，要创建一个bucket 0
    page_id_t bucket_pageid = INVALID_PAGE_ID;
    // 申请一个页，并将页的id放在bucket_pageid中
    AssertPage(buffer_pool_manager_->NewPage(&bucket_pageid));
    // directory_page ->IncrGlobalDepth();
    directory_page ->SetBucketPageId(0, bucket_pageid);
    // 申请完成之后要unpin,放到lru队列中
    assert(buffer_pool_manager_->UnpinPage(bucket_pageid, true));
    assert(buffer_pool_manager_->UnpinPage(directory_page_id_, true));
  }
  // my_lock_.unlock();
  // 重新从bufferpool中获取，上面时在bufferpool中创建
  assert(directory_page_id_ != INVALID_PAGE_ID);
  // FetchPage会在内部pin这个页面
  directory_page  = reinterpret_cast<HashTableDirectoryPage *>(AssertPage(buffer_pool_manager_
                                                    ->FetchPage(directory_page_id_))->GetData());
  return directory_page ;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
Page *HASH_TABLE_TYPE::FetchBucketPage(page_id_t bucket_page_id) {
  return AssertPage(buffer_pool_manager_->FetchPage(bucket_page_id));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_BUCKET_TYPE *HASH_TABLE_TYPE::RetrieveBucket(Page *page) {
  return reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(page->GetData());
}

/*****************************************************************************
 * SEARCH  获取key对应的value，
 * 先获取这个directory，然后根据用hash，得到key对应的bucket index，然后获取到
 * bucket_page_id，然后拿到这个页，然后遍历这个页中的array，查找key对应的pair，然后放进这个result中
 * 在获取某个bucketpage的页的时候，会加一个读锁，防止别的进程修改？
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) {
  table_latch_.RLock();  // 申请一个读锁
  // 这里我们pin了这个页
  HashTableDirectoryPage *dir = FetchDirectoryPage();
  // uint32_t bucket_idx = Hash(key);
  page_id_t bucket_pageid = KeyToPageId(key, dir);
  // 这里我们pin了这个页
  Page *bucket_page = FetchBucketPage(bucket_pageid);

  bucket_page->RLatch();  // 申请一个page 的读锁
  HASH_TABLE_BUCKET_TYPE *bucket = RetrieveBucket(bucket_page);
  bool res =  bucket->GetValue(key, comparator_, result);
  bucket_page->RUnlatch();  // 释放这个page的读锁
  // 对bucketpage要进行unpin操作
  // 对dirpage要进行unpin操作
  assert(buffer_pool_manager_->UnpinPage(bucket_pageid, false));
  assert(buffer_pool_manager_->UnpinPage(dir->GetPageId(), false));
  table_latch_.RUnlock();  // 释放锁
  return res;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  // LOG_INFO("enter insert function");
  // 这个锁是用来针对这个整个对象来说的，因为这个对象是线程共享的，所以必须保证所有的方法都是线程安全的
  // 别的不能执行insert操作，但是为什么还要给page锁呢，因为别的非insert函数可能会执行此page的变更操作
  table_latch_.RLock();
  HashTableDirectoryPage *dir = FetchDirectoryPage();  // 获取这个directory，然后pin
  page_id_t bucket_pageid = KeyToPageId(key, dir);
  Page *bucket_page = FetchBucketPage(bucket_pageid);  // 获取这个桶对应的page
  bucket_page->WLatch();  // 获取这个page的写锁，保证别的地方不能操作这个page对象
  HASH_TABLE_BUCKET_TYPE *bucket = RetrieveBucket(bucket_page);  // 转换成HASH_TABLE_BUCKET_TYPE
  if (!bucket->IsFull()) {
    // 判断这个bucket页是否已经满了
    bool res = bucket->Insert(key, value, comparator_);
    bucket_page->WUnlatch();  // 对这个page的锁释放
    assert(buffer_pool_manager_->UnpinPage(bucket_pageid, true));  // 对这个bucketpage执行unpin，并标志这个page是个脏页
    assert(buffer_pool_manager_->UnpinPage(dir->GetPageId(), false));  // 对这个diretorypage进行unpin操作
    table_latch_.RUnlock();
    return res;
  }
  // 说明这个bucket已经满了，那么就需要进行分裂
  // 现在要先解锁，？？？？要解锁吗？
  bucket_page->WUnlatch();  // 对这个page的锁释放
  assert(buffer_pool_manager_->UnpinPage(bucket_pageid, false));  // 对这个bucketpage执行unpin，并标志这个page不是脏页
  assert(buffer_pool_manager_->UnpinPage(dir->GetPageId(), false));  // 对这个diretorypage进行unpin操作
  table_latch_.RUnlock();
  return SplitInsert(transaction, key, value);
}
/**
 * 这个是如果要插入的bucket满了，那么就分裂
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::SplitInsert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.WLock();  // 获取这个对象的全局锁
  HashTableDirectoryPage *dir = FetchDirectoryPage();  // 获取dir
  int64_t split_bucket_index = KeyToDirectoryIndex(key, dir);  // 获取这个key对应的dirctory的index，也是要分裂的下标
  uint32_t split_bucket_depth = dir->GetLocalDepth(split_bucket_index);  // 获取这个要分割的bucket的深度
  if (split_bucket_depth >= MAX_BUCKET_DEPTH) {
    // 表示已经满了，无法分割了
    assert(buffer_pool_manager_->UnpinPage(dir->GetPageId(), false));
    table_latch_.WUnlock();
    return false;
  }
  if (split_bucket_depth == dir->GetGlobalDepth()) {
    // 给dir的entry grow一倍
    dir->IncrGlobalDepth();
  }
  // 获取这个要分割的桶页
  page_id_t split_bucket_page_id = KeyToPageId(key, dir);
  Page *split_page = FetchBucketPage(split_bucket_page_id);
  split_page->WLatch();  // 给这个页上锁
  HASH_TABLE_BUCKET_TYPE *split_bucket = RetrieveBucket(split_page);

  MappingType *origin_array = split_bucket->GetArrayCopy();
  uint32_t origin_array_size = split_bucket->NumReadable();
  // 给原来的要分割的页全部清理干净
  split_bucket->Clear();

  // 然后将开始申请一个新的页作为桶页分割后存储的页
  page_id_t image_bunket_page_id;
  HASH_TABLE_BUCKET_TYPE *image_bucket = RetrieveBucket(
    AssertPage(buffer_pool_manager_->NewPage(&image_bunket_page_id)));

  // 增加这个localdepth
  dir->IncrLocalDepth(split_bucket_index);
  // 将index的  旧localdepth +1 位取反，为了我们能够找到所有的 localdepth个位 和 key对应的相同的index
  uint32_t split_image_bucket_index = dir->GetSplitImageIndex(split_bucket_index);
  dir->SetLocalDepth(split_image_bucket_index, dir->GetLocalDepth(split_bucket_index));
  dir->SetBucketPageId(split_image_bucket_index, image_bunket_page_id);

  /**
   * 上面我们完成了新bucketpage的申请，并且，将镜像index指向新的页（ 比如splitpageindex = 110，那么镜像的那个就是100 ，
   * 对于000和010还没映射，反正最后应该110和010指向旧页，100和000指向新页
   * 也就是dir中的split_image_bucket_index存的是新的页id，现在老页和新页都没有数据）
   * 但是，我们的GetSplitImageIndex只是获得了截掉localdepth长度的相同的另一个，
   * 对于比如100和110，他们之前也指向这个分割的也，也就是之前的页的localdepth是1，
   * 现在分割了变成了2，那么000和100指向旧页，010和110指向新页
   * 我们在上面只完成了000和010index的设置，对于100和110，甚至1000，1010等都指向这个分割的页，
   * 那么我们现在要对这些index对应的pageid重新设置，要么是新页，要么是老页
   */
  // 下面我们就设置这些没有设置的index
  // ---------------------------------------
  uint32_t distance = 1 << dir->GetLocalDepth(split_bucket_index);
  // 先设置和split_bucket_index指向的同一个页面的index
  uint32_t i = split_bucket_index;
  while (i >= distance ) {
    if(i == 0-distance){
      break;
    }
    dir->SetLocalDepth(i, dir->GetLocalDepth(split_bucket_index));
    dir->SetBucketPageId(i, split_bucket_page_id);
    i -= distance;
  }
  i = split_bucket_index;
  while ( i < dir->Size()){
    dir->SetBucketPageId(i, split_bucket_page_id);
    dir->SetLocalDepth(i, dir->GetLocalDepth(split_bucket_index));
    i += distance;
  }
  // 再设置和split_image_bucket_index指向同一个页面的index
  i = split_image_bucket_index;
  while (i >= distance) {
    if(i == 0 - distance){
      break;
    }
    dir->SetLocalDepth(i, dir->GetLocalDepth(split_bucket_index));
    dir->SetBucketPageId(i, image_bunket_page_id);
    i -= distance;
  }
  i = split_image_bucket_index;
  while (i < dir->Size()) {
    dir->SetBucketPageId(i, split_bucket_page_id);
    dir->SetLocalDepth(i, dir->GetLocalDepth(split_image_bucket_index));
    i += distance;
  }
  // ---------------------------------
  // 现在我们完成了bucketpage的分割和所有原来指向同一个老页的都重新设置指向了新页和就页，
  // 现在就是将之前旧页中的元素，重新进行散列到
  // 我们这两个页面中
  uint32_t mask = dir->GetLocalDepthMask(split_bucket_index);
  for (uint32_t i = 0 ; i < origin_array_size; i++) {
    MappingType temp = origin_array[i];
    uint32_t target_bucket_index = Hash(temp.first) & mask;
    page_id_t target_bucket_page_id = dir->GetBucketPageId(target_bucket_index);
    if (target_bucket_page_id == split_bucket_page_id) {
      split_bucket->Insert(temp.first, temp.second, comparator_);
    } else {
      image_bucket->Insert(temp.first, temp.second, comparator_);
    }
  }

  // 最后一定要delete 在堆中申请的空间
  delete[] origin_array;
  split_page->WUnlatch();
  assert(buffer_pool_manager_->UnpinPage(dir->GetPageId(), true));
  assert(buffer_pool_manager_->UnpinPage(split_bucket_page_id, true));
  assert(buffer_pool_manager_->UnpinPage(image_bunket_page_id, true));
  table_latch_.WUnlock();
  return Insert(transaction, key, value);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.RLock();

  HashTableDirectoryPage *dir = FetchDirectoryPage();
  page_id_t bucket_page_id = KeyToPageId(key, dir);
  Page *page = FetchBucketPage(bucket_page_id);
  page->WLatch();
  HASH_TABLE_BUCKET_TYPE *bucket = RetrieveBucket(page);
  bool res = bucket->Remove(key, value, comparator_);
  // 如果bucket空了，那么就将他的image bucket page进行merge
  if (bucket->IsEmpty()) {
    page->WUnlatch();
    assert(buffer_pool_manager_->UnpinPage(bucket_page_id, true));
    assert(buffer_pool_manager_->UnpinPage(dir->GetPageId(), false));
    table_latch_.RUnlock();
    Merge(transaction, key, value);
    return res;
  }
  page->WUnlatch();
  assert(buffer_pool_manager_->UnpinPage(bucket_page_id, true));
  assert(buffer_pool_manager_->UnpinPage(dir->GetPageId(), false));
  table_latch_.RUnlock();
  return res;
}

/*****************************************************************************
 * MERGE 合并这个bucket page，什么时候合并呢，当一个bucketpage里面没有元素的时候，
 * 我们需要将这个page和他的imagepage合并，并且修改dir中的index对应的pageid，其实和split的逆过程很像
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Merge(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.WLock();
  HashTableDirectoryPage *dir = FetchDirectoryPage();
  uint32_t merge_bucket_index = KeyToDirectoryIndex(key, dir);
  if (merge_bucket_index >= dir->Size()) {
    table_latch_.WUnlock();
    return;
  }
  page_id_t merge_bucket_page_id = dir->GetBucketPageId(merge_bucket_index);
   // 就是将localdepth对应的位取反
  uint32_t image_bucket_index =  dir->GetSplitImageIndex(merge_bucket_index);
  uint32_t local_depth = dir->GetLocalDepth(merge_bucket_index);
  if (local_depth == 0) {
    // 说明无法merge了
    assert(buffer_pool_manager_->UnpinPage(dir->GetPageId(), false));
    table_latch_.WUnlock();
    return;
  }
  if (local_depth != dir->GetLocalDepth(image_bucket_index)) {
    assert(buffer_pool_manager_->UnpinPage(dir->GetPageId(), false));
    table_latch_.WUnlock();
    return;
  }
  Page* merge_bucket_page = FetchBucketPage(merge_bucket_page_id);
  merge_bucket_page->RLatch();
  HASH_TABLE_BUCKET_TYPE *bucket = RetrieveBucket(merge_bucket_page);
  if (!bucket->IsEmpty()) {
    merge_bucket_page->RUnlatch();
    assert(buffer_pool_manager_->UnpinPage(dir->GetPageId(), false));
    assert(buffer_pool_manager_->UnpinPage(merge_bucket_page_id, false));
    table_latch_.WUnlock();
    return;
  }
  merge_bucket_page->RUnlatch();
  assert(buffer_pool_manager_->UnpinPage(merge_bucket_page_id, false));
  assert(buffer_pool_manager_->DeletePage(merge_bucket_page_id));
  page_id_t image_bucket_page_id = dir->GetBucketPageId(image_bucket_index);
  dir->SetBucketPageId(merge_bucket_index, image_bucket_page_id);
  dir->DecrLocalDepth(merge_bucket_index);
  dir->DecrLocalDepth(image_bucket_index);


  uint32_t distance = 1 << (dir->GetLocalDepth(merge_bucket_index) +1);
  uint32_t i = merge_bucket_index;
  while (i >= distance) {
    if(i == 0-distance){
      break;
    }
    dir->SetBucketPageId(i, image_bucket_page_id);
    dir->DecrLocalDepth(i);
    i -= distance;
  }
  i = merge_bucket_index;
  while ( i < dir->Size()) {
    dir->SetBucketPageId(i, image_bucket_page_id);
    dir->DecrLocalDepth(i);
    i += distance;
  }

  // 这个用来判断是否这个dir可以收缩，所有的localdepth小于globaldepth
  if (dir->CanShrink()) {
    dir->DecrGlobalDepth();
  }
  assert(buffer_pool_manager_->UnpinPage(dir->GetPageId(), true));
  table_latch_.WUnlock();
  return;
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

template <typename KeyType, typename ValueType, typename KeyComparator>
Page *ExtendibleHashTable<KeyType, ValueType, KeyComparator>::AssertPage(Page *page) {
  assert(page != nullptr);
  return page;
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
