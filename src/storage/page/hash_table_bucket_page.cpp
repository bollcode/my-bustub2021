//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_table_bucket_page.cpp
//
// Identification: src/storage/page/hash_table_bucket_page.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/hash_table_bucket_page.h"
#include "common/logger.h"
#include "common/util/hash_util.h"
#include "storage/index/generic_key.h"
#include "storage/index/hash_comparator.h"
#include "storage/table/tmp_tuple.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::GetValue(KeyType key, KeyComparator cmp, std::vector<ValueType> *result) {
  bool res = false;
  for (size_t i = 0; i < BUCKET_ARRAY_SIZE; ++i) {
    if (IsReadable(i) && cmp(key, array_[i].first) == 0) {
      result->push_back(array_[i].second);
      res = true;
    }
  }
  return res;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::Insert(KeyType key, ValueType value, KeyComparator cmp) {
  int64_t free_slot = -1;
  for (size_t i = 0; i < BUCKET_ARRAY_SIZE; i++) {
    if (IsReadable(i)) {
      if (cmp(key, array_[i].first) == 0 && value == array_[i].second) {
        // already existed the same key & value
        //                LOG_DEBUG("Same kv");
        return false;
      }
    } else if (free_slot == -1) {
      free_slot = i;
    }
  }

  if (free_slot == -1) {
    // is full
    LOG_DEBUG("Bucket is full");
    return false;
  }

  // insert it and return true
  SetOccupied(free_slot);
  SetReadable(free_slot);
  array_[free_slot] = MappingType(key, value);
  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::Remove(KeyType key, ValueType value, KeyComparator cmp) {
  for (size_t i = 0; i < BUCKET_ARRAY_SIZE; i++) {
    if (IsReadable(i)) {
      if (cmp(key, array_[i].first) == 0 && value == array_[i].second) {
        // find it
        RemoveAt(i);
        return true;
      }
    }
  }
  return false;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
KeyType HASH_TABLE_BUCKET_TYPE::KeyAt(uint32_t bucket_idx) const {
  return array_[bucket_idx].first;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
ValueType HASH_TABLE_BUCKET_TYPE::ValueAt(uint32_t bucket_idx) const {
  return array_[bucket_idx].second;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::RemoveAt(uint32_t bucket_idx) {
  // 删除元素只是将是否可读的标志位给清零而已
  SetOccupied(bucket_idx, 1);
  SetReadable(bucket_idx, 0);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsOccupied(uint32_t bucket_idx) const {
  char c = occupied_[bucket_idx/8];  // 获取bucket_idx占居的字节
  uint32_t idx = bucket_idx%8;  // 在 c中的 偏移位置
  uint32_t mask = 1;
  mask = mask << idx;
  return (mask & c) > 0;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetOccupied(uint32_t bucket_idx) {
  SetOccupied(bucket_idx, true);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetOccupied(uint32_t bucket_idx, bool status) {
  char c = occupied_[bucket_idx/8];  // 获取bucket_idx占居的字节
  uint8_t ic = static_cast<uint8_t>(c);
  uint32_t idx = bucket_idx%8;  // 在 c中的 偏移位置
  if (status) {  // 这个是给指定的位设置为1，表示插入元素，被占用
    uint8_t mask = 1 << idx;
    ic = (ic | mask);
  } else {   // 这个是给指定的位置设置为0，表示删除元素
    uint8_t mask = 1;
    mask =  mask << idx;
    mask = ~mask;
    ic = (ic&mask);
  }
  occupied_[bucket_idx/8] = static_cast<char>(ic);
}



template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsReadable(uint32_t bucket_idx) const {
  char c = readable_[bucket_idx/8];  // 获取bucket_idx占居的字节
  uint32_t idx = bucket_idx%8;  // 在 c中的 偏移位置
  uint32_t mask = 1;
  mask = mask << idx;
  return (mask & c) > 0;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetReadable(uint32_t bucket_idx) {
  SetReadable(bucket_idx, true);
}


template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetReadable(uint32_t bucket_idx, bool status) {
  char c = readable_[bucket_idx/8];  // 获取bucket_idx占居的字节
  uint8_t ic = static_cast<uint8_t>(c);
  uint32_t idx = bucket_idx%8;  // 在 c中的 偏移位置
  if (status) {  // 这个是给指定的位设置为1，表示插入元素，被占用
    uint8_t mask = 1 << idx;
    ic = (ic | mask);
  } else {   // 这个是给指定的位置设置为0，表示删除元素
    uint8_t mask = 1;
    mask = mask << idx;
    mask = ~mask;
    ic = (ic&mask);
  }
  readable_[bucket_idx/8] = static_cast<char>(ic);
}

/**
 * 判断这个桶页是否满了
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsFull() {
  u_int8_t mask = 255;
  size_t times = BUCKET_ARRAY_SIZE / 8;
  for (size_t i = 0; i < times; i++) {
    char c = readable_[i];
    uint8_t ic = static_cast<uint8_t>(c);
    if ((ic & mask) != mask) {
      return false;
    }
  }

  size_t remain = BUCKET_ARRAY_SIZE % 8;
  if (remain > 0) {
    char c = readable_[times];
    uint8_t ic = static_cast<uint8_t>(c);
    for (size_t i = 0; i < remain; i++) {
      if ((ic & 1) != 1) {
        return false;
      }
      ic = ic >> 1;
    }
  }
  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_BUCKET_TYPE::NumReadable() {
  uint32_t num = 0;
  size_t times = BUCKET_ARRAY_SIZE / 8;
  for (size_t i = 0; i < times; i++) {
    char c = readable_[i];
    uint8_t ic = static_cast<uint8_t>(c);
    for (uint32_t j = 0; j < 8; j++) {
      if ((ic & 1) > 0) {
        num++;
      }
      ic = ic >> 1;
    }
  }

  size_t remain = BUCKET_ARRAY_SIZE % 8;
  if (remain > 0) {
    char c = readable_[times];
    uint8_t ic = static_cast<uint8_t>(c);
    for (size_t i = 0; i < remain; i++) {
      if ((ic & 1) == 1) {
        num++;
      }
      ic = ic >> 1;
    }
  }
  return num;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsEmpty() {
  uint8_t mask = 255;
  for (size_t i = 0; i < sizeof(readable_)/sizeof(readable_[0]); i++) {
    char c = readable_[i];
    if ((c & mask) > 0) {
      return false;
    }
  }
  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
MappingType *HASH_TABLE_BUCKET_TYPE::GetArrayCopy() {
  uint32_t num = NumReadable();
  MappingType *copy = new MappingType[num];
  for (uint32_t i = 0, index = 0; i < BUCKET_ARRAY_SIZE; i++) {
    if (IsReadable(i)) {
      copy[index++] = array_[i];
    }
  }
  return copy;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::PrintBucket() {
  uint32_t size = 0;
  uint32_t taken = 0;
  uint32_t free = 0;
  for (size_t bucket_idx = 0; bucket_idx < BUCKET_ARRAY_SIZE; bucket_idx++) {
    if (!IsOccupied(bucket_idx)) {
      break;
    }

    size++;

    if (IsReadable(bucket_idx)) {
      taken++;
    } else {
      free++;
    }
  }

  LOG_INFO("Bucket Capacity: %lu, Size: %u, Taken: %u, Free: %u", BUCKET_ARRAY_SIZE, size, taken, free);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::Clear() {
  LOG_DEBUG("clear");
  memset(occupied_, 0, sizeof(occupied_));
  memset(readable_, 0, sizeof(readable_));
  // memset(array_, 0, sizeof(array_));
}

// DO NOT REMOVE ANYTHING BELOW THIS LINE
template class HashTableBucketPage<int, int, IntComparator>;

template class HashTableBucketPage<GenericKey<4>, RID, GenericComparator<4>>;
template class HashTableBucketPage<GenericKey<8>, RID, GenericComparator<8>>;
template class HashTableBucketPage<GenericKey<16>, RID, GenericComparator<16>>;
template class HashTableBucketPage<GenericKey<32>, RID, GenericComparator<32>>;
template class HashTableBucketPage<GenericKey<64>, RID, GenericComparator<64>>;

// template class HashTableBucketPage<hash_t, TmpTuple, HashComparator>;

}  // namespace bustub
