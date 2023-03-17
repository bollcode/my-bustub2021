//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_table_header_page.cpp
//
// Identification: src/storage/page/hash_table_header_page.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/hash_table_directory_page.h"
#include <algorithm>
#include <unordered_map>
#include "common/logger.h"
#include "storage/page/hash_table_bucket_page.h"
namespace bustub {
page_id_t HashTableDirectoryPage::GetPageId() const { return page_id_; }

void HashTableDirectoryPage::SetPageId(bustub::page_id_t page_id) { page_id_ = page_id; }

lsn_t HashTableDirectoryPage::GetLSN() const { return lsn_; }

void HashTableDirectoryPage::SetLSN(lsn_t lsn) { lsn_ = lsn; }

uint32_t HashTableDirectoryPage::GetGlobalDepth() { return global_depth_; }

/**
 * 获取GlobalDepth的掩码，就是GlobalDepth个1
 * 1->1   2->11  3-> 111
 */
uint32_t HashTableDirectoryPage::GetGlobalDepthMask() {
  return (1 << global_depth_) -1;
}

uint32_t HashTableDirectoryPage::GetLocalDepthMask(uint32_t bucket_idx) {
  uint8_t localdepth = local_depths_[bucket_idx];
  return (1<< localdepth) -1;
}
/**
 * 给directory 的global depth 加1，也就是grow，将原来的桶的索引的个数增加一倍
 * 增加一倍后，然后将之前的数都复制到新拓展的索引中
 */
void HashTableDirectoryPage::IncrGlobalDepth() {
  // assert(global_depth_ < MAX_BUCKET_DEPTH);
  int new_start_depth = 1 << global_depth_;
  int old = new_start_depth;

  for (int i = 0; i < old; i++, new_start_depth++) {
    bucket_page_ids_[new_start_depth] = bucket_page_ids_[i];
    // 这个是将directory 扩容后加入，将新拓展的地方依然指向同一个bucket；例如00 和10指向同一个
    // 将桶信息复制到新拓展的地方
    local_depths_[new_start_depth] = local_depths_[i];
  }
  global_depth_++;
  return;
}

void HashTableDirectoryPage::DecrGlobalDepth() { global_depth_--; }

page_id_t HashTableDirectoryPage::GetBucketPageId(uint32_t bucket_idx) {
  return bucket_page_ids_[bucket_idx];
}

void HashTableDirectoryPage::SetBucketPageId(uint32_t bucket_idx, page_id_t bucket_page_id) {
  bucket_page_ids_[bucket_idx] = bucket_page_id;
}

uint32_t HashTableDirectoryPage::Size() {
  return 1 << global_depth_;
}

/**
 * 只有当所有的localdepth都小于 globaldepth时，才可以缩容
 */
bool HashTableDirectoryPage::CanShrink() {
  for (uint32_t i = 0; i < Size(); i++) {
    uint32_t localdepth = local_depths_[i];
    if (localdepth == global_depth_) {
      return false;
    }
  }
  return true;
}

uint32_t HashTableDirectoryPage::GetLocalDepth(uint32_t bucket_idx) {
  // assert(local_depth <= global_depth_);
  return local_depths_[bucket_idx];
}

void HashTableDirectoryPage::SetLocalDepth(uint32_t bucket_idx, uint8_t local_depth) {
  local_depths_[bucket_idx] = local_depth;
}

void HashTableDirectoryPage::IncrLocalDepth(uint32_t bucket_idx) {
  // uint32_t localdepth = local_depths_[bucket_idx];
  // if(localdepth == global_depth_){
  //   IncrGlobalDepth();
  // }
  // local_depths_[bucket_idx]++;
  local_depths_[bucket_idx]++;
  assert(local_depths_[bucket_idx] <= global_depth_);
}

void HashTableDirectoryPage::DecrLocalDepth(uint32_t bucket_idx) {
  local_depths_[bucket_idx]--;
}
/**
 * 他的作用是获取兄弟bucket的bucket_idx(也就是所谓的splitImage), 也就是说,
 * 我们要将传入的bucket_idx的local_depth的最高位取反后返回
 * 在extendible hash index中，当插入导致bucket分裂或者移除导致bucket合并时，
 * 我们都要找到待分离或合并的bucket的另一半。
 * https://www.cnblogs.com/huasyuan/p/16611858.html
 */
uint32_t HashTableDirectoryPage::GetSplitImageIndex(uint32_t bucket_idx) {
  // 合并时找另一半
  uint32_t local_depth = local_depths_[bucket_idx];
  return bucket_idx ^ (1 << (local_depth - 1));
}

/**
 * VerifyIntegrity - Use this for debugging but **DO NOT CHANGE**
 *
 * If you want to make changes to this, make a new function and extend it.
 *
 * Verify the following invariants:
 * (1) All LD <= GD.
 * (2) Each bucket has precisely 2^(GD - LD) pointers pointing to it.
 * (3) The LD is the same at each index with the same bucket_page_id
 */
void HashTableDirectoryPage::VerifyIntegrity() {
  //  build maps of {bucket_page_id : pointer_count} and {bucket_page_id : local_depth}
  std::unordered_map<page_id_t, uint32_t> page_id_to_count = std::unordered_map<page_id_t, uint32_t>();
  std::unordered_map<page_id_t, uint32_t> page_id_to_ld = std::unordered_map<page_id_t, uint32_t>();

  //  verify for each bucket_page_id, pointer
  for (uint32_t curr_idx = 0; curr_idx < Size(); curr_idx++) {
    page_id_t curr_page_id = bucket_page_ids_[curr_idx];
    uint32_t curr_ld = local_depths_[curr_idx];
    assert(curr_ld <= global_depth_);

    ++page_id_to_count[curr_page_id];

    if (page_id_to_ld.count(curr_page_id) > 0 && curr_ld != page_id_to_ld[curr_page_id]) {
      uint32_t old_ld = page_id_to_ld[curr_page_id];
      LOG_WARN("Verify Integrity: curr_local_depth: %u, old_local_depth %u, for page_id: %u", curr_ld, old_ld,
               curr_page_id);
      PrintDirectory();
      assert(curr_ld == page_id_to_ld[curr_page_id]);
    } else {
      page_id_to_ld[curr_page_id] = curr_ld;
    }
  }

  auto it = page_id_to_count.begin();

  while (it != page_id_to_count.end()) {
    page_id_t curr_page_id = it->first;
    uint32_t curr_count = it->second;
    uint32_t curr_ld = page_id_to_ld[curr_page_id];
    uint32_t required_count = 0x1 << (global_depth_ - curr_ld);

    if (curr_count != required_count) {
      LOG_WARN("Verify Integrity: curr_count: %u, required_count %u, for page_id: %u", curr_ld, required_count,
               curr_page_id);
      PrintDirectory();
      assert(curr_count == required_count);
    }
    it++;
  }
}

void HashTableDirectoryPage::PrintDirectory() {
  LOG_DEBUG("======== DIRECTORY (global_depth_: %u) ========", global_depth_);
  LOG_DEBUG("| bucket_idx | page_id | local_depth |");
  for (uint32_t idx = 0; idx < static_cast<uint32_t>(0x1 << global_depth_); idx++) {
    LOG_DEBUG("|      %u     |     %u     |     %u     |", idx, bucket_page_ids_[idx], local_depths_[idx]);
  }
  LOG_DEBUG("================ END DIRECTORY ================");
}

}  // namespace bustub
