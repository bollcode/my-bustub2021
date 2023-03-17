//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) {}

LRUReplacer::~LRUReplacer() = default;
/**
 * 这个函数用来牺牲一个帧,将这个frame_id对应的节点抹去，并将这个抹去的frame_id防止 *frame_id指针中，这样就获取了抹去的frame_id
 */
bool LRUReplacer::Victim(frame_id_t *frame_id) {
    std::lock_guard<std::mutex> guard(lru_latch_);
    if (lru_map_.empty()) {
        // 说明这个链表中没有可以牺牲的帧
        *frame_id = -1;
        return false;
    }
    *frame_id = lru_list_.front();  // 返回这个链表的最前面一个，最前面的一个就是要被淘汰的
    lru_map_.erase(*frame_id);  // 淘汰了那个节点后，要在map中也将其抹去
    lru_list_.pop_front();  // 将最前面的节点抹去
    return true;
}

/**
 * 意味着被pin的页正在cpu使用中，不能被LRU算法选中；将一个帧从牺牲候选队列中移除。
 */ 
void LRUReplacer::Pin(frame_id_t frame_id) {
    std::lock_guard<std::mutex> guard(lru_latch_);
    auto iter = lru_map_.find(frame_id);  // 找到这个frame_id对应的在list链表中的节点
    if (iter == lru_map_.end()) {
        // 表示没有在map中找到对应的元素
        return;
    }
    lru_list_.erase(iter->second);
    lru_map_.erase(iter);
}
/**
 *  这个函数用来将cpu刚使用完成的页对应的frameid加入到lru中
 */
void LRUReplacer::Unpin(frame_id_t frame_id) {
    std::lock_guard<std::mutex> guard(lru_latch_);
    if (lru_map_.find(frame_id) != lru_map_.end()) {
        // 说明这个已经在lru缓存中了
        return;
    }
    lru_list_.push_back(frame_id);
    auto p = lru_list_.end();
    p--;  // 找到双向链表中的最后一个节点，也就是刚才加的
    lru_map_[frame_id] = p;  // 将节点和frame_id的关系加入到map中
}

size_t LRUReplacer::Size() {
    std::lock_guard<std::mutex> guard(lru_latch_);
    return lru_list_.size();
}

}  // namespace bustub
