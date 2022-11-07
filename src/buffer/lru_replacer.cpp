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

LRUReplacer::LRUReplacer(size_t num_pages) {
    this->max_pages_num = num_pages;
}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) { 
    std::lock_guard<std::mutex> lk(mtx);
    if (!lru_list.size()) {
        return false;
    }
    
    *frame_id = lru_list.front();
    lru_list.pop_front();
    return true;
}


void LRUReplacer::Pin(frame_id_t frame_id) {
    std::lock_guard<std::mutex> lk(mtx);
    std::list<frame_id_t>::iterator it = lru_list.begin();
    while (it != lru_list.end()) {
        if (*it == frame_id) {
            lru_list.erase(it);
            break;
        }
        it++;
    }
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
    std::lock_guard<std::mutex> lk(mtx);
    for(auto it = lru_list.begin(); it != lru_list.end(); it++) {
        if (*it == frame_id)
            return;
    }
    
    lru_list.push_back(frame_id);
}

size_t LRUReplacer::Size() {
    std::lock_guard<std::mutex> lk(mtx);
    return lru_list.size(); 
}

}  // namespace bustub
