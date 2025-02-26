//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// limit_executor.cpp
//
// Identification: src/execution/limit_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/limit_executor.h"

namespace bustub {

LimitExecutor::LimitExecutor(ExecutorContext *exec_ctx, const LimitPlanNode *plan,
                             std::unique_ptr<AbstractExecutor> &&child_executor)
  : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)), count_(0) {}

void LimitExecutor::Init() {
  child_executor_.get()->Init();
}

bool LimitExecutor::Next(Tuple *tuple, RID *rid) {
  Tuple child_tuple;
  RID child_rid;
  if (count_ >= plan_->GetLimit()) {
    return false;
  }

  auto res = child_executor_.get()->Next(&child_tuple, &child_rid);
  if (!res) {
    return false;
  }
  *tuple = child_tuple;
  count_++;
  return true; 
}

}  // namespace bustub
