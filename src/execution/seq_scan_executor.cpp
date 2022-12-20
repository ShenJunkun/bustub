//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"
#include "include/storage/table/table_iterator.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan) : AbstractExecutor(exec_ctx), 
                    cursor_(nullptr, RID(INVALID_PAGE_ID, 0), nullptr),
                    end_(nullptr, RID(INVALID_PAGE_ID, 0), nullptr) {
  this->plan_ = plan;
}

void SeqScanExecutor::Init() {
  auto tableOid = plan_->GetTableOid();
  auto catalog = this->GetExecutorContext()->GetCatalog();
  auto tableInfo = catalog->GetTable(tableOid);
  auto tableHeap = tableInfo->table_.get();
  auto txn = this->GetExecutorContext()->GetTransaction();
  cursor_ = tableHeap->Begin(txn);
  end_ = tableHeap->End();
}

bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) {

  if (cursor_ == end_) {
    return false;
  }

  bool res = this->plan_->GetPredicate()->Evaluate(&(*cursor_), GetOutputSchema()).GetAs<bool>();
  

  while (!res)
  {
    cursor_++;
    if (cursor_ == end_) {
      break;
    }
    res = this->plan_->GetPredicate()->Evaluate(&(*cursor_), GetOutputSchema()).GetAs<bool>();
  }

  if (cursor_ != end_) {
    *tuple = *cursor_;
    *rid = (*cursor_).GetRid();
    cursor_++;
    return true;
  } 
  
  return false; 
}
    
}  // namespace bustub
