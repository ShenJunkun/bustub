//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), childExecutor_(std::move(child_executor)) {
  this->plan_ = plan;
  isRawInsert_ = this->plan_->IsRawInsert();
  rawIdx_ = 0;
  this->tableHeap_ = nullptr;
  this->tableInfo_ = nullptr;
}

void InsertExecutor::Init() {
  //1.获取插入的数据，数据来源包括两种情况
  //  1.1第一种情况是直接获得的数据
  //  1.2第二种情况是通过select获得的数据
  //所以，把insertExecutor看做成为一个root node

  //插入数据的时候，需要考虑索引。
  auto tableOid = this->plan_->TableOid();
  auto tableInfo = this->GetExecutorContext()->GetCatalog()->GetTable(tableOid);
  auto tableHeap = tableInfo->table_.get();
  tableInfo_ = tableInfo;
  tableHeap_ = tableHeap;

  // std::vector<IndexInfo *> index = this->GetExecutorContext()->GetCatalog()->GetTableIndexes(tableInfo->name_);
  
  if (isRawInsert_) {
    rawIdx_ = 0;
  } else {
    childExecutor_.get()->Init();
  }

}

bool InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  if (isRawInsert_) {
    if (rawIdx_ >= plan_->RawValues().size()) {
      return false;
    }
    *tuple = Tuple(plan_->RawValuesAt(rawIdx_++), &tableInfo_->schema_);
    tableHeap_->InsertTuple(*tuple, rid, exec_ctx_->GetTransaction());

    //update indexes
    std::vector<IndexInfo* > indexes = this->GetExecutorContext()->GetCatalog()->GetTableIndexes(tableInfo_->name_);
    for (auto index : indexes) {
      Tuple index_tuple = tuple->KeyFromTuple(tableInfo_->schema_, index->key_schema_, index->index_.get()->GetKeyAttrs());
      index->index_.get()->InsertEntry(index_tuple, *rid, exec_ctx_->GetTransaction());
    }

    return true;
  } else {
    bool res = childExecutor_.get()->Next(tuple, rid);
    if (!res) {
      return false;
    }
    tableHeap_->InsertTuple(*tuple, rid, exec_ctx_->GetTransaction());

    //update indexes
    std::vector<IndexInfo* > indexes = this->GetExecutorContext()->GetCatalog()->GetTableIndexes(tableInfo_->name_);
    for (auto index : indexes) {
      Tuple index_tuple = tuple->KeyFromTuple(*(plan_->GetChildPlan()->OutputSchema()) , index->key_schema_, index->index_.get()->GetKeyAttrs());
      index->index_.get()->InsertEntry(index_tuple, *rid, exec_ctx_->GetTransaction());
    }
    return true;
  }
  
}

}  // namespace bustub
