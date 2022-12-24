//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), child_executor_(std::move(child_executor)) {
  this->plan_ = plan;
  
}

void DeleteExecutor::Init() {
  auto tableOid = this->plan_->TableOid();
  table_info_ = this->GetExecutorContext()->GetCatalog()->GetTable(tableOid);
  child_executor_.get()->Init();
}

bool DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) { 

  bool res = child_executor_.get()->Next(tuple, rid);
  if (!res) {
    return false;
  }

  bool deleteRes = table_info_->table_.get()->MarkDelete(*rid, exec_ctx_->GetTransaction());
  if (!deleteRes) {
    LOG_WARN("delete executor删除失败");
  }

  //delete indexes
  // std::vector<IndexInfo *> indexes = this->GetExecutorContext()->GetCatalog()->GetTableIndexes(table_info_->name_);
  // for (auto index : indexes) {
  //   // index->index_.get()->DeleteEntry(*tuple, *rid, exec_ctx_->GetTransaction());
  // }

  return true; 
}

}  // namespace bustub
