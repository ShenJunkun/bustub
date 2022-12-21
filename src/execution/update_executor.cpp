//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"
#include "common/logger.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),  child_executor_(std::move(child_executor)) {
  this->plan_ = plan;
}

void UpdateExecutor::Init() {
  auto talbeOid = this->plan_->TableOid();
  this->table_info_ = this->GetExecutorContext()->GetCatalog()->GetTable(talbeOid);
  child_executor_.get()->Init();
}

bool UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {

  bool res =child_executor_.get()->Next(tuple, rid);
  if (!res) {
    return false;
  }

  Tuple newTuple = this->GenerateUpdatedTuple(*tuple);

  bool updateRes = table_info_->table_.get()->UpdateTuple(newTuple, *rid, exec_ctx_->GetTransaction());
  if (!updateRes) {
    LOG_WARN("update executor更新失败");
  }

  //update indexes
  std::vector<IndexInfo *> indexes = this->GetExecutorContext()->GetCatalog()->GetTableIndexes(table_info_->name_);
  for (auto index : indexes) {
    index->index_.get()->DeleteEntry(*tuple, *rid, exec_ctx_->GetTransaction());
    index->index_.get()->InsertEntry(newTuple, *rid, exec_ctx_->GetTransaction());
  }

  
   return true; 
}

Tuple UpdateExecutor::GenerateUpdatedTuple(const Tuple &src_tuple) {
  const auto &update_attrs = plan_->GetUpdateAttr();
  Schema schema = table_info_->schema_;
  uint32_t col_count = schema.GetColumnCount();
  std::vector<Value> values;
  for (uint32_t idx = 0; idx < col_count; idx++) {
    if (update_attrs.find(idx) == update_attrs.cend()) {
      values.emplace_back(src_tuple.GetValue(&schema, idx));
    } else {
      const UpdateInfo info = update_attrs.at(idx);
      Value val = src_tuple.GetValue(&schema, idx);
      switch (info.type_) {
        case UpdateType::Add:
          values.emplace_back(val.Add(ValueFactory::GetIntegerValue(info.update_val_)));
          break;
        case UpdateType::Set:
          values.emplace_back(ValueFactory::GetIntegerValue(info.update_val_));
          break;
      }
    }
  }
  return Tuple{values, &schema};
}

}  // namespace bustub
