//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx), plan_(plan), child_(std::move(child)), aht_(plan_->GetAggregates(), plan_->GetAggregateTypes()),
        aht_iterator_(aht_.Begin()) {}

void AggregationExecutor::Init() {
  child_.get()->Init();
  Tuple tuple;
  RID rid;
  //遍历
  while (child_.get()->Next(&tuple, &rid)) {
    AggregateKey aggregateKey = this->MakeAggregateKey(&tuple);
    AggregateValue aggregateValue = this->MakeAggregateValue(&tuple);
    aht_.InsertCombine(aggregateKey, aggregateValue); 
  }
  aht_iterator_ = aht_.Begin();
}

bool AggregationExecutor::Next(Tuple *tuple, RID *rid) {
  if (aht_iterator_ == aht_.End()) {
    return false;
  }
  if (plan_->GetHaving() != nullptr) {
    while (!plan_->GetHaving()->EvaluateAggregate(aht_iterator_.Key().group_bys_, 
        aht_iterator_.Val().aggregates_).GetAs<bool>()) {
      ++aht_iterator_;
      if (aht_iterator_ == aht_.End()) {
        return false;
      }
    }
    //找到第一个满足条件的
    //TODO输出元组
    std::vector<Value> res;
    for (auto column : plan_->OutputSchema()->GetColumns()) {
      auto val = column.GetExpr()->EvaluateAggregate(aht_iterator_.Key().group_bys_, 
        aht_iterator_.Val().aggregates_);
      res.push_back(val);
    }
    *tuple = Tuple(res, plan_->OutputSchema());
    ++aht_iterator_;
    return true;
  } else {
    //输出元组
    std::vector<Value> res;
    for (auto column : plan_->OutputSchema()->GetColumns()) {
      auto val = column.GetExpr()->EvaluateAggregate(aht_iterator_.Key().group_bys_, 
        aht_iterator_.Val().aggregates_);
      res.push_back(val);
    }
    *tuple = Tuple(res, plan_->OutputSchema());
    ++aht_iterator_;
    return true;
  }
  return false; 
}

const AbstractExecutor *AggregationExecutor::GetChildExecutor() const { 
  return child_.get(); 
}

}  // namespace bustub
