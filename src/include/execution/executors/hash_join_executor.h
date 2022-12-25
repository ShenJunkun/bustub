//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.h
//
// Identification: src/include/execution/executors/hash_join_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <utility>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/hash_join_plan.h"
#include "storage/table/tuple.h"

#include "common/util/hash_util.h"

namespace bustub {
  struct HashjoinKey {
  /** The group-by values */

  std::vector<Value> group_bys_;

  /**
   * Compares two hashjoin keys for equality.
   * @param other the other hashjoin key to be compared with
   * @return `true` if both hashjoin keys have equivalent group-by expressions, `false` otherwise
   */
  bool operator==(const HashjoinKey &other) const {
    for (uint32_t i = 0; i < other.group_bys_.size(); i++) {
      if (group_bys_[i].CompareEquals(other.group_bys_[i]) != CmpBool::CmpTrue) {
        return false;
      }
    }
    return true;
  }

};

struct HashjoinValue {
  std::vector<Tuple> tuples;
};

} // namespace bustub


namespace std {

/** Implements std::hash on AggregateKey */
template <>
struct hash<bustub::HashjoinKey> {
  std::size_t operator()(const bustub::HashjoinKey &agg_key) const {
    size_t curr_hash = 0;
    for (const auto &key : agg_key.group_bys_) {
      if (!key.IsNull()) {
        curr_hash = bustub::HashUtil::CombineHashes(curr_hash, bustub::HashUtil::HashValue(&key));
      }
    }
    return curr_hash;
  }
};

}  // namespace std

namespace bustub {

/**
 * HashJoinExecutor executes a nested-loop JOIN on two tables.
 */
class HashJoinExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new HashJoinExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The HashJoin join plan to be executed
   * @param left_child The child executor that produces tuples for the left side of join
   * @param right_child The child executor that produces tuples for the right side of join
   */
  HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&left_child, std::unique_ptr<AbstractExecutor> &&right_child);

  /** Initialize the join */
  void Init() override;

  /**
   * Yield the next tuple from the join.
   * @param[out] tuple The next tuple produced by the join
   * @param[out] rid The next tuple RID produced by the join
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  bool Next(Tuple *tuple, RID *rid) override;

  /** @return The output schema for the join */
  const Schema *GetOutputSchema() override { return plan_->OutputSchema(); };

  void GetoutputTuple(Tuple* tuple, RID *rid);

 private:
  /** The NestedLoopJoin plan node to be executed. */
  const HashJoinPlanNode *plan_;

  std::unique_ptr<AbstractExecutor> left_child_;
  std::unique_ptr<AbstractExecutor> right_child_;
  std::unordered_map<HashjoinKey, HashjoinValue> ht_{};
  Tuple right_tuple_;
  RID right_rid_;
  bool find_already_;
  std::vector<Tuple>::iterator iter_;
  std::vector<Tuple>::iterator end_;

};

}  // namespace bustub

