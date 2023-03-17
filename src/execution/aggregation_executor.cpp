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
// 聚合就是groupby这些 然后结合sum avg
AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_(std::move(child)),
      aht_(SimpleAggregationHashTable(plan->GetAggregates(), plan->GetAggregateTypes())),
      aht_iterator_(aht_.Begin()) {}

void AggregationExecutor::Init() {
  child_->Init();
  Tuple tuple;
  RID rid;
  // 聚合只有一个child
  while (child_->Next(&tuple, &rid)) {
    // key是groupby对象的列，value是聚合的对象的值
    aht_.InsertCombine(MakeAggregateKey(&tuple), MakeAggregateValue(&tuple));
  }
  aht_iterator_ = aht_.Begin();
}

/**
 * 遍历完整张表以后，哈希表也就建立了。现在哈希表中key的数量由整张表中group指定的列中unique value的数量确定。
 * 而哈希表的value则是每个unique value对应的count，sum值。
 * 之后每次next只要使用哈希表的迭代器根据having（如果有的话）找到下一个符合要求的key/value键值对，构造出新的tuple返回就行。
 */
bool AggregationExecutor::Next(Tuple *tuple, RID *rid) {
  if (aht_iterator_ == aht_.End()) {
    return false;
  }
  const AggregateKey &agg_key = aht_iterator_.Key();
  const AggregateValue &agg_value = aht_iterator_.Val();
  ++aht_iterator_;

  // 判断Having条件，符合返回，不符合则继续查找
  if (plan_->GetHaving() == nullptr ||
      plan_->GetHaving()->EvaluateAggregate(agg_key.group_bys_, agg_value.aggregates_).GetAs<bool>()) {
    std::vector<Value> ret;
    for (const auto &col : plan_->OutputSchema()->GetColumns()) {
      ret.push_back(col.GetExpr()->EvaluateAggregate(agg_key.group_bys_, agg_value.aggregates_));
    }
    *tuple = Tuple(ret, plan_->OutputSchema());
    return true;
  }
  return Next(tuple, rid);
}

const AbstractExecutor *AggregationExecutor::GetChildExecutor() const { return child_.get(); }

}  // namespace bustub
