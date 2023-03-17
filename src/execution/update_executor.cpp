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

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
    plan_(plan),
    table_info_(exec_ctx->GetCatalog()->GetTable(plan->TableOid())),
    child_executor_(std::move(child_executor)) {}

void UpdateExecutor::Init() { child_executor_->Init(); }

bool UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  LockManager *lock_mgr = exec_ctx_->GetLockManager();
  Transaction *txn = exec_ctx_->GetTransaction();
  // 如果这个事务已经获取了这个事务的读锁，那么我们需要升级锁，如果没有，就上写锁，写锁上完是不能释放的
  while (child_executor_->Next(tuple, rid)) {
    if (txn->IsSharedLocked(*rid)) {
      if (!lock_mgr->LockUpgrade(txn, *rid)) {
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
      }
    } else {
      if (!lock_mgr->LockExclusive(txn, *rid)) {
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
      }
    }
    // 获取到更新的tuple，然后下面调用updateTuple更新tuple，并删除掉旧的索引，然后插入新的索引
    Tuple oldtuple = *tuple;
    *tuple = GenerateUpdatedTuple(*tuple);
    if (!table_info_->table_.get()->UpdateTuple(*tuple, *rid, exec_ctx_->GetTransaction())) {
      LOG_DEBUG("Update tuple failed");
      return false;
    }
    // 更新索引
    for (const auto &indexinfo : exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_)) {
      indexinfo->index_->DeleteEntry(
        (&oldtuple)->KeyFromTuple(table_info_->schema_, *indexinfo->index_->GetKeySchema(), indexinfo->index_->GetKeyAttrs()),
        *rid,
        exec_ctx_->GetTransaction());
      indexinfo->index_->InsertEntry(
          tuple->KeyFromTuple(table_info_->schema_, *indexinfo->index_->GetKeySchema(), indexinfo->index_->GetKeyAttrs()),
          *rid,
          exec_ctx_->GetTransaction());
      txn->GetIndexWriteSet()->emplace_back(*rid, table_info_->oid_, WType::UPDATE, *tuple, indexinfo->index_oid_,
                                          exec_ctx_->GetCatalog());
    }
    // 更新完之后不需要释放锁，读锁在最后事务提交的时候释放
  }
  return false;
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
