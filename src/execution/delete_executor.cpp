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
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      table_info_(exec_ctx->GetCatalog()->GetTable(plan->TableOid())),
      child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() { child_executor_->Init(); }

bool DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  LockManager *lock_mgr = GetExecutorContext()->GetLockManager();
  Transaction *txn = GetExecutorContext()->GetTransaction();
  // 和更新一样上锁
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
    // 删除是标记删除，并不是真正的删除，当后面的事务提交后才真正的删除
    if (!table_info_->table_.get()->MarkDelete(*rid, exec_ctx_->GetTransaction())) {
      LOG_DEBUG("Delete failed");
      return false;
    }

    // 删除索引
    for (const auto &index : exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_)) {
      index->index_->DeleteEntry(
          tuple->KeyFromTuple(table_info_->schema_, *index->index_->GetKeySchema(), index->index_->GetKeyAttrs()), *rid,
          exec_ctx_->GetTransaction());
      txn->GetIndexWriteSet()->emplace_back(
          IndexWriteRecord(*rid, table_info_->oid_, WType::DELETE, *tuple, index->index_oid_, exec_ctx_->GetCatalog()));
    }
  }
  return false;
}

}  // namespace bustub
