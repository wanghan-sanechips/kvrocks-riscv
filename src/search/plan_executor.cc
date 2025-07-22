/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

#include "plan_executor.h"

#include <memory>

#include "search/executors/filter_executor.h"
#include "search/executors/full_index_scan_executor.h"
#include "search/executors/hnsw_vector_field_knn_scan_executor.h"
#include "search/executors/hnsw_vector_field_range_scan_executor.h"
#include "search/executors/limit_executor.h"
#include "search/executors/merge_executor.h"
#include "search/executors/mock_executor.h"
#include "search/executors/noop_executor.h"
#include "search/executors/numeric_field_scan_executor.h"
#include "search/executors/projection_executor.h"
#include "search/executors/sort_executor.h"
#include "search/executors/tag_field_scan_executor.h"
#include "search/executors/topn_executor.h"
#include "search/indexer.h"
#include "search/ir_plan.h"

namespace kqir {

namespace details {

struct ExecutorContextVisitor {
  ExecutorContext *ctx;

  void Transform(PlanOperator *op) {
    if (auto v = dynamic_cast<Limit *>(op)) {
      return Visit(v);
    }

    if (auto v = dynamic_cast<Noop *>(op)) {
      return Visit(v);
    }

    if (auto v = dynamic_cast<Merge *>(op)) {
      return Visit(v);
    }

    if (auto v = dynamic_cast<Sort *>(op)) {
      return Visit(v);
    }

    if (auto v = dynamic_cast<Filter *>(op)) {
      return Visit(v);
    }

    if (auto v = dynamic_cast<Projection *>(op)) {
      return Visit(v);
    }

    if (auto v = dynamic_cast<TopN *>(op)) {
      return Visit(v);
    }

    if (auto v = dynamic_cast<FullIndexScan *>(op)) {
      return Visit(v);
    }

    if (auto v = dynamic_cast<NumericFieldScan *>(op)) {
      return Visit(v);
    }

    if (auto v = dynamic_cast<TagFieldScan *>(op)) {
      return Visit(v);
    }

    if (auto v = dynamic_cast<HnswVectorFieldKnnScan *>(op)) {
      return Visit(v);
    }

    if (auto v = dynamic_cast<HnswVectorFieldRangeScan *>(op)) {
      return Visit(v);
    }

    if (auto v = dynamic_cast<Mock *>(op)) {
      return Visit(v);
    }

    unreachable();
  }

  void Visit(Limit *op) {
    ctx->nodes[op] = std::make_unique<LimitExecutor>(ctx, op);
    Transform(op->op.get());
  }

  void Visit(Sort *op) {
    ctx->nodes[op] = std::make_unique<SortExecutor>(ctx, op);
    Transform(op->op.get());
  }

  void Visit(Noop *op) { ctx->nodes[op] = std::make_unique<NoopExecutor>(ctx, op); }

  void Visit(Merge *op) {
    ctx->nodes[op] = std::make_unique<MergeExecutor>(ctx, op);
    for (const auto &child : op->ops) Transform(child.get());
  }

  void Visit(Filter *op) {
    ctx->nodes[op] = std::make_unique<FilterExecutor>(ctx, op);
    Transform(op->source.get());
  }

  void Visit(Projection *op) {
    ctx->nodes[op] = std::make_unique<ProjectionExecutor>(ctx, op);
    Transform(op->source.get());
  }

  void Visit(TopN *op) {
    ctx->nodes[op] = std::make_unique<TopNExecutor>(ctx, op);
    Transform(op->op.get());
  }

  void Visit(FullIndexScan *op) { ctx->nodes[op] = std::make_unique<FullIndexScanExecutor>(ctx, op); }

  void Visit(NumericFieldScan *op) { ctx->nodes[op] = std::make_unique<NumericFieldScanExecutor>(ctx, op); }

  void Visit(TagFieldScan *op) { ctx->nodes[op] = std::make_unique<TagFieldScanExecutor>(ctx, op); }

  void Visit(HnswVectorFieldKnnScan *op) { ctx->nodes[op] = std::make_unique<HnswVectorFieldKnnScanExecutor>(ctx, op); }

  void Visit(HnswVectorFieldRangeScan *op) {
    ctx->nodes[op] = std::make_unique<HnswVectorFieldRangeScanExecutor>(ctx, op);
  }

  void Visit(Mock *op) { ctx->nodes[op] = std::make_unique<MockExecutor>(ctx, op); }
};

}  // namespace details

ExecutorContext::ExecutorContext(PlanOperator *op) : root(op), db_ctx(engine::Context::NoTransactionContext(nullptr)) {
  details::ExecutorContextVisitor visitor{this};
  visitor.Transform(root);
}

ExecutorContext::ExecutorContext(PlanOperator *op, engine::Storage *storage)
    : root(op), storage(storage), db_ctx(storage) {
  details::ExecutorContextVisitor visitor{this};
  visitor.Transform(root);
}

auto ExecutorContext::Retrieve(engine::Context &ctx, RowType &row, const FieldInfo *field) const
    -> StatusOr<ValueType> {  // NOLINT
  if (auto iter = row.fields.find(field); iter != row.fields.end()) {
    return iter->second;
  }

  auto retriever = GET_OR_RET(
      redis::FieldValueRetriever::Create(field->index->metadata.on_data_type, row.key, storage, field->index->ns));

  auto s = retriever.Retrieve(ctx, field->name, field->metadata.get());
  if (!s) return s;

  row.fields.emplace(field, *s);
  return *s;
}

}  // namespace kqir
