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

#pragma once

#include <variant>

#include "parse_util.h"
#include "search/hnsw_indexer.h"
#include "search/ir.h"
#include "search/plan_executor.h"
#include "search/search_encoding.h"
#include "string_util.h"

namespace kqir {

struct QueryExprEvaluator {
  ExecutorContext *ctx;
  ExecutorNode::RowType &row;

  StatusOr<bool> Transform(QueryExpr *e) const {
    if (auto v = dynamic_cast<AndExpr *>(e)) {
      return Visit(v);
    }
    if (auto v = dynamic_cast<OrExpr *>(e)) {
      return Visit(v);
    }
    if (auto v = dynamic_cast<NotExpr *>(e)) {
      return Visit(v);
    }
    if (auto v = dynamic_cast<VectorRangeExpr *>(e)) {
      return Visit(v);
    }
    if (auto v = dynamic_cast<NumericCompareExpr *>(e)) {
      return Visit(v);
    }
    if (auto v = dynamic_cast<TagContainExpr *>(e)) {
      return Visit(v);
    }

    unreachable();
  }

  StatusOr<bool> Visit(AndExpr *v) const {
    for (const auto &n : v->inners) {
      if (!GET_OR_RET(Transform(n.get()))) return false;
    }

    return true;
  }

  StatusOr<bool> Visit(OrExpr *v) const {
    for (const auto &n : v->inners) {
      if (GET_OR_RET(Transform(n.get()))) return true;
    }

    return false;
  }

  StatusOr<bool> Visit(NotExpr *v) const { return !GET_OR_RET(Transform(v->inner.get())); }

  StatusOr<bool> Visit(TagContainExpr *v) const {
    auto val = GET_OR_RET(ctx->Retrieve(ctx->db_ctx, row, v->field->info));

    CHECK(val.Is<kqir::StringArray>());
    auto tags = val.Get<kqir::StringArray>();

    auto meta = v->field->info->MetadataAs<redis::TagFieldMetadata>();
    if (meta->case_sensitive) {
      return std::find(tags.begin(), tags.end(), v->tag->val) != tags.end();
    } else {
      return std::find_if(tags.begin(), tags.end(),
                          [v](const auto &tag) { return util::EqualICase(tag, v->tag->val); }) != tags.end();
    }
  }

  StatusOr<bool> Visit(NumericCompareExpr *v) const {
    auto l_val = GET_OR_RET(ctx->Retrieve(ctx->db_ctx, row, v->field->info));

    CHECK(l_val.Is<kqir::Numeric>());
    auto l = l_val.Get<kqir::Numeric>();
    auto r = v->num->val;

    switch (v->op) {
      case NumericCompareExpr::EQ:
        return l == r;
      case NumericCompareExpr::NE:
        return l != r;
      case NumericCompareExpr::LT:
        return l < r;
      case NumericCompareExpr::LET:
        return l <= r;
      case NumericCompareExpr::GT:
        return l > r;
      case NumericCompareExpr::GET:
        return l >= r;
      default:
        unreachable();
    }
  }

  StatusOr<bool> Visit(VectorRangeExpr *v) const {
    auto val = GET_OR_RET(ctx->Retrieve(ctx->db_ctx, row, v->field->info));

    CHECK(val.Is<kqir::NumericArray>());
    auto l_values = val.Get<kqir::NumericArray>();
    auto r_values = v->vector->values;
    auto meta = v->field->info->MetadataAs<redis::HnswVectorFieldMetadata>();

    redis::VectorItem left, right;
    GET_OR_RET(redis::VectorItem::Create({}, l_values, meta, &left));
    GET_OR_RET(redis::VectorItem::Create({}, r_values, meta, &right));

    auto dist = GET_OR_RET(redis::ComputeSimilarity(left, right));
    auto effective_range = v->range->val * (1 + meta->epsilon);

    return (dist >= -abs(effective_range) && dist <= abs(effective_range));
  }
};

struct FilterExecutor : ExecutorNode {
  Filter *filter;

  FilterExecutor(ExecutorContext *ctx, Filter *filter) : ExecutorNode(ctx), filter(filter) {}

  StatusOr<Result> Next() override {
    while (true) {
      auto v = GET_OR_RET(ctx->Get(filter->source)->Next());

      if (std::holds_alternative<End>(v)) return end;

      QueryExprEvaluator eval{ctx, std::get<RowType>(v)};

      bool res = GET_OR_RET(eval.Transform(filter->filter_expr.get()));

      if (res) {
        return v;
      }
    }
  }
};

}  // namespace kqir
