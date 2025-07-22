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

#include <string>

#include "db_util.h"
#include "encoding.h"
#include "search/plan_executor.h"
#include "search/search_encoding.h"
#include "search/value.h"
#include "storage/redis_db.h"
#include "storage/redis_metadata.h"
#include "storage/storage.h"

namespace kqir {

struct NumericFieldScanExecutor : ExecutorNode {
  NumericFieldScan *scan;
  util::UniqueIterator iter{nullptr};

  IndexInfo *index;
  redis::SearchKey search_key;

  NumericFieldScanExecutor(ExecutorContext *ctx, NumericFieldScan *scan)
      : ExecutorNode(ctx),
        scan(scan),
        index(scan->field->info->index),
        search_key(index->ns, index->name, scan->field->name) {}

  std::string IndexKey(double num) const { return search_key.ConstructNumericFieldData(num, {}); }

  bool InRangeDecode(Slice key, double *curr, Slice *user_key) const {
    uint8_t ns_size = 0;
    if (!GetFixed8(&key, &ns_size)) return false;
    if (ns_size != index->ns.size()) return false;
    if (!key.starts_with(index->ns)) return false;
    key.remove_prefix(ns_size);

    uint8_t subkey_type = 0;
    if (!GetFixed8(&key, &subkey_type)) return false;
    if (subkey_type != (uint8_t)redis::SearchSubkeyType::FIELD) return false;

    Slice value;
    if (!GetSizedString(&key, &value)) return false;
    if (value != index->name) return false;

    if (!GetSizedString(&key, &value)) return false;
    if (value != scan->field->name) return false;

    if (!GetDouble(&key, curr)) return false;

    if (!GetSizedString(&key, user_key)) return false;

    return true;
  }

  StatusOr<Result> Next() override {
    if (!iter) {
      iter = util::UniqueIterator(ctx->db_ctx, ctx->db_ctx.DefaultScanOptions(),
                                  ctx->storage->GetCFHandle(ColumnFamilyID::Search));
      if (scan->order == SortByClause::ASC) {
        iter->Seek(IndexKey(scan->range.l));
      } else {
        iter->SeekForPrev(IndexKey(IntervalSet::PrevNum(scan->range.r)));
      }
    }

    if (!iter->Valid()) {
      return end;
    }

    double curr = 0;
    Slice user_key;
    if (!InRangeDecode(iter->key(), &curr, &user_key)) {
      return end;
    }

    if (scan->order == SortByClause::ASC ? curr >= scan->range.r : curr < scan->range.l) {
      return end;
    }

    auto key_str = user_key.ToString();

    if (scan->order == SortByClause::ASC) {
      iter->Next();
    } else {
      iter->Prev();
    }
    return RowType{key_str, {{scan->field->info, kqir::MakeValue<kqir::Numeric>(curr)}}, scan->field->info->index};
  }
};

}  // namespace kqir
