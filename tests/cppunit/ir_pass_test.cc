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

#include "search/ir_pass.h"

#include "fmt/core.h"
#include "gtest/gtest.h"
#include "search/interval.h"
#include "search/ir_sema_checker.h"
#include "search/passes/interval_analysis.h"
#include "search/passes/lower_to_plan.h"
#include "search/passes/manager.h"
#include "search/passes/push_down_not_expr.h"
#include "search/passes/simplify_and_or_expr.h"
#include "search/passes/simplify_boolean.h"
#include "search/sql_transformer.h"

using namespace kqir;

static auto Parse(const std::string& in) { return sql::ParseToIR(peg::string_input(in, "test")); }

TEST(IRPassTest, Simple) {
  auto ir = *Parse("select a from b where not c = 1 or d hastag \"x\" and 2 <= e order by e asc limit 0, 10");

  auto original = ir->Dump();

  struct MyVisitor : Visitor {
    std::string_view Name() override { return ""; };
  } visitor;
  auto ir2 = visitor.Transform(std::move(ir));
  ASSERT_EQ(original, ir2->Dump());
}

TEST(IRPassTest, SimplifyBoolean) {
  SimplifyBoolean sb;
  ASSERT_EQ(sb.Transform(*Parse("select a from b where not false"))->Dump(), "select a from b where true");
  ASSERT_EQ(sb.Transform(*Parse("select a from b where not not false"))->Dump(), "select a from b where false");
  ASSERT_EQ(sb.Transform(*Parse("select a from b where true and true"))->Dump(), "select a from b where true");
  ASSERT_EQ(sb.Transform(*Parse("select a from b where true and false"))->Dump(), "select a from b where false");
  ASSERT_EQ(sb.Transform(*Parse("select a from b where false and true"))->Dump(), "select a from b where false");
  ASSERT_EQ(sb.Transform(*Parse("select a from b where true and false and true"))->Dump(),
            "select a from b where false");
  ASSERT_EQ(sb.Transform(*Parse("select a from b where true and true and true"))->Dump(), "select a from b where true");
  ASSERT_EQ(sb.Transform(*Parse("select a from b where x > 1 and false"))->Dump(), "select a from b where false");
  ASSERT_EQ(sb.Transform(*Parse("select a from b where x > 1 and true"))->Dump(), "select a from b where x > 1");
  ASSERT_EQ(sb.Transform(*Parse("select a from b where x > 1 and true and y < 10"))->Dump(),
            "select a from b where (and x > 1, y < 10)");
  ASSERT_EQ(sb.Transform(*Parse("select a from b where not (false and (not true))"))->Dump(),
            "select a from b where true");
  ASSERT_EQ(sb.Transform(*Parse("select a from b where true or true"))->Dump(), "select a from b where true");
  ASSERT_EQ(sb.Transform(*Parse("select a from b where true or false"))->Dump(), "select a from b where true");
  ASSERT_EQ(sb.Transform(*Parse("select a from b where false or true"))->Dump(), "select a from b where true");
  ASSERT_EQ(sb.Transform(*Parse("select a from b where true or false or true"))->Dump(), "select a from b where true");
  ASSERT_EQ(sb.Transform(*Parse("select a from b where true or false or true"))->Dump(), "select a from b where true");
  ASSERT_EQ(sb.Transform(*Parse("select a from b where not ((x < 1 or true) and (y > 2 and true))"))->Dump(),
            "select a from b where not y > 2");
}

TEST(IRPassTest, SimplifyAndOrExpr) {
  SimplifyAndOrExpr saoe;

  ASSERT_EQ(Parse("select a from b where true and (false and true)").GetValue()->Dump(),
            "select a from b where (and true, (and false, true))");
  ASSERT_EQ(saoe.Transform(*Parse("select a from b where true and (false and true)"))->Dump(),
            "select a from b where (and true, false, true)");
  ASSERT_EQ(saoe.Transform(*Parse("select a from b where true or (false or true)"))->Dump(),
            "select a from b where (or true, false, true)");
  ASSERT_EQ(saoe.Transform(*Parse("select a from b where true and (false or true)"))->Dump(),
            "select a from b where (and true, (or false, true))");
  ASSERT_EQ(saoe.Transform(*Parse("select a from b where true or (false and true)"))->Dump(),
            "select a from b where (or true, (and false, true))");
  ASSERT_EQ(saoe.Transform(*Parse("select a from b where x > 1 or (y < 2 or z = 3)"))->Dump(),
            "select a from b where (or x > 1, y < 2, z = 3)");
}

TEST(IRPassTest, PushDownNotExpr) {
  PushDownNotExpr pdne;

  ASSERT_EQ(pdne.Transform(*Parse("select * from a where not a > 1"))->Dump(), "select * from a where a <= 1");
  ASSERT_EQ(pdne.Transform(*Parse("select * from a where not a hastag \"\""))->Dump(),
            "select * from a where not a hastag \"\"");
  ASSERT_EQ(pdne.Transform(*Parse("select * from a where not not a > 1"))->Dump(), "select * from a where a > 1");
  ASSERT_EQ(pdne.Transform(*Parse("select * from a where not (a > 1 and b <= 3)"))->Dump(),
            "select * from a where (or a <= 1, b > 3)");
  ASSERT_EQ(pdne.Transform(*Parse("select * from a where not (a > 1 or b <= 3)"))->Dump(),
            "select * from a where (and a <= 1, b > 3)");
  ASSERT_EQ(pdne.Transform(*Parse("select * from a where not (not a > 1 or (b < 3 and c hastag \"\"))"))->Dump(),
            "select * from a where (and a > 1, (or b >= 3, not c hastag \"\"))");
}

TEST(IRPassTest, Manager) {
  auto expr_passes = PassManager::ExprPasses();
  ASSERT_EQ(PassManager::Execute(expr_passes,
                                 *Parse("select * from a where not (x > 1 or (y < 2 or z = 3)) and (true or x = 1)"))
                ->Dump(),
            "select * from a where (and x <= 1, y >= 2, z != 3)");
}

TEST(IRPassTest, SortByWithLimitToKnnExpr) {
  SortByWithLimitToKnnExpr tsbtke;

  ASSERT_EQ(tsbtke.Transform(*Parse("select a from b order by embedding <-> [3.6] limit 5"))->Dump(),
            "select a from b where KNN k=5, embedding <-> [3.600000] limit 0, 5");
  ASSERT_EQ(tsbtke.Transform(*Parse("select a from b where false order by embedding <-> [3,1,2] limit 5"))->Dump(),
            "select a from b where false sortby embedding <-> [3.000000, 1.000000, 2.000000] limit 0, 5");
  ASSERT_EQ(tsbtke.Transform(*Parse("select a from b where true order by embedding <-> [3,1,2] limit 5"))->Dump(),
            "select a from b where KNN k=5, embedding <-> [3.000000, 1.000000, 2.000000] limit 0, 5");
  ASSERT_EQ(tsbtke.Transform(*Parse("select a from b where true order by embedding <-> [3,1,2] limit 3, 5"))->Dump(),
            "select a from b where KNN k=8, embedding <-> [3.000000, 1.000000, 2.000000] limit 3, 5");
}

TEST(IRPassTest, LowerToPlan) {
  LowerToPlan ltp;

  ASSERT_EQ(ltp.Transform(*Parse("select * from a"))->Dump(), "project *: full-scan a");
  ASSERT_EQ(ltp.Transform(*Parse("select * from a limit 1"))->Dump(), "project *: (limit 0, 1: full-scan a)");
  ASSERT_EQ(ltp.Transform(*Parse("select * from a where false"))->Dump(), "project *: noop");
  ASSERT_EQ(ltp.Transform(*Parse("select * from a where false limit 1"))->Dump(), "project *: noop");
  ASSERT_EQ(ltp.Transform(*Parse("select * from a where false order by embedding <-> [3,1,2] limit 5"))->Dump(),
            "project *: noop");
  ASSERT_EQ(ltp.Transform(*Parse("select * from a where b > 1"))->Dump(), "project *: (filter b > 1: full-scan a)");
  ASSERT_EQ(ltp.Transform(*Parse("select a from b where c = 1 order by d"))->Dump(),
            "project a: (sort d, asc: (filter c = 1: full-scan b))");
  ASSERT_EQ(ltp.Transform(*Parse("select a from b where c = 1 limit 1"))->Dump(),
            "project a: (limit 0, 1: (filter c = 1: full-scan b))");
  ASSERT_EQ(ltp.Transform(*Parse("select a from b where c = 1 and d = 2 order by e limit 1"))->Dump(),
            "project a: (limit 0, 1: (sort e, asc: (filter (and c = 1, d = 2): full-scan b)))");
  ASSERT_EQ(ltp.Transform(*Parse("select a from b where c = 1 order by d limit 1"))->Dump(),
            "project a: (limit 0, 1: (sort d, asc: (filter c = 1: full-scan b)))");
}

TEST(IRPassTest, IntervalAnalysis) {
  auto ia_passes = PassManager::Create(IntervalAnalysis{true}, SimplifyAndOrExpr{}, SimplifyBoolean{});

  ASSERT_EQ(PassManager::Execute(ia_passes, *Parse("select * from a where a > 1 or a < 3"))->Dump(),
            "select * from a where true");
  ASSERT_EQ(PassManager::Execute(ia_passes, *Parse("select * from a where a < 1 and a > 3"))->Dump(),
            "select * from a where false");
  ASSERT_EQ(PassManager::Execute(ia_passes, *Parse("select * from a where (a > 3 or a < 1) and a = 2"))->Dump(),
            "select * from a where false");
  ASSERT_EQ(PassManager::Execute(ia_passes, *Parse("select * from a where b = 1 and (a = 1 or a != 1)"))->Dump(),
            "select * from a where b = 1");
  ASSERT_EQ(PassManager::Execute(ia_passes, *Parse("select * from a where a = 1 or b = 1 or a != 1"))->Dump(),
            "select * from a where true");
  ASSERT_EQ(PassManager::Execute(ia_passes, *Parse("select * from a where (a < 3 or a > 1) and b >= 1"))->Dump(),
            "select * from a where b >= 1");
  ASSERT_EQ(PassManager::Execute(ia_passes, *Parse("select * from a where a != 1 or a != 2"))->Dump(),
            "select * from a where true");
  ASSERT_EQ(PassManager::Execute(ia_passes, *Parse("select * from a where a = 1 and a = 2"))->Dump(),
            "select * from a where false");

  ASSERT_EQ(PassManager::Execute(ia_passes, *Parse("select * from a where a < 1 and a < 3"))->Dump(),
            "select * from a where a < 1");
  ASSERT_EQ(PassManager::Execute(ia_passes, *Parse("select * from a where a < 1 or a < 3"))->Dump(),
            "select * from a where a < 3");
  ASSERT_EQ(PassManager::Execute(ia_passes, *Parse("select * from a where a = 1 and a < 3"))->Dump(),
            "select * from a where a = 1");
  ASSERT_EQ(PassManager::Execute(ia_passes, *Parse("select * from a where a = 1 or a < 3"))->Dump(),
            "select * from a where a < 3");
  ASSERT_EQ(PassManager::Execute(ia_passes, *Parse("select * from a where a = 1 or a = 3"))->Dump(),
            "select * from a where (or a = 1, a = 3)");
  ASSERT_EQ(PassManager::Execute(ia_passes, *Parse("select * from a where a != 1"))->Dump(),
            "select * from a where a != 1");
  ASSERT_EQ(PassManager::Execute(ia_passes, *Parse("select * from a where a != 1 and a != 2"))->Dump(),
            "select * from a where (and a != 1, a != 2)");
  ASSERT_EQ(
      PassManager::Execute(ia_passes, *Parse("select * from a where a >= 0 and a >= 1 and a < 4 and a != 2"))->Dump(),
      fmt::format("select * from a where (or (and a >= 1, a < 2), (and a >= {}, a < 4))", IntervalSet::NextNum(2)));
  ASSERT_EQ(PassManager::Execute(ia_passes, *Parse("select * from a where a != 1 and b > 1 and b = 2"))->Dump(),
            "select * from a where (and a != 1, b = 2)");
}

static IndexMap MakeIndexMap() {
  auto f1 = FieldInfo("t1", std::make_unique<redis::TagFieldMetadata>());
  auto f2 = FieldInfo("t2", std::make_unique<redis::TagFieldMetadata>());
  f2.metadata->noindex = true;
  auto f3 = FieldInfo("n1", std::make_unique<redis::NumericFieldMetadata>());
  auto f4 = FieldInfo("n2", std::make_unique<redis::NumericFieldMetadata>());
  auto f5 = FieldInfo("n3", std::make_unique<redis::NumericFieldMetadata>());
  f5.metadata->noindex = true;

  auto hnsw_field_meta = std::make_unique<redis::HnswVectorFieldMetadata>();
  hnsw_field_meta->vector_type = redis::VectorType::FLOAT64;
  hnsw_field_meta->dim = 3;
  hnsw_field_meta->distance_metric = redis::DistanceMetric::L2;
  auto f6 = FieldInfo("v1", std::move(hnsw_field_meta));

  hnsw_field_meta = std::make_unique<redis::HnswVectorFieldMetadata>();
  hnsw_field_meta->vector_type = redis::VectorType::FLOAT64;
  hnsw_field_meta->dim = 3;
  hnsw_field_meta->distance_metric = redis::DistanceMetric::L2;
  auto f7 = FieldInfo("v2", std::move(hnsw_field_meta));
  f7.metadata->noindex = true;

  auto ia = std::make_unique<IndexInfo>("ia", redis::IndexMetadata(), "");
  ia->Add(std::move(f1));
  ia->Add(std::move(f2));
  ia->Add(std::move(f3));
  ia->Add(std::move(f4));
  ia->Add(std::move(f5));
  ia->Add(std::move(f6));
  ia->Add(std::move(f7));

  IndexMap res;
  res.Insert(std::move(ia));
  return res;
}

std::unique_ptr<Node> ParseS(SemaChecker& sc, const std::string& in) {
  auto res = *Parse(in);
  EXPECT_EQ(sc.Check(res.get()).Msg(), Status::ok_msg);
  return res;
}

TEST(IRPassTest, IndexSelection) {
  auto index_map = MakeIndexMap();
  auto sc = SemaChecker(index_map);

  auto passes = PassManager::Default();
  ASSERT_EQ(PassManager::Execute(passes, ParseS(sc, "select * from ia"))->Dump(), "project *: full-scan ia");
  ASSERT_EQ(PassManager::Execute(passes, ParseS(sc, "select * from ia order by n1"))->Dump(),
            "project *: numeric-scan n1, [-inf, inf), asc");
  ASSERT_EQ(PassManager::Execute(passes, ParseS(sc, "select * from ia order by n1 limit 1"))->Dump(),
            "project *: (limit 0, 1: numeric-scan n1, [-inf, inf), asc)");
  ASSERT_EQ(PassManager::Execute(passes, ParseS(sc, "select * from ia order by n3"))->Dump(),
            "project *: (sort n3, asc: full-scan ia)");
  ASSERT_EQ(PassManager::Execute(passes, ParseS(sc, "select * from ia order by n3 limit 1"))->Dump(),
            "project *: (top-n sort n3, asc, 0, 1: full-scan ia)");
  ASSERT_EQ(PassManager::Execute(passes, ParseS(sc, "select * from ia where n2 = 1 order by n1"))->Dump(),
            "project *: (filter n2 = 1: numeric-scan n1, [-inf, inf), asc)");

  ASSERT_EQ(PassManager::Execute(passes, ParseS(sc, "select * from ia where n1 = 1"))->Dump(),
            fmt::format("project *: numeric-scan n1, [1, {}), asc", IntervalSet::NextNum(1)));
  ASSERT_EQ(PassManager::Execute(passes, ParseS(sc, "select * from ia where n1 != 1"))->Dump(),
            "project *: (filter n1 != 1: full-scan ia)");
  ASSERT_EQ(PassManager::Execute(passes, ParseS(sc, "select * from ia where n1 >= 1"))->Dump(),
            "project *: numeric-scan n1, [1, inf), asc");
  ASSERT_EQ(PassManager::Execute(passes, ParseS(sc, "select * from ia where n1 >= 1 and n1 < 2"))->Dump(),
            "project *: numeric-scan n1, [1, 2), asc");
  ASSERT_EQ(PassManager::Execute(passes, ParseS(sc, "select * from ia where n1 >= 1 and n2 >= 2"))->Dump(),
            "project *: (filter n2 >= 2: numeric-scan n1, [1, inf), asc)");
  ASSERT_EQ(PassManager::Execute(passes, ParseS(sc, "select * from ia where n1 >= 1 and n2 = 2"))->Dump(),
            fmt::format("project *: (filter n1 >= 1: numeric-scan n2, [2, {}), asc)", IntervalSet::NextNum(2)));
  ASSERT_EQ(PassManager::Execute(passes, ParseS(sc, "select * from ia where n1 >= 1 and n3 = 2"))->Dump(),
            "project *: (filter n3 = 2: numeric-scan n1, [1, inf), asc)");
  ASSERT_EQ(PassManager::Execute(passes, ParseS(sc, "select * from ia where n3 = 1"))->Dump(),
            "project *: (filter n3 = 1: full-scan ia)");
  ASSERT_EQ(PassManager::Execute(passes, ParseS(sc, "select * from ia where n1 = 1 and n3 = 2"))->Dump(),
            fmt::format("project *: (filter n3 = 2: numeric-scan n1, [1, {}), asc)", IntervalSet::NextNum(1)));
  ASSERT_EQ(PassManager::Execute(passes, ParseS(sc, "select * from ia where n1 = 1 and t1 hastag \"a\""))->Dump(),
            fmt::format("project *: (filter t1 hastag \"a\": numeric-scan n1, [1, {}), asc)", IntervalSet::NextNum(1)));
  ASSERT_EQ(PassManager::Execute(passes, ParseS(sc, "select * from ia where t1 hastag \"a\""))->Dump(),
            "project *: tag-scan t1, a");
  ASSERT_EQ(
      PassManager::Execute(passes, ParseS(sc, "select * from ia where t1 hastag \"a\" and t2 hastag \"a\""))->Dump(),
      "project *: (filter t2 hastag \"a\": tag-scan t1, a)");
  ASSERT_EQ(PassManager::Execute(passes, ParseS(sc, "select * from ia where t2 hastag \"a\""))->Dump(),
            "project *: (filter t2 hastag \"a\": full-scan ia)");
  ASSERT_EQ(PassManager::Execute(passes, ParseS(sc, "select * from ia where v1 <-> [3,1,2] < 5"))->Dump(),
            "project *: hnsw-vector-range-scan v1, [3.000000, 1.000000, 2.000000], 5");
  ASSERT_EQ(PassManager::Execute(passes, ParseS(sc, "select * from ia order by v1 <-> [3,1,2] limit 5"))->Dump(),
            "project *: (limit 0, 5: hnsw-vector-knn-scan v1, [3.000000, 1.000000, 2.000000], 5)");
  ASSERT_EQ(PassManager::Execute(passes, ParseS(sc, "select * from ia order by v1 <-> [3,1,2] limit 2, 7"))->Dump(),
            "project *: (limit 2, 7: hnsw-vector-knn-scan v1, [3.000000, 1.000000, 2.000000], 9)");
  ASSERT_EQ(PassManager::Execute(passes, ParseS(sc, "select * from ia where v2 <-> [3,1,2] < 5"))->Dump(),
            "project *: (filter v2 <-> [3.000000, 1.000000, 2.000000] < 5: full-scan ia)");
  ASSERT_EQ(PassManager::Execute(passes, ParseS(sc, "select * from ia where n1 >= 1 and v1 <-> [3,1,2] < 5"))->Dump(),
            "project *: (filter n1 >= 1: hnsw-vector-range-scan v1, [3.000000, 1.000000, 2.000000], 5)");
  ASSERT_EQ(
      PassManager::Execute(passes, ParseS(sc, "select * from ia where v1 <-> [3,1,2] < 5 and t1 hastag \"a\""))->Dump(),
      "project *: (filter t1 hastag \"a\": hnsw-vector-range-scan v1, [3.000000, 1.000000, 2.000000], 5)");
  ASSERT_EQ(PassManager::Execute(passes, ParseS(sc, "select * from ia where n1 >= 2 or n1 < 1"))->Dump(),
            "project *: (merge numeric-scan n1, [-inf, 1), asc, numeric-scan n1, [2, inf), asc)");
  ASSERT_EQ(PassManager::Execute(passes, ParseS(sc, "select * from ia where n1 >= 1 or n2 >= 2"))->Dump(),
            "project *: (merge numeric-scan n1, [1, inf), asc, (filter n1 < 1: numeric-scan n2, [2, inf), asc))");
  ASSERT_EQ(
      PassManager::Execute(passes, ParseS(sc, "select * from ia where n1 >= 1 or n2 = 2"))->Dump(),
      fmt::format("project *: (merge numeric-scan n1, [1, inf), asc, (filter n1 < 1: numeric-scan n2, [2, {}), asc))",
                  IntervalSet::NextNum(2)));
  ASSERT_EQ(PassManager::Execute(passes, ParseS(sc, "select * from ia where n1 >= 1 or n3 = 2"))->Dump(),
            "project *: (filter (or n1 >= 1, n3 = 2): full-scan ia)");
  ASSERT_EQ(PassManager::Execute(passes, ParseS(sc, "select * from ia where n1 = 1 or n3 = 2"))->Dump(),
            "project *: (filter (or n1 = 1, n3 = 2): full-scan ia)");
  ASSERT_EQ(
      PassManager::Execute(passes, ParseS(sc, "select * from ia where n1 = 1 or t1 hastag \"a\""))->Dump(),
      fmt::format("project *: (merge tag-scan t1, a, (filter not t1 hastag \"a\": numeric-scan n1, [1, {}), asc))",
                  IntervalSet::NextNum(1)));
  ASSERT_EQ(
      PassManager::Execute(passes, ParseS(sc, "select * from ia where t1 hastag \"a\" or t2 hastag \"a\""))->Dump(),
      "project *: (filter (or t1 hastag \"a\", t2 hastag \"a\"): full-scan ia)");
  ASSERT_EQ(
      PassManager::Execute(passes, ParseS(sc, "select * from ia where t1 hastag \"a\" or t1 hastag \"b\""))->Dump(),
      "project *: (merge tag-scan t1, a, (filter not t1 hastag \"a\": tag-scan t1, b))");

  ASSERT_EQ(
      PassManager::Execute(
          passes, ParseS(sc, "select * from ia where (n1 < 2 or n1 >= 3) and (n1 >= 1 and n1 < 4) and not n3 != 1"))
          ->Dump(),
      "project *: (filter n3 = 1: (merge numeric-scan n1, [1, 2), asc, numeric-scan n1, [3, 4), asc))");
}
