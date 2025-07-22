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

#include <gtest/gtest.h>

#include <memory>

#include "test_base.h"
#include "types/redis_hyperloglog.h"

class RedisHyperLogLogTest : public TestBase {
 protected:
  explicit RedisHyperLogLogTest() : TestBase() {
    hll_ = std::make_unique<redis::HyperLogLog>(storage_.get(), "hll_ns");
  }
  ~RedisHyperLogLogTest() override = default;

  void SetUp() override {
    TestBase::SetUp();
    [[maybe_unused]] auto s = hll_->Del(*ctx_, "hll");
    for (int x = 1; x <= 3; x++) {
      s = hll_->Del(*ctx_, "hll" + std::to_string(x));
    }
  }

  void TearDown() override {
    TestBase::SetUp();
    [[maybe_unused]] auto s = hll_->Del(*ctx_, "hll");
    for (int x = 1; x <= 3; x++) {
      s = hll_->Del(*ctx_, "hll" + std::to_string(x));
    }
  }

  std::unique_ptr<redis::HyperLogLog> hll_;

  static std::vector<uint64_t> computeHashes(const std::vector<std::string_view> &elements) {
    std::vector<uint64_t> hashes;
    hashes.reserve(elements.size());
    for (const auto &element : elements) {
      hashes.push_back(redis::HyperLogLog::HllHash(element));
    }
    return hashes;
  }
};

TEST_F(RedisHyperLogLogTest, PFADD) {
  uint64_t ret = 0;
  ASSERT_TRUE(hll_->Add(*ctx_, "hll", {}, &ret).ok() && ret == 0);
  // Approximated cardinality after creation is zero
  ASSERT_TRUE(hll_->Count(*ctx_, "hll", &ret).ok() && ret == 0);
  // PFADD returns 1 when at least 1 reg was modified
  ASSERT_TRUE(hll_->Add(*ctx_, "hll", computeHashes({"a", "b", "c"}), &ret).ok());
  ASSERT_EQ(1, ret);
  ASSERT_TRUE(hll_->Count(*ctx_, "hll", &ret).ok());
  ASSERT_EQ(3, ret);
  // PFADD returns 0 when no reg was modified
  ASSERT_TRUE(hll_->Add(*ctx_, "hll", computeHashes({"a", "b", "c"}), &ret).ok() && ret == 0);
  // PFADD works with empty string
  ASSERT_TRUE(hll_->Add(*ctx_, "hll", computeHashes({""}), &ret).ok() && ret == 1);
  // PFADD works with similar hash, which is likely to be in the same bucket
  ASSERT_TRUE(hll_->Add(*ctx_, "hll", {1, 2, 3, 2, 1}, &ret).ok() && ret == 1);
  ASSERT_TRUE(hll_->Count(*ctx_, "hll", &ret).ok());
  ASSERT_EQ(7, ret);
}

TEST_F(RedisHyperLogLogTest, PFCOUNT_returns_approximated_cardinality_of_set) {
  uint64_t ret = 0;
  // pf add "1" to "5"
  ASSERT_TRUE(hll_->Add(*ctx_, "hll", computeHashes({"1", "2", "3", "4", "5"}), &ret).ok() && ret == 1);
  // pf count is 5
  ASSERT_TRUE(hll_->Count(*ctx_, "hll", &ret).ok() && ret == 5);
  // pf add "6" to "10"
  ASSERT_TRUE(hll_->Add(*ctx_, "hll", computeHashes({"6", "7", "8", "8", "9", "10"}), &ret).ok() && ret == 1);
  // pf count is 10
  ASSERT_TRUE(hll_->Count(*ctx_, "hll", &ret).ok() && ret == 10);
}

TEST_F(RedisHyperLogLogTest, PFMERGE_results_on_the_cardinality_of_union_of_sets) {
  uint64_t ret = 0;
  // pf add hll1 a b c
  ASSERT_TRUE(hll_->Add(*ctx_, "hll1", computeHashes({"a", "b", "c"}), &ret).ok() && ret == 1);
  // pf add hll2 b c d
  ASSERT_TRUE(hll_->Add(*ctx_, "hll2", computeHashes({"b", "c", "d"}), &ret).ok() && ret == 1);
  // pf add hll3 c d e
  ASSERT_TRUE(hll_->Add(*ctx_, "hll3", computeHashes({"c", "d", "e"}), &ret).ok() && ret == 1);
  // pf merge hll hll1 hll2 hll3
  ASSERT_TRUE(hll_->Merge(*ctx_, "hll", {"hll1", "hll2", "hll3"}).ok());
  // pf count hll is 5
  ASSERT_TRUE(hll_->Count(*ctx_, "hll", &ret).ok());
  ASSERT_EQ(5, ret);
}

TEST_F(RedisHyperLogLogTest, PFCOUNT_multiple) {
  uint64_t ret = 0;
  ASSERT_TRUE(hll_->CountMultiple(*ctx_, {"hll1", "hll2", "hll3"}, &ret).ok());
  ASSERT_EQ(0, ret);
  // pf add hll1 a b c
  ASSERT_TRUE(hll_->Add(*ctx_, "hll1", computeHashes({"a", "b", "c"}), &ret).ok() && ret == 1);
  ASSERT_TRUE(hll_->Count(*ctx_, "hll1", &ret).ok());
  ASSERT_EQ(3, ret);
  ASSERT_TRUE(hll_->CountMultiple(*ctx_, {"hll1", "hll2", "hll3"}, &ret).ok());
  ASSERT_EQ(3, ret);
  // pf add hll2 b c d
  ASSERT_TRUE(hll_->Add(*ctx_, "hll2", computeHashes({"b", "c", "d"}), &ret).ok() && ret == 1);
  ASSERT_TRUE(hll_->CountMultiple(*ctx_, {"hll1", "hll2", "hll3"}, &ret).ok());
  ASSERT_EQ(4, ret);
  // pf add hll3 c d e
  ASSERT_TRUE(hll_->Add(*ctx_, "hll3", computeHashes({"c", "d", "e"}), &ret).ok() && ret == 1);
  ASSERT_TRUE(hll_->CountMultiple(*ctx_, {"hll1", "hll2", "hll3"}, &ret).ok());
  ASSERT_EQ(5, ret);
  // pf merge hll hll1 hll2 hll3
  ASSERT_TRUE(hll_->Merge(*ctx_, "hll", {"hll1", "hll2", "hll3"}).ok());
  // pf count hll is 5
  ASSERT_TRUE(hll_->Count(*ctx_, "hll", &ret).ok());
  ASSERT_EQ(5, ret);
  ASSERT_TRUE(hll_->CountMultiple(*ctx_, {"hll1", "hll2", "hll3", "hll"}, &ret).ok());
  ASSERT_EQ(5, ret);
}

TEST_F(RedisHyperLogLogTest, PFCOUNT_multiple_keys_merge_returns_cardinality_of_union_1) {
  for (int x = 1; x < 1000; x++) {
    uint64_t ret = 0;
    ASSERT_TRUE(hll_->Add(*ctx_, "hll0", computeHashes({"foo-" + std::to_string(x)}), &ret).ok());
    ASSERT_TRUE(hll_->Add(*ctx_, "hll1", computeHashes({"bar-" + std::to_string(x)}), &ret).ok());
    ASSERT_TRUE(hll_->Add(*ctx_, "hll2", computeHashes({"zap-" + std::to_string(x)}), &ret).ok());
    std::vector<uint64_t> cards(3);
    ASSERT_TRUE(hll_->Count(*ctx_, "hll0", &cards[0]).ok());
    ASSERT_TRUE(hll_->Count(*ctx_, "hll1", &cards[1]).ok());
    ASSERT_TRUE(hll_->Count(*ctx_, "hll2", &cards[2]).ok());
    auto card = static_cast<double>(cards[0] + cards[1] + cards[2]);
    double realcard = x * 3;
    // assert the ABS of 'card' and 'realcart' is within 5% of the cardinality
    double left = std::abs(card - realcard);
    double right = card / 100 * 5;
    ASSERT_LT(left, right) << "left : " << left << ", right: " << right;
  }
}

TEST_F(RedisHyperLogLogTest, PFCOUNT_multiple_keys_merge_returns_cardinality_of_union_2) {
  std::srand(time(nullptr));
  std::vector<int> realcard_vec;
  for (auto i = 1; i < 1000; i++) {
    for (auto j = 0; j < 3; j++) {
      uint64_t ret = 0;
      int rint = std::rand() % 20000;
      ASSERT_TRUE(hll_->Add(*ctx_, "hll" + std::to_string(j), computeHashes({std::to_string(rint)}), &ret).ok());
      realcard_vec.push_back(rint);
    }
  }
  std::vector<uint64_t> cards(3);
  ASSERT_TRUE(hll_->Count(*ctx_, "hll0", &cards[0]).ok());
  ASSERT_TRUE(hll_->Count(*ctx_, "hll1", &cards[1]).ok());
  ASSERT_TRUE(hll_->Count(*ctx_, "hll2", &cards[2]).ok());
  auto card = static_cast<double>(cards[0] + cards[1] + cards[2]);
  auto realcard = static_cast<double>(realcard_vec.size());
  double left = std::abs(card - realcard);
  double right = card / 100 * 5;
  ASSERT_LT(left, right) << "left : " << left << ", right: " << right;
}
