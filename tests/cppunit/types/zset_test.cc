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
#include "types/redis_zset.h"

class RedisZSetTest : public TestBase {
 protected:
  RedisZSetTest() { zset_ = std::make_unique<redis::ZSet>(storage_.get(), "zset_ns"); }
  ~RedisZSetTest() override = default;

  void SetUp() override {
    key_ = "test_zset_key";
    fields_ = {"zset_test_key-1", "zset_test_key-2", "zset_test_key-3", "zset_test_key-4",
               "zset_test_key-5", "zset_test_key-6", "zset_test_key-7"};
    scores_ = {-100.1, -100.1, -1.234, 0, 1.234, 1.234, 100.1};
  }

  std::vector<double> scores_;
  std::unique_ptr<redis::ZSet> zset_;
};

TEST_F(RedisZSetTest, Add) {
  uint64_t ret = 0;
  std::vector<MemberScore> mscores;
  for (size_t i = 0; i < fields_.size(); i++) {
    mscores.emplace_back(MemberScore{fields_[i].ToString(), scores_[i]});
  }
  zset_->Add(*ctx_, key_, ZAddFlags::Default(), &mscores, &ret);
  EXPECT_EQ(static_cast<int>(fields_.size()), ret);
  for (size_t i = 0; i < fields_.size(); i++) {
    double got = 0.0;
    rocksdb::Status s = zset_->Score(*ctx_, key_, fields_[i], &got);
    EXPECT_EQ(scores_[i], got);
  }
  zset_->Add(*ctx_, key_, ZAddFlags::Default(), &mscores, &ret);
  EXPECT_EQ(ret, 0);
  auto s = zset_->Del(*ctx_, key_);
}

TEST_F(RedisZSetTest, IncrBy) {
  uint64_t ret = 0;
  std::vector<MemberScore> mscores;
  for (size_t i = 0; i < fields_.size(); i++) {
    mscores.emplace_back(MemberScore{fields_[i].ToString(), scores_[i]});
  }
  zset_->Add(*ctx_, key_, ZAddFlags::Default(), &mscores, &ret);
  EXPECT_EQ(fields_.size(), ret);
  for (size_t i = 0; i < fields_.size(); i++) {
    double increment = 12.3;
    double score = 0.0;
    zset_->IncrBy(*ctx_, key_, fields_[i], increment, &score);
    EXPECT_EQ(scores_[i] + increment, score);
  }
  auto s = zset_->Del(*ctx_, key_);
}

TEST_F(RedisZSetTest, Remove) {
  uint64_t ret = 0;
  std::vector<MemberScore> mscores;
  for (size_t i = 0; i < fields_.size(); i++) {
    mscores.emplace_back(MemberScore{fields_[i].ToString(), scores_[i]});
  }
  zset_->Add(*ctx_, key_, ZAddFlags::Default(), &mscores, &ret);
  EXPECT_EQ(fields_.size(), ret);
  zset_->Remove(*ctx_, key_, fields_, &ret);
  EXPECT_EQ(fields_.size(), ret);
  for (auto &field : fields_) {
    double score = 0.0;
    rocksdb::Status s = zset_->Score(*ctx_, key_, field, &score);
    EXPECT_TRUE(s.IsNotFound());
  }
  auto s = zset_->Del(*ctx_, key_);
}

TEST_F(RedisZSetTest, AddAndRemoveRepeated) {
  std::vector<std::string> members{"m1", "m1", "m2", "m3"};
  std::vector<double> scores{1.1, 1.11, 2.2, 3.3};

  uint64_t ret = 0;
  std::vector<MemberScore> mscores;
  for (size_t i = 0; i < members.size(); i++) {
    mscores.emplace_back(MemberScore{members[i], scores[i]});
  }
  zset_->Add(*ctx_, key_, ZAddFlags::Default(), &mscores, &ret);
  EXPECT_EQ(mscores.size() - 1, ret);
  double score = 0.0;
  zset_->Score(*ctx_, key_, members[0], &score);
  EXPECT_EQ(scores[1], score);
  uint64_t card = 0;
  zset_->Card(*ctx_, key_, &card);
  EXPECT_EQ(mscores.size() - 1, card);

  std::vector<rocksdb::Slice> members_to_remove{"m1", "m2", "m2"};
  zset_->Remove(*ctx_, key_, members_to_remove, &ret);
  EXPECT_EQ(members_to_remove.size() - 1, ret);
  zset_->Card(*ctx_, key_, &card);
  EXPECT_EQ(mscores.size() - 1 - ret, card);
  zset_->Score(*ctx_, key_, members[3], &score);
  EXPECT_EQ(scores[3], score);

  auto s = zset_->Del(*ctx_, key_);
}

TEST_F(RedisZSetTest, Range) {
  uint64_t ret = 0;
  std::vector<MemberScore> mscores;
  for (size_t i = 0; i < fields_.size(); i++) {
    mscores.emplace_back(MemberScore{fields_[i].ToString(), scores_[i]});
  }
  uint64_t count = mscores.size() - 1;
  zset_->Add(*ctx_, key_, ZAddFlags::Default(), &mscores, &ret);
  EXPECT_EQ(fields_.size(), ret);
  RangeRankSpec rank_spec;
  rank_spec.start = 0;
  rank_spec.stop = -2;
  zset_->RangeByRank(*ctx_, key_, rank_spec, &mscores, nullptr);
  EXPECT_EQ(mscores.size(), count);
  for (size_t i = 0; i < mscores.size(); i++) {
    EXPECT_EQ(mscores[i].member, fields_[i].ToString());
    EXPECT_EQ(mscores[i].score, scores_[i]);
  }
  auto s = zset_->Del(*ctx_, key_);
}

TEST_F(RedisZSetTest, RevRange) {
  uint64_t ret = 0;
  std::vector<MemberScore> mscores;
  for (size_t i = 0; i < fields_.size(); i++) {
    mscores.emplace_back(MemberScore{fields_[i].ToString(), scores_[i]});
  }
  uint64_t count = mscores.size() - 1;
  zset_->Add(*ctx_, key_, ZAddFlags::Default(), &mscores, &ret);
  EXPECT_EQ(static_cast<int>(fields_.size()), ret);
  RangeRankSpec rank_spec;
  rank_spec.start = 0;
  rank_spec.stop = -2;
  rank_spec.reversed = true;
  zset_->RangeByRank(*ctx_, key_, rank_spec, &mscores, nullptr);
  EXPECT_EQ(mscores.size(), count);
  for (size_t i = 0; i < mscores.size(); i++) {
    EXPECT_EQ(mscores[i].member, fields_[count - i].ToString());
    EXPECT_EQ(mscores[i].score, scores_[count - i]);
  }
  auto s = zset_->Del(*ctx_, key_);
}

TEST_F(RedisZSetTest, PopMin) {
  uint64_t ret = 0;
  std::vector<MemberScore> mscores;
  for (size_t i = 0; i < fields_.size(); i++) {
    mscores.emplace_back(MemberScore{fields_[i].ToString(), scores_[i]});
  }
  zset_->Add(*ctx_, key_, ZAddFlags::Default(), &mscores, &ret);
  EXPECT_EQ(fields_.size(), ret);
  zset_->Pop(*ctx_, key_, static_cast<int>(mscores.size() - 1), true, &mscores);
  for (size_t i = 0; i < mscores.size(); i++) {
    EXPECT_EQ(mscores[i].member, fields_[i].ToString());
    EXPECT_EQ(mscores[i].score, scores_[i]);
  }
  zset_->Pop(*ctx_, key_, 1, true, &mscores);
  EXPECT_EQ(mscores[0].member, fields_[fields_.size() - 1].ToString());
  EXPECT_EQ(mscores[0].score, scores_[fields_.size() - 1]);
}

TEST_F(RedisZSetTest, PopMax) {
  uint64_t ret = 0;
  std::vector<MemberScore> mscores;
  uint64_t count = fields_.size();
  for (size_t i = 0; i < fields_.size(); i++) {
    mscores.emplace_back(MemberScore{fields_[i].ToString(), scores_[i]});
  }
  zset_->Add(*ctx_, key_, ZAddFlags::Default(), &mscores, &ret);
  EXPECT_EQ(fields_.size(), ret);
  zset_->Pop(*ctx_, key_, static_cast<int>(mscores.size() - 1), false, &mscores);
  for (size_t i = 0; i < mscores.size(); i++) {
    EXPECT_EQ(mscores[i].member, fields_[count - i - 1].ToString());
    EXPECT_EQ(mscores[i].score, scores_[count - i - 1]);
  }
  zset_->Pop(*ctx_, key_, 1, true, &mscores);
  EXPECT_EQ(mscores[0].member, fields_[0].ToString());
}

TEST_F(RedisZSetTest, RangeByLex) {
  uint64_t ret = 0;
  uint64_t count = fields_.size();
  std::vector<MemberScore> mscores;
  for (size_t i = 0; i < fields_.size(); i++) {
    mscores.emplace_back(MemberScore{fields_[i].ToString(), scores_[i]});
  }
  zset_->Add(*ctx_, key_, ZAddFlags::Default(), &mscores, &ret);
  EXPECT_EQ(count, ret);

  RangeLexSpec spec;
  spec.min = fields_[0].ToString();
  spec.max = fields_[fields_.size() - 1].ToString();
  zset_->RangeByLex(*ctx_, key_, spec, &mscores, nullptr);
  EXPECT_EQ(mscores.size(), fields_.size());
  for (size_t i = 0; i < mscores.size(); i++) {
    EXPECT_EQ(mscores[i].member, fields_[i].ToString());
    EXPECT_EQ(mscores[i].score, scores_[i]);
  }

  spec.minex = true;
  zset_->RangeByLex(*ctx_, key_, spec, &mscores, nullptr);
  EXPECT_EQ(mscores.size(), fields_.size() - 1);
  for (size_t i = 0; i < mscores.size(); i++) {
    EXPECT_EQ(mscores[i].member, fields_[i + 1].ToString());
    EXPECT_EQ(mscores[i].score, scores_[i + 1]);
  }

  spec.minex = false;
  spec.maxex = true;
  zset_->RangeByLex(*ctx_, key_, spec, &mscores, nullptr);
  EXPECT_EQ(mscores.size(), fields_.size() - 1);
  for (size_t i = 0; i < mscores.size(); i++) {
    EXPECT_EQ(mscores[i].member, fields_[i].ToString());
    EXPECT_EQ(mscores[i].score, scores_[i]);
  }

  spec.minex = true;
  spec.maxex = true;
  zset_->RangeByLex(*ctx_, key_, spec, &mscores, nullptr);
  EXPECT_EQ(mscores.size(), fields_.size() - 2);
  for (size_t i = 0; i < mscores.size(); i++) {
    EXPECT_EQ(mscores[i].member, fields_[i + 1].ToString());
    EXPECT_EQ(mscores[i].score, scores_[i + 1]);
  }
  spec.minex = false;
  spec.maxex = false;
  spec.min = "-";
  spec.max = "+";
  spec.max_infinite = true;
  spec.reversed = true;
  zset_->RangeByLex(*ctx_, key_, spec, &mscores, nullptr);
  EXPECT_EQ(mscores.size(), fields_.size());
  for (size_t i = 0; i < mscores.size(); i++) {
    EXPECT_EQ(mscores[i].member, fields_[count - i - 1].ToString());
    EXPECT_EQ(mscores[i].score, scores_[count - i - 1]);
  }

  auto s = zset_->Del(*ctx_, key_);
}

TEST_F(RedisZSetTest, RangeByScore) {
  uint64_t ret = 0;
  std::vector<MemberScore> mscores;
  for (size_t i = 0; i < fields_.size(); i++) {
    mscores.emplace_back(MemberScore{fields_[i].ToString(), scores_[i]});
  }
  zset_->Add(*ctx_, key_, ZAddFlags::Default(), &mscores, &ret);
  EXPECT_EQ(fields_.size(), ret);

  // test case: inclusive the min and max score
  RangeScoreSpec spec;
  spec.min = scores_[0];
  spec.max = scores_[scores_.size() - 2];
  zset_->RangeByScore(*ctx_, key_, spec, &mscores, nullptr);
  EXPECT_EQ(mscores.size(), scores_.size() - 1);
  for (size_t i = 0; i < mscores.size(); i++) {
    EXPECT_EQ(mscores[i].member, fields_[i].ToString());
    EXPECT_EQ(mscores[i].score, scores_[i]);
  }
  // test case: exclusive the min score
  spec.minex = true;
  zset_->RangeByScore(*ctx_, key_, spec, &mscores, nullptr);
  EXPECT_EQ(mscores.size(), scores_.size() - 3);
  for (size_t i = 0; i < mscores.size(); i++) {
    EXPECT_EQ(mscores[i].member, fields_[i + 2].ToString());
    EXPECT_EQ(mscores[i].score, scores_[i + 2]);
  }
  // test case: exclusive the max score
  spec.minex = false;
  spec.maxex = true;
  zset_->RangeByScore(*ctx_, key_, spec, &mscores, nullptr);
  EXPECT_EQ(mscores.size(), scores_.size() - 3);
  for (size_t i = 0; i < mscores.size(); i++) {
    EXPECT_EQ(mscores[i].member, fields_[i].ToString());
    EXPECT_EQ(mscores[i].score, scores_[i]);
  }
  // test case: exclusive the min and max score
  spec.minex = true;
  spec.maxex = true;
  zset_->RangeByScore(*ctx_, key_, spec, &mscores, nullptr);
  EXPECT_EQ(mscores.size(), scores_.size() - 5);
  for (size_t i = 0; i < mscores.size(); i++) {
    EXPECT_EQ(mscores[i].member, fields_[i + 2].ToString());
    EXPECT_EQ(mscores[i].score, scores_[i + 2]);
  }
  auto s = zset_->Del(*ctx_, key_);
}

TEST_F(RedisZSetTest, RangeByScoreWithLimit) {
  uint64_t ret = 0;
  std::vector<MemberScore> mscores;
  for (size_t i = 0; i < fields_.size(); i++) {
    mscores.emplace_back(MemberScore{fields_[i].ToString(), scores_[i]});
  }
  zset_->Add(*ctx_, key_, ZAddFlags::Default(), &mscores, &ret);
  EXPECT_EQ(fields_.size(), ret);

  RangeScoreSpec spec;
  spec.offset = 1;
  spec.count = 2;
  zset_->RangeByScore(*ctx_, key_, spec, &mscores, nullptr);
  EXPECT_EQ(mscores.size(), 2);
  for (size_t i = 0; i < mscores.size(); i++) {
    EXPECT_EQ(mscores[i].member, fields_[i + 1].ToString());
    EXPECT_EQ(mscores[i].score, scores_[i + 1]);
  }
  auto s = zset_->Del(*ctx_, key_);
}

TEST_F(RedisZSetTest, RemRangeByScore) {
  uint64_t ret = 0;
  std::vector<MemberScore> mscores;
  for (size_t i = 0; i < fields_.size(); i++) {
    mscores.emplace_back(MemberScore{fields_[i].ToString(), scores_[i]});
  }
  zset_->Add(*ctx_, key_, ZAddFlags::Default(), &mscores, &ret);
  EXPECT_EQ(fields_.size(), ret);
  RangeScoreSpec spec;
  spec.with_deletion = true;

  spec.min = scores_[0];
  spec.max = scores_[scores_.size() - 2];
  zset_->RangeByScore(*ctx_, key_, spec, nullptr, &ret);
  EXPECT_EQ(scores_.size() - 1, ret);

  spec.min = scores_[scores_.size() - 1];
  spec.max = spec.min;
  zset_->RangeByScore(*ctx_, key_, spec, nullptr, &ret);
  EXPECT_EQ(1, ret);
}

TEST_F(RedisZSetTest, RemoveRangeByRank) {
  uint64_t ret = 0;
  std::vector<MemberScore> mscores;
  for (size_t i = 0; i < fields_.size(); i++) {
    mscores.emplace_back(MemberScore{fields_[i].ToString(), scores_[i]});
  }
  zset_->Add(*ctx_, key_, ZAddFlags::Default(), &mscores, &ret);
  EXPECT_EQ(fields_.size(), ret);

  RangeRankSpec spec;
  spec.with_deletion = true;

  spec.start = 0;
  spec.stop = static_cast<int>(fields_.size() - 2);
  zset_->RangeByRank(*ctx_, key_, spec, nullptr, &ret);
  EXPECT_EQ(fields_.size() - 1, ret);

  spec.start = 0;
  spec.stop = 2;
  zset_->RangeByRank(*ctx_, key_, spec, nullptr, &ret);
  EXPECT_EQ(1, ret);
}

TEST_F(RedisZSetTest, RemoveRevRangeByRank) {
  uint64_t ret = 0;
  std::vector<MemberScore> mscores;
  for (size_t i = 0; i < fields_.size(); i++) {
    mscores.emplace_back(MemberScore{fields_[i].ToString(), scores_[i]});
  }
  zset_->Add(*ctx_, key_, ZAddFlags::Default(), &mscores, &ret);
  EXPECT_EQ(fields_.size(), ret);

  RangeRankSpec spec;
  spec.with_deletion = true;

  spec.start = 0;
  spec.stop = static_cast<int>(fields_.size() - 2);
  zset_->RangeByRank(*ctx_, key_, spec, nullptr, &ret);
  EXPECT_EQ(fields_.size() - 1, ret);

  spec.start = 0;
  spec.stop = 2;
  zset_->RangeByRank(*ctx_, key_, spec, nullptr, &ret);
  EXPECT_EQ(1, ret);
}

TEST_F(RedisZSetTest, Rank) {
  uint64_t ret = 0;
  std::vector<MemberScore> mscores;
  for (size_t i = 0; i < fields_.size(); i++) {
    mscores.emplace_back(MemberScore{fields_[i].ToString(), scores_[i]});
  }
  zset_->Add(*ctx_, key_, ZAddFlags::Default(), &mscores, &ret);
  EXPECT_EQ(static_cast<int>(fields_.size()), ret);

  for (size_t i = 0; i < fields_.size(); i++) {
    int rank = 0;
    double score = 0.0;
    zset_->Rank(*ctx_, key_, fields_[i], false, &rank, &score);
    EXPECT_EQ(i, rank);
    EXPECT_EQ(scores_[i], score);
  }
  for (size_t i = 0; i < fields_.size(); i++) {
    int rank = 0;
    double score = 0.0;
    zset_->Rank(*ctx_, key_, fields_[i], true, &rank, &score);
    EXPECT_EQ(i, static_cast<int>(fields_.size() - rank - 1));
    EXPECT_EQ(scores_[i], score);
  }
  std::vector<std::string> no_exist_members = {"a", "b"};
  for (const auto &member : no_exist_members) {
    int rank = 0;
    double score = 0.0;
    zset_->Rank(*ctx_, key_, member, true, &rank, &score);
    EXPECT_EQ(-1, rank);
    EXPECT_EQ(0.0, score);
  }
  auto s = zset_->Del(*ctx_, key_);
}

TEST_F(RedisZSetTest, RandMember) {
  uint64_t ret = 0;
  {
    std::vector<MemberScore> in_mscores;
    in_mscores.reserve(fields_.size());
    for (size_t i = 0; i < fields_.size(); i++) {
      in_mscores.emplace_back(MemberScore{fields_[i].ToString(), scores_[i]});
    }
    zset_->Add(*ctx_, key_, ZAddFlags::Default(), &in_mscores, &ret);
    EXPECT_EQ(static_cast<int>(fields_.size()), ret);
  }

  std::unordered_map<std::string, double> member_map;
  for (size_t i = 0; i < fields_.size(); i++) {
    member_map[fields_[i].ToString()] = scores_[i];
  }

  // count = 0
  {
    std::vector<MemberScore> mscores;
    rocksdb::Status s = zset_->RandMember(*ctx_, key_, 0, &mscores);
    EXPECT_EQ(0, mscores.size());
    EXPECT_TRUE(s.ok());
  }

  // count = 1/-1
  for (int64_t count : {1, -1}) {
    std::vector<MemberScore> mscores;
    rocksdb::Status s = zset_->RandMember(*ctx_, key_, count, &mscores);
    EXPECT_EQ(1, mscores.size());
    EXPECT_TRUE(s.ok());
    EXPECT_NE(member_map.find(mscores[0].member), member_map.end());
  }

  auto no_duplicate_members = [](const std::vector<MemberScore> &mscores) {
    std::unordered_set<std::string> member_set;
    for (const auto &mscore : mscores) {
      if (member_set.find(mscore.member) != member_set.end()) {
        return false;
      }
      member_set.insert(mscore.member);
    }
    return true;
  };

  auto no_non_exist_members = [&member_map](const std::vector<MemberScore> &mscores) {
    for (const auto &mscore : mscores) {
      const auto find_res = member_map.find(mscore.member);
      if (find_res == member_map.end() || find_res->second != mscore.score) {
        return false;
      }
    }
    return true;
  };

  // count > 1, but count <= fields_.size()
  for (int64_t count : {
           static_cast<int64_t>(fields_.size()),
           static_cast<int64_t>(fields_.size() / 2),
       }) {
    std::vector<MemberScore> mscores;
    rocksdb::Status s = zset_->RandMember(*ctx_, key_, count, &mscores);
    EXPECT_EQ(static_cast<size_t>(count), mscores.size());
    EXPECT_TRUE(s.ok());
    ASSERT_TRUE(no_non_exist_members(mscores));
    ASSERT_TRUE(no_duplicate_members(mscores));
  }

  // count < -1, but count >= -fields_.size()
  for (int64_t count : {
           -static_cast<int64_t>(fields_.size()),
           -static_cast<int64_t>(fields_.size() / 2),
       }) {
    std::vector<MemberScore> mscores;
    rocksdb::Status s = zset_->RandMember(*ctx_, key_, count, &mscores);
    EXPECT_EQ(static_cast<size_t>(-count), mscores.size());
    EXPECT_TRUE(s.ok());
    ASSERT_TRUE(no_non_exist_members(mscores));
  }

  // cout < -fields_.size() or count > fields_.size()

  for (int64_t count : {
           static_cast<int64_t>(fields_.size() + 10),
           -static_cast<int64_t>(fields_.size() + 10),
       }) {
    std::vector<MemberScore> mscores;
    rocksdb::Status s = zset_->RandMember(*ctx_, key_, count, &mscores);
    EXPECT_TRUE(s.ok());
    ASSERT_TRUE(no_non_exist_members(mscores));
    if (count > 0) {
      EXPECT_EQ(fields_.size(), mscores.size());
      ASSERT_TRUE(no_duplicate_members(mscores));
    } else {
      EXPECT_EQ(static_cast<size_t>(-count), mscores.size());
    }
  }

  auto s = zset_->Del(*ctx_, key_);
  EXPECT_TRUE(s.ok());
}

TEST_F(RedisZSetTest, Diff) {
  uint64_t ret = 0;

  std::string k1 = "key1";
  std::vector<MemberScore> k1_mscores = {{"a", -100.1}, {"b", -100.1}, {"c", 0}, {"d", 1.234}};

  std::string k2 = "key2";
  std::vector<MemberScore> k2_mscores = {{"c", -150.1}};

  std::string k3 = "key3";
  std::vector<MemberScore> k3_mscores = {{"a", -1000.1}, {"c", -100.1}, {"e", 8000.9}};

  auto s = zset_->Add(*ctx_, k1, ZAddFlags::Default(), &k1_mscores, &ret);
  EXPECT_EQ(ret, 4);
  zset_->Add(*ctx_, k2, ZAddFlags::Default(), &k2_mscores, &ret);
  EXPECT_EQ(ret, 1);
  zset_->Add(*ctx_, k3, ZAddFlags::Default(), &k3_mscores, &ret);
  EXPECT_EQ(ret, 3);

  std::vector<MemberScore> mscores;
  zset_->Diff(*ctx_, {k1, k2, k3}, &mscores);

  EXPECT_EQ(2, mscores.size());
  std::vector<MemberScore> expected_mscores = {{"b", -100.1}, {"d", 1.234}};
  int index = 0;
  for (const auto &mscore : expected_mscores) {
    EXPECT_EQ(mscore.member, mscores[index].member);
    EXPECT_EQ(mscore.score, mscores[index].score);
    index++;
  }

  s = zset_->Del(*ctx_, k1);
  EXPECT_TRUE(s.ok());
  s = zset_->Del(*ctx_, k2);
  EXPECT_TRUE(s.ok());
  s = zset_->Del(*ctx_, k3);
  EXPECT_TRUE(s.ok());
}

TEST_F(RedisZSetTest, DiffStore) {
  uint64_t ret = 0;

  std::string k1 = "key1";
  std::vector<MemberScore> k1_mscores = {{"a", -100.1}, {"b", -100.1}, {"c", 0}, {"d", 1.234}};

  std::string k2 = "key2";
  std::vector<MemberScore> k2_mscores = {{"c", -150.1}};

  auto s = zset_->Add(*ctx_, k1, ZAddFlags::Default(), &k1_mscores, &ret);
  EXPECT_EQ(ret, 4);
  zset_->Add(*ctx_, k2, ZAddFlags::Default(), &k2_mscores, &ret);
  EXPECT_EQ(ret, 1);

  uint64_t stored_count = 0;
  zset_->DiffStore(*ctx_, "zsetdiff", {k1, k2}, &stored_count);
  EXPECT_EQ(stored_count, 3);

  RangeScoreSpec spec;
  std::vector<MemberScore> mscores;
  zset_->RangeByScore(*ctx_, "zsetdiff", spec, &mscores, nullptr);
  EXPECT_EQ(mscores.size(), 3);

  std::vector<MemberScore> expected_mscores = {{"a", -100.1}, {"b", -100.1}, {"d", 1.234}};
  int index = 0;
  for (const auto &mscore : expected_mscores) {
    EXPECT_EQ(mscore.member, mscores[index].member);
    EXPECT_EQ(mscore.score, mscores[index].score);
    index++;
  }

  s = zset_->Del(*ctx_, k1);
  EXPECT_TRUE(s.ok());
  s = zset_->Del(*ctx_, k2);
  EXPECT_TRUE(s.ok());
  s = zset_->Del(*ctx_, "zsetdiff");
  EXPECT_TRUE(s.ok());
}
