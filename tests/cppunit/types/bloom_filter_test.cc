// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "types/bloom_filter.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <limits>
#include <memory>
#include <random>
#include <string>
#include <utility>
#include <vector>

namespace test {

TEST(ConstructorTest, TestBloomFilter) {
  // It return false because the number of bytes of Bloom filter bitset must be a power of 2.
  std::unique_ptr<uint8_t[]> bitset1(new uint8_t[1024]());
  EXPECT_FALSE(CreateBlockSplitBloomFilter(bitset1.get(), 1023));

  // It return false because the number of bytes of Bloom filter bitset must be a power of 2.
  std::string bitset2(1022, 's');
  EXPECT_FALSE(CreateBlockSplitBloomFilter(bitset2));
}

// The BasicTest is used to test basic operations including InsertHash, FindHash and
// serializing and de-serializing.
TEST(BasicTest, TestBloomFilter) {
  const std::vector<uint32_t> kBloomFilterSizes = {32, 64, 128, 256, 512, 1024, 2048};
  const std::vector<std::string> kStringInserts = {"1", "2",  "3",  "5", "6",   "7",    "8",
                                                   "9", "10", "42", "a", "abc", "qwert"};
  const std::vector<std::string> kStringLookups = {"-1", "-2", "-3",  "-5",    "-6",   "-7",
                                                   "-8", "b",  "acb", "tyuio", "trewq"};

  for (const auto bloom_filter_bytes : kBloomFilterSizes) {
    auto [bloom_filter, data] = CreateBlockSplitBloomFilter(bloom_filter_bytes);

    // Empty bloom filter deterministically returns false
    for (const auto& v : kStringInserts) {
      EXPECT_FALSE(bloom_filter.FindHash(bloom_filter.Hash(v.data(), v.size())));
    }

    // Insert all values
    for (const auto& v : kStringInserts) {
      bloom_filter.InsertHash(bloom_filter.Hash(v.data(), v.size()));
    }

    // They should always lookup successfully
    for (const auto& v : kStringInserts) {
      EXPECT_TRUE(bloom_filter.FindHash(bloom_filter.Hash(v.data(), v.size())));
    }

    // Values not inserted in the filter should only rarely lookup successfully
    int false_positives = 0;
    for (const auto& v : kStringLookups) {
      false_positives += bloom_filter.FindHash(bloom_filter.Hash(v.data(), v.size()));
    }
    // (this is a crude check, see FPPTest below for a more rigorous formula)
    EXPECT_LE(false_positives, 2);

    // ReBuild Bloom filter from string bitset
    BlockSplitBloomFilter bloom_filter_new(data);

    // Lookup previously inserted values
    for (const auto& v : kStringInserts) {
      EXPECT_TRUE(bloom_filter_new.FindHash(bloom_filter_new.Hash(v.data(), v.size())));
    }
    false_positives = 0;
    for (const auto& v : kStringLookups) {
      false_positives += bloom_filter_new.FindHash(bloom_filter_new.Hash(v.data(), v.size()));
    }
    EXPECT_LE(false_positives, 2);
  }
}

// Helper function to generate random string.
std::string GetRandomString(uint32_t length) {
  // Character set used to generate random string
  const std::string charset = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

  std::default_random_engine gen(42);
  std::uniform_int_distribution<uint32_t> dist(0, static_cast<int>(charset.size() - 1));
  std::string ret(length, 'x');

  for (uint32_t i = 0; i < length; i++) {
    ret[i] = charset[dist(gen)];
  }
  return ret;
}

TEST(FPPTest, TestBloomFilter) {
  // It counts the number of times FindHash returns true.
  int exist = 0;

  const int total_count = 100000;

  // Bloom filter fpp parameter
  const double fpp = 0.01;

  std::vector<std::string> members;
  auto [bloom_filter, _] = CreateBlockSplitBloomFilter(BlockSplitBloomFilter::OptimalNumOfBytes(total_count, fpp));

  // Insert elements into the Bloom filter
  for (int i = 0; i < total_count; i++) {
    // Insert random string which length is 8
    std::string tmp = GetRandomString(8);
    members.push_back(tmp);
    bloom_filter.InsertHash(bloom_filter.Hash(tmp.data(), tmp.size()));
  }

  for (int i = 0; i < total_count; i++) {
    ASSERT_TRUE(bloom_filter.FindHash(bloom_filter.Hash(members[i].data(), members[i].size())));
    std::string tmp = GetRandomString(7);

    if (bloom_filter.FindHash(bloom_filter.Hash(tmp.data(), tmp.size()))) {
      exist++;
    }
  }

  // The exist should be probably less than 1000 according default FPP 0.01.
  EXPECT_LT(exist, total_count * fpp);
}

// OptimalValueTest is used to test whether OptimalNumOfBits returns expected
// numbers according to formula:
//     num_of_bits = -8.0 * ndv / log(1 - pow(fpp, 1.0 / 8.0))
// where ndv is the number of distinct values and fpp is the false positive probability.
// Also it is used to test whether OptimalNumOfBits returns value between
// [MINIMUM_BLOOM_FILTER_SIZE, MAXIMUM_BLOOM_FILTER_SIZE].
TEST(OptimalValueTest, TestBloomFilter) {
  auto test_optimal_num_estimation = [](uint32_t ndv, double fpp, uint32_t num_bits) {
    EXPECT_EQ(BlockSplitBloomFilter::OptimalNumOfBits(ndv, fpp), num_bits);
    EXPECT_EQ(BlockSplitBloomFilter::OptimalNumOfBytes(ndv, fpp), num_bits / 8);
  };

  test_optimal_num_estimation(256, 0.01, UINT32_C(4096));
  test_optimal_num_estimation(512, 0.01, UINT32_C(8192));
  test_optimal_num_estimation(1024, 0.01, UINT32_C(16384));
  test_optimal_num_estimation(2048, 0.01, UINT32_C(32768));

  test_optimal_num_estimation(200, 0.01, UINT32_C(2048));
  test_optimal_num_estimation(300, 0.01, UINT32_C(4096));
  test_optimal_num_estimation(700, 0.01, UINT32_C(8192));
  test_optimal_num_estimation(1500, 0.01, UINT32_C(16384));

  test_optimal_num_estimation(200, 0.025, UINT32_C(2048));
  test_optimal_num_estimation(300, 0.025, UINT32_C(4096));
  test_optimal_num_estimation(700, 0.025, UINT32_C(8192));
  test_optimal_num_estimation(1500, 0.025, UINT32_C(16384));

  test_optimal_num_estimation(200, 0.05, UINT32_C(2048));
  test_optimal_num_estimation(300, 0.05, UINT32_C(4096));
  test_optimal_num_estimation(700, 0.05, UINT32_C(8192));
  test_optimal_num_estimation(1500, 0.05, UINT32_C(16384));

  // Boundary check
  test_optimal_num_estimation(4, 0.01, kMinimumBloomFilterBytes * 8);
  test_optimal_num_estimation(4, 0.25, kMinimumBloomFilterBytes * 8);

  test_optimal_num_estimation(std::numeric_limits<uint32_t>::max(), 0.01, kMaximumBloomFilterBytes * 8);
  test_optimal_num_estimation(std::numeric_limits<uint32_t>::max(), 0.25, kMaximumBloomFilterBytes * 8);
}

}  // namespace test
