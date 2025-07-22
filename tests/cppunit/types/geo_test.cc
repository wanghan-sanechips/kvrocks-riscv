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
#include <math.h>

#include <memory>

#include "test_base.h"
#include "types/redis_geo.h"

class RedisGeoTest : public TestBase {
 protected:
  RedisGeoTest() { geo_ = std::make_unique<redis::Geo>(storage_.get(), "geo_ns"); }
  ~RedisGeoTest() override = default;

  void SetUp() override {
    key_ = "test_geo_key";
    fields_ = {"geo_test_key-1", "geo_test_key-2", "geo_test_key-3", "geo_test_key-4",
               "geo_test_key-5", "geo_test_key-6", "geo_test_key-7"};
    longitudes_ = {-180, -1.23402, -1.23402, 0, 1.23402, 1.23402, 179.12345};
    latitudes_ = {-85.05112878, -1.23402, -1.23402, 0, 1.23402, 1.23402, 85.0511};
    geo_hashes_ = {"00bh0hbj200", "7zz0gzm7m10", "7zz0gzm7m10", "s0000000000",
                   "s00zh0dsdy0", "s00zh0dsdy0", "zzp7u51dwf0"};
  }

  std::vector<double> longitudes_;
  std::vector<double> latitudes_;
  std::vector<std::string> geo_hashes_;
  std::unique_ptr<redis::Geo> geo_;
};

TEST_F(RedisGeoTest, Add) {
  uint64_t ret = 0;
  std::vector<GeoPoint> geo_points;
  for (size_t i = 0; i < fields_.size(); i++) {
    geo_points.emplace_back(GeoPoint{longitudes_[i], latitudes_[i], fields_[i].ToString()});
  }
  geo_->Add(*ctx_, key_, &geo_points, &ret);
  EXPECT_EQ(fields_.size(), ret);
  std::vector<std::string> geo_hashes;
  geo_->Hash(*ctx_, key_, fields_, &geo_hashes);
  for (size_t i = 0; i < fields_.size(); i++) {
    EXPECT_EQ(geo_hashes[i], geo_hashes_[i]);
  }
  geo_->Add(*ctx_, key_, &geo_points, &ret);
  EXPECT_EQ(ret, 0);
  auto s = geo_->Del(*ctx_, key_);
}

TEST_F(RedisGeoTest, Dist) {
  uint64_t ret = 0;
  std::vector<GeoPoint> geo_points;
  for (size_t i = 0; i < fields_.size(); i++) {
    geo_points.emplace_back(GeoPoint{longitudes_[i], latitudes_[i], fields_[i].ToString()});
  }
  geo_->Add(*ctx_, key_, &geo_points, &ret);
  EXPECT_EQ(fields_.size(), ret);
  double dist = 0.0;
  geo_->Dist(*ctx_, key_, fields_[2], fields_[3], &dist);
  EXPECT_EQ(ceilf(dist), 194102);
  auto s = geo_->Del(*ctx_, key_);
}

TEST_F(RedisGeoTest, Hash) {
  uint64_t ret = 0;
  std::vector<GeoPoint> geo_points;
  for (size_t i = 0; i < fields_.size(); i++) {
    geo_points.emplace_back(GeoPoint{longitudes_[i], latitudes_[i], fields_[i].ToString()});
  }
  geo_->Add(*ctx_, key_, &geo_points, &ret);
  EXPECT_EQ(static_cast<int>(fields_.size()), ret);
  std::vector<std::string> geo_hashes;
  geo_->Hash(*ctx_, key_, fields_, &geo_hashes);
  for (size_t i = 0; i < fields_.size(); i++) {
    EXPECT_EQ(geo_hashes[i], geo_hashes_[i]);
  }
  auto s = geo_->Del(*ctx_, key_);
}

TEST_F(RedisGeoTest, Pos) {
  uint64_t ret = 0;
  std::vector<GeoPoint> geo_points;
  for (size_t i = 0; i < fields_.size(); i++) {
    geo_points.emplace_back(GeoPoint{longitudes_[i], latitudes_[i], fields_[i].ToString()});
  }
  geo_->Add(*ctx_, key_, &geo_points, &ret);
  EXPECT_EQ(static_cast<int>(fields_.size()), ret);
  std::map<std::string, GeoPoint> gps;
  geo_->Pos(*ctx_, key_, fields_, &gps);
  for (size_t i = 0; i < fields_.size(); i++) {
    EXPECT_EQ(gps[fields_[i].ToString()].member, fields_[i].ToString());
    EXPECT_EQ(geo_->EncodeGeoHash(gps[fields_[i].ToString()].longitude, gps[fields_[i].ToString()].latitude),
              geo_hashes_[i]);
  }
  auto s = geo_->Del(*ctx_, key_);
}

TEST_F(RedisGeoTest, Radius) {
  uint64_t ret = 0;
  std::vector<GeoPoint> geo_points;
  for (size_t i = 0; i < fields_.size(); i++) {
    geo_points.emplace_back(GeoPoint{longitudes_[i], latitudes_[i], fields_[i].ToString()});
  }
  geo_->Add(*ctx_, key_, &geo_points, &ret);
  EXPECT_EQ(static_cast<int>(fields_.size()), ret);
  std::vector<GeoPoint> gps;
  geo_->Radius(*ctx_, key_, longitudes_[0], latitudes_[0], 100000000, 100, kSortASC, std::string(), false, 1, &gps);
  EXPECT_EQ(gps.size(), fields_.size());
  for (size_t i = 0; i < gps.size(); i++) {
    EXPECT_EQ(gps[i].member, fields_[i].ToString());
    EXPECT_EQ(geo_->EncodeGeoHash(gps[i].longitude, gps[i].latitude), geo_hashes_[i]);
  }
  auto s = geo_->Del(*ctx_, key_);
}

TEST_F(RedisGeoTest, RadiusByMember) {
  uint64_t ret = 0;
  std::vector<GeoPoint> geo_points;
  for (size_t i = 0; i < fields_.size(); i++) {
    geo_points.emplace_back(GeoPoint{longitudes_[i], latitudes_[i], fields_[i].ToString()});
  }
  geo_->Add(*ctx_, key_, &geo_points, &ret);
  EXPECT_EQ(fields_.size(), ret);
  std::vector<GeoPoint> gps;
  geo_->RadiusByMember(*ctx_, key_, fields_[0], 100000000, 100, kSortASC, std::string(), false, 1, &gps);
  EXPECT_EQ(gps.size(), fields_.size());
  for (size_t i = 0; i < gps.size(); i++) {
    EXPECT_EQ(gps[i].member, fields_[i].ToString());
    EXPECT_EQ(geo_->EncodeGeoHash(gps[i].longitude, gps[i].latitude), geo_hashes_[i]);
  }
  auto s = geo_->Del(*ctx_, key_);
}
