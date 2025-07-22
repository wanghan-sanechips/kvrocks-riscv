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

#include <limits>
#include <map>
#include <string>
#include <vector>

#include "geohash.h"
#include "storage/redis_db.h"
#include "storage/redis_metadata.h"
#include "types/redis_zset.h"

enum DistanceUnit {
  kDistanceMeter,
  kDistanceKilometers,
  kDistanceMiles,
  kDistanceFeet,
};

enum DistanceSort {
  kSortNone,
  kSortASC,
  kSortDESC,
};

enum OriginPointType { kNone, kLongLat, kMember };

// Structures represent points and array of points on the earth.
struct GeoPoint {
  double longitude;
  double latitude;
  std::string member;
  double dist;
  double score;
};

namespace redis {

class Geo : public ZSet {
 public:
  explicit Geo(engine::Storage *storage, const std::string &ns) : ZSet(storage, ns) {}
  rocksdb::Status Add(engine::Context &ctx, const Slice &user_key, std::vector<GeoPoint> *geo_points,
                      uint64_t *added_cnt);
  rocksdb::Status Dist(engine::Context &ctx, const Slice &user_key, const Slice &member_1, const Slice &member_2,
                       double *dist);
  rocksdb::Status Hash(engine::Context &ctx, const Slice &user_key, const std::vector<Slice> &members,
                       std::vector<std::string> *geo_hashes);
  rocksdb::Status Pos(engine::Context &ctx, const Slice &user_key, const std::vector<Slice> &members,
                      std::map<std::string, GeoPoint> *geo_points);
  rocksdb::Status Radius(engine::Context &ctx, const Slice &user_key, double longitude, double latitude,
                         double radius_meters, int count, DistanceSort sort, const std::string &store_key,
                         bool store_distance, double unit_conversion, std::vector<GeoPoint> *geo_points);
  rocksdb::Status RadiusByMember(engine::Context &ctx, const Slice &user_key, const Slice &member, double radius_meters,
                                 int count, DistanceSort sort, const std::string &store_key, bool store_distance,
                                 double unit_conversion, std::vector<GeoPoint> *geo_points);
  rocksdb::Status Search(engine::Context &ctx, const Slice &user_key, GeoShape geo_shape, OriginPointType point_type,
                         std::string &member, int count, DistanceSort sort, bool store_distance, double unit_conversion,
                         std::vector<GeoPoint> *geo_points);
  rocksdb::Status SearchStore(engine::Context &ctx, const Slice &user_key, GeoShape geo_shape,
                              OriginPointType point_type, std::string &member, int count, DistanceSort sort,
                              const std::string &store_key, bool store_distance, double unit_conversion,
                              std::vector<GeoPoint> *geo_points);
  rocksdb::Status Get(engine::Context &ctx, const Slice &user_key, const Slice &member, GeoPoint *geo_point);
  rocksdb::Status MGet(engine::Context &ctx, const Slice &user_key, const std::vector<Slice> &members,
                       std::map<std::string, GeoPoint> *geo_points);
  static std::string EncodeGeoHash(double longitude, double latitude);

 private:
  static int decodeGeoHash(double bits, double *xy);
  int membersOfAllNeighbors(engine::Context &ctx, const Slice &user_key, GeoHashRadius n, const GeoShape &geo_shape,
                            std::vector<GeoPoint> *geo_points);
  int membersOfGeoHashBox(engine::Context &ctx, const Slice &user_key, GeoHashBits hash,
                          std::vector<GeoPoint> *geo_points, const GeoShape &geo_shape);
  static void scoresOfGeoHashBox(GeoHashBits hash, GeoHashFix52Bits *min, GeoHashFix52Bits *max);
  int getPointsInRange(engine::Context &ctx, const Slice &user_key, double min, double max, const GeoShape &geo_shape,
                       std::vector<GeoPoint> *geo_points);
  static bool appendIfWithinRadius(std::vector<GeoPoint> *geo_points, double lon, double lat, double radius,
                                   double score, const std::string &member);
  static bool appendIfWithinShape(std::vector<GeoPoint> *geo_points, const GeoShape &geo_shape, double score,
                                  const std::string &member);
  static bool sortGeoPointASC(const GeoPoint &gp1, const GeoPoint &gp2);
  static bool sortGeoPointDESC(const GeoPoint &gp1, const GeoPoint &gp2);
};

}  // namespace redis
