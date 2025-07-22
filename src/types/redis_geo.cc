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

#include "redis_geo.h"

#include <algorithm>

namespace redis {

rocksdb::Status Geo::Add(engine::Context &ctx, const Slice &user_key, std::vector<GeoPoint> *geo_points,
                         uint64_t *added_cnt) {
  std::vector<MemberScore> member_scores;
  for (const auto &geo_point : *geo_points) {
    /* Turn the coordinates into the score of the element. */
    GeoHashBits hash;
    GeohashEncodeWGS84(geo_point.longitude, geo_point.latitude, GEO_STEP_MAX, &hash);
    GeoHashFix52Bits bits = GeoHashHelper::Align52Bits(hash);
    member_scores.emplace_back(MemberScore{geo_point.member, static_cast<double>(bits)});
  }
  return ZSet::Add(ctx, user_key, ZAddFlags::Default(), &member_scores, added_cnt);
}

rocksdb::Status Geo::Dist(engine::Context &ctx, const Slice &user_key, const Slice &member_1, const Slice &member_2,
                          double *dist) {
  std::map<std::string, GeoPoint> geo_points;
  auto s = MGet(ctx, user_key, {member_1, member_2}, &geo_points);
  if (!s.ok()) return s;

  for (const auto &member : {member_1, member_2}) {
    auto iter = geo_points.find(member.ToString());
    if (iter == geo_points.end()) {
      return rocksdb::Status::NotFound();
    }
  }
  *dist =
      GeoHashHelper::GetDistance(geo_points[member_1.ToString()].longitude, geo_points[member_1.ToString()].latitude,
                                 geo_points[member_2.ToString()].longitude, geo_points[member_2.ToString()].latitude);
  return rocksdb::Status::OK();
}

rocksdb::Status Geo::Hash(engine::Context &ctx, const Slice &user_key, const std::vector<Slice> &members,
                          std::vector<std::string> *geo_hashes) {
  std::map<std::string, GeoPoint> geo_points;
  auto s = MGet(ctx, user_key, members, &geo_points);
  if (!s.ok()) return s;

  for (const auto &member : members) {
    auto iter = geo_points.find(member.ToString());
    if (iter == geo_points.end()) {
      geo_hashes->emplace_back(std::string());
      continue;
    }
    geo_hashes->emplace_back(EncodeGeoHash(iter->second.longitude, iter->second.latitude));
  }

  return rocksdb::Status::OK();
}

rocksdb::Status Geo::Pos(engine::Context &ctx, const Slice &user_key, const std::vector<Slice> &members,
                         std::map<std::string, GeoPoint> *geo_points) {
  return MGet(ctx, user_key, members, geo_points);
}

rocksdb::Status Geo::Radius(engine::Context &ctx, const Slice &user_key, double longitude, double latitude,
                            double radius_meters, int count, DistanceSort sort, const std::string &store_key,
                            bool store_distance, double unit_conversion, std::vector<GeoPoint> *geo_points) {
  GeoShape geo_shape;
  geo_shape.type = kGeoShapeTypeCircular;
  geo_shape.xy[0] = longitude;
  geo_shape.xy[1] = latitude;
  geo_shape.radius = radius_meters;
  geo_shape.conversion = 1;

  std::string dummy_member;
  return SearchStore(ctx, user_key, geo_shape, kLongLat, dummy_member, count, sort, store_key, store_distance,
                     unit_conversion, geo_points);
}

rocksdb::Status Geo::RadiusByMember(engine::Context &ctx, const Slice &user_key, const Slice &member,
                                    double radius_meters, int count, DistanceSort sort, const std::string &store_key,
                                    bool store_distance, double unit_conversion, std::vector<GeoPoint> *geo_points) {
  GeoPoint geo_point;
  auto s = Get(ctx, user_key, member, &geo_point);
  if (!s.ok()) return s.IsNotFound() ? rocksdb::Status::OK() : s;

  return Radius(ctx, user_key, geo_point.longitude, geo_point.latitude, radius_meters, count, sort, store_key,
                store_distance, unit_conversion, geo_points);
}

rocksdb::Status Geo::Search(engine::Context &ctx, const Slice &user_key, GeoShape geo_shape, OriginPointType point_type,
                            std::string &member, int count, DistanceSort sort, bool store_distance,
                            double unit_conversion, std::vector<GeoPoint> *geo_points) {
  return SearchStore(ctx, user_key, geo_shape, point_type, member, count, sort, "", store_distance, unit_conversion,
                     geo_points);
}

rocksdb::Status Geo::SearchStore(engine::Context &ctx, const Slice &user_key, GeoShape geo_shape,
                                 OriginPointType point_type, std::string &member, int count, DistanceSort sort,
                                 const std::string &store_key, bool store_distance, double unit_conversion,
                                 std::vector<GeoPoint> *geo_points) {
  if (point_type == kMember) {
    GeoPoint geo_point;
    auto s = Get(ctx, user_key, member, &geo_point);
    // store key is not empty, try to remove it before returning.
    if (!s.ok() && s.IsNotFound() && !store_key.empty()) {
      auto del_s = ZSet::Del(ctx, store_key);
      if (!del_s.ok()) return del_s;
    }
    if (!s.ok()) return s.IsNotFound() ? rocksdb::Status::OK() : s;

    geo_shape.xy[0] = geo_point.longitude;
    geo_shape.xy[1] = geo_point.latitude;
  }

  std::string ns_key = AppendNamespacePrefix(user_key);
  ZSetMetadata metadata(false);
  rocksdb::Status s = ZSet::GetMetadata(ctx, ns_key, &metadata);
  // store key is not empty, try to remove it before returning.
  if (!s.ok() && s.IsNotFound() && !store_key.empty()) {
    auto del_s = ZSet::Del(ctx, store_key);
    if (!del_s.ok()) return del_s;
  }
  if (!s.ok()) return s.IsNotFound() ? rocksdb::Status::OK() : s;

  // Get neighbor geohash boxes for radius search
  GeoHashRadius georadius = GeoHashHelper::GetAreasByShapeWGS84(geo_shape);

  // Get zset for all matching points
  membersOfAllNeighbors(ctx, user_key, georadius, geo_shape, geo_points);

  // if no matching results, give empty reply
  if (geo_points->empty()) {
    // store key is not empty, try to remove it before returning.
    if (!store_key.empty()) {
      auto del_s = ZSet::Del(ctx, store_key);
      if (!del_s.ok()) return del_s;
    }
    return rocksdb::Status::OK();
  }

  // process [optional] sorting
  if (sort == kSortASC) {
    std::sort(geo_points->begin(), geo_points->end(), sortGeoPointASC);
  } else if (sort == kSortDESC) {
    std::sort(geo_points->begin(), geo_points->end(), sortGeoPointDESC);
  }

  // storing
  if (!store_key.empty()) {
    auto result_length = static_cast<int64_t>(geo_points->size());
    int64_t returned_items_count = (count == 0 || result_length < count) ? result_length : count;
    if (returned_items_count == 0) {
      auto s = ZSet::Del(ctx, user_key);
      if (!s.ok()) return s;
    } else {
      std::vector<MemberScore> member_scores;
      for (const auto &geo_point : *geo_points) {
        if (returned_items_count-- <= 0) {
          break;
        }
        double score = store_distance ? geo_point.dist / unit_conversion : geo_point.score;
        member_scores.emplace_back(MemberScore{geo_point.member, score});
      }
      ZSet::Overwrite(ctx, store_key, member_scores);
    }
  }
  return rocksdb::Status::OK();
}

rocksdb::Status Geo::Get(engine::Context &ctx, const Slice &user_key, const Slice &member, GeoPoint *geo_point) {
  std::map<std::string, GeoPoint> geo_points;
  auto s = MGet(ctx, user_key, {member}, &geo_points);
  if (!s.ok()) return s;

  auto iter = geo_points.find(member.ToString());
  if (iter == geo_points.end()) {
    return rocksdb::Status::InvalidArgument("could not decode requested zset member");
  }
  *geo_point = iter->second;
  return rocksdb::Status::OK();
}

rocksdb::Status Geo::MGet(engine::Context &ctx, const Slice &user_key, const std::vector<Slice> &members,
                          std::map<std::string, GeoPoint> *geo_points) {
  std::map<std::string, double> member_scores;
  auto s = ZSet::MGet(ctx, user_key, members, &member_scores);
  if (!s.ok()) return s;

  for (const auto &member : members) {
    auto iter = member_scores.find(member.ToString());
    if (iter == member_scores.end()) {
      continue;
    }
    double xy[2];
    if (!decodeGeoHash(iter->second, xy)) {
      continue;
    }
    (*geo_points)[member.ToString()] = GeoPoint{xy[0], xy[1], member.ToString()};
  }
  return rocksdb::Status::OK();
}

std::string Geo::EncodeGeoHash(double longitude, double latitude) {
  const std::string geoalphabet = "0123456789bcdefghjkmnpqrstuvwxyz";
  /* The internal format we use for geocoding is a bit different
   * than the standard, since we use as initial latitude range
   * -85,85, while the normal geohashing algorithm uses -90,90.
   * So we have to decode our position and re-encode using the
   * standard ranges in order to output a valid geohash string. */

  /* Re-encode */
  GeoHashRange r[2];
  GeoHashBits hash;
  r[0].min = -180;
  r[0].max = 180;
  r[1].min = -90;
  r[1].max = 90;
  GeohashEncode(&r[0], &r[1], longitude, latitude, 26, &hash);

  std::string geo_hash;
  for (int i = 0; i < 11; i++) {
    int idx = 0;
    if (i == 10) {
      /* We have just 52 bits, but the API used to output
       * an 11 bytes geohash. For compatibility we assume
       * zero. */
      idx = 0;
    } else {
      idx = static_cast<int>((hash.bits >> (52 - ((i + 1) * 5))) & 0x1f);
    }
    geo_hash += geoalphabet[idx];
  }
  return geo_hash;
}

int Geo::decodeGeoHash(double bits, double *xy) {
  GeoHashBits hash = {(uint64_t)bits, GEO_STEP_MAX};
  return GeohashDecodeToLongLatWGS84(hash, xy);
}

/* Search all eight neighbors + self geohash box */
int Geo::membersOfAllNeighbors(engine::Context &ctx, const Slice &user_key, GeoHashRadius n, const GeoShape &geo_shape,
                               std::vector<GeoPoint> *geo_points) {
  GeoHashBits neighbors[9];
  unsigned int last_processed = 0;
  int count = 0;

  neighbors[0] = n.hash;
  neighbors[1] = n.neighbors.north;
  neighbors[2] = n.neighbors.south;
  neighbors[3] = n.neighbors.east;
  neighbors[4] = n.neighbors.west;
  neighbors[5] = n.neighbors.north_east;
  neighbors[6] = n.neighbors.north_west;
  neighbors[7] = n.neighbors.south_east;
  neighbors[8] = n.neighbors.south_west;

  /* For each neighbor (*and* our own hashbox), get all the matching
   * members and add them to the potential result list. */
  for (unsigned int i = 0; i < sizeof(neighbors) / sizeof(*neighbors); i++) {
    if (HASHISZERO(neighbors[i])) {
      continue;
    }

    /* When a huge Radius (in the 5000 km range or more) is used,
     * adjacent neighbors can be the same, leading to duplicated
     * elements. Skip every range which is the same as the one
     * processed previously. */
    if (last_processed && neighbors[i].bits == neighbors[last_processed].bits &&
        neighbors[i].step == neighbors[last_processed].step) {
      continue;
    }
    count += membersOfGeoHashBox(ctx, user_key, neighbors[i], geo_points, geo_shape);
    last_processed = i;
  }
  return count;
}

/* Obtain all members between the min/max of this geohash bounding box.
 * Populate a GeoArray of GeoPoints by calling getPointsInRange().
 * Return the number of points added to the array. */
int Geo::membersOfGeoHashBox(engine::Context &ctx, const Slice &user_key, GeoHashBits hash,
                             std::vector<GeoPoint> *geo_points, const GeoShape &geo_shape) {
  GeoHashFix52Bits min = 0, max = 0;

  scoresOfGeoHashBox(hash, &min, &max);
  return getPointsInRange(ctx, user_key, static_cast<double>(min), static_cast<double>(max), geo_shape, geo_points);
}

/* Compute the sorted set scores min (inclusive), max (exclusive) we should
 * query in order to retrieve all the elements inside the specified area
 * 'hash'. The two scores are returned by reference in *min and *max. */
void Geo::scoresOfGeoHashBox(GeoHashBits hash, GeoHashFix52Bits *min, GeoHashFix52Bits *max) {
  /* We want to compute the sorted set scores that will include all the
   * elements inside the specified GeoHash 'hash', which has as many
   * bits as specified by hash.step * 2.
   *
   * So if step is, for example, 3, and the hash value in binary
   * is 101010, since our score is 52 bits we want every element which
   * is in binary: 101010?????????????????????????????????????????????
   * Where ? can be 0 or 1.
   *
   * To get the min score we just use the initial hash value left
   * shifted enough to get the 52 bit value. Later we increment the
   * 6 bit prefis (see the hash.bits++ statement), and get the new
   * prefix: 101011, which we align again to 52 bits to get the maximum
   * value (which is excluded from the search). So we get everything
   * between the two following scores (represented in binary):
   *
   * 1010100000000000000000000000000000000000000000000000 (included)
   * and
   * 1010110000000000000000000000000000000000000000000000 (excluded).
   */
  *min = GeoHashHelper::Align52Bits(hash);
  hash.bits++;
  *max = GeoHashHelper::Align52Bits(hash);
}

/* Query a Redis sorted set to extract all the elements between 'min' and
 * 'max', appending them into the array of GeoPoint structures 'gparray'.
 * The command returns the number of elements added to the array.
 *
 * Elements which are farthest than 'radius' from the specified 'x' and 'y'
 * coordinates are not included.
 *
 * The ability of this function to append to an existing set of points is
 * important for good performances because querying by radius is performed
 * using multiple queries to the sorted set, that we later need to sort
 * via qsort. Similarly we need to be able to reject points outside the search
 * radius area ASAP in order to allocate and process more points than needed. */
int Geo::getPointsInRange(engine::Context &ctx, const Slice &user_key, double min, double max,
                          const GeoShape &geo_shape, std::vector<GeoPoint> *geo_points) {
  /* include min in range; exclude max in range */
  /* That's: min <= val < max */
  RangeScoreSpec spec;
  spec.min = min;
  spec.max = max;
  spec.maxex = true;
  uint64_t size = 0;
  std::vector<MemberScore> member_scores;
  rocksdb::Status s = ZSet::RangeByScore(ctx, user_key, spec, &member_scores, &size);
  if (!s.ok()) return 0;

  for (const auto &member_score : member_scores) {
    appendIfWithinShape(geo_points, geo_shape, member_score.score, member_score.member);
  }
  return 0;
}

/* Helper function for geoGetPointsInRange(): given a sorted set score
 * representing a point, and another point (the center of our search) and
 * a radius, appends this entry as a geoPoint into the specified geoArray
 * only if the point is within the search area.
 *
 * returns true if the point is included, or false if it is outside. */
bool Geo::appendIfWithinRadius(std::vector<GeoPoint> *geo_points, double lon, double lat, double radius, double score,
                               const std::string &member) {
  double distance = NAN, xy[2];

  if (!decodeGeoHash(score, xy)) return false; /* Can't decode. */
  /* Note that geohashGetDistanceIfInRadiusWGS84() takes arguments in
   * reverse order: longitude first, latitude later. */
  if (!GeoHashHelper::GetDistanceIfInRadiusWGS84(lon, lat, xy[0], xy[1], radius, &distance)) {
    return false;
  }

  /* Append the new element. */
  GeoPoint geo_point;
  geo_point.longitude = xy[0];
  geo_point.latitude = xy[1];
  geo_point.dist = distance;
  geo_point.member = member;
  geo_point.score = score;
  geo_points->emplace_back(geo_point);
  return true;
}

bool Geo::appendIfWithinShape(std::vector<GeoPoint> *geo_points, const GeoShape &geo_shape, double score,
                              const std::string &member) {
  double distance = NAN, xy[2];
  if (!decodeGeoHash(score, xy)) return false;
  if (geo_shape.type == kGeoShapeTypeCircular) {
    if (!GeoHashHelper::GetDistanceIfInRadiusWGS84(geo_shape.xy[0], geo_shape.xy[1], xy[0], xy[1],
                                                   geo_shape.radius * geo_shape.conversion, &distance)) {
      return false;
    }
  } else if (geo_shape.type == kGeoShapeTypeRectangular) {
    if (!GeoHashHelper::GetDistanceIfInBoxWGS84(geo_shape.bounds, geo_shape.xy[0], geo_shape.xy[1], xy[0], xy[1],
                                                &distance)) {
      return false;
    }
  }

  /* Append the new element. */
  GeoPoint geo_point;
  geo_point.longitude = xy[0];
  geo_point.latitude = xy[1];
  geo_point.dist = distance;
  geo_point.member = member;
  geo_point.score = score;
  geo_points->emplace_back(geo_point);
  return true;
}

bool Geo::sortGeoPointASC(const GeoPoint &gp1, const GeoPoint &gp2) { return gp1.dist < gp2.dist; }

bool Geo::sortGeoPointDESC(const GeoPoint &gp1, const GeoPoint &gp2) { return gp1.dist >= gp2.dist; }

}  // namespace redis
