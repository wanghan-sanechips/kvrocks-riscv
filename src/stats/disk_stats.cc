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

#include "disk_stats.h"

#include <memory>
#include <string>

#include "db_util.h"
#include "rocksdb/status.h"
#include "storage/redis_metadata.h"
#include "types/redis_zset.h"

namespace redis {

rocksdb::Status Disk::GetApproximateSizes(const Metadata &metadata, const Slice &ns_key,
                                          rocksdb::ColumnFamilyHandle *column_family, uint64_t *key_size,
                                          Slice subkeyleft, Slice subkeyright) {
  std::string prefix_key = InternalKey(ns_key, subkeyleft, metadata.version, storage_->IsSlotIdEncoded()).Encode();
  std::string next_version_prefix_key =
      InternalKey(ns_key, subkeyright, metadata.version + 1, storage_->IsSlotIdEncoded()).Encode();
  auto key_range = rocksdb::Range(prefix_key, next_version_prefix_key);
  uint64_t tmp_size = 0;
  rocksdb::Status s = storage_->GetDB()->GetApproximateSizes(option_, column_family, &key_range, 1, &tmp_size);
  if (!s.ok()) return s;
  *key_size += tmp_size;
  return rocksdb::Status::OK();
}

rocksdb::Status Disk::GetKeySize(engine::Context &ctx, const Slice &user_key, RedisType type, uint64_t *key_size) {
  *key_size = 0;
  std::string ns_key = AppendNamespacePrefix(user_key);
  switch (type) {
    case RedisType::kRedisString:
      return GetStringSize(ns_key, key_size);
    case RedisType::kRedisHash:
      return GetHashSize(ctx, ns_key, key_size);
    case RedisType::kRedisBitmap:
      return GetBitmapSize(ctx, ns_key, key_size);
    case RedisType::kRedisList:
      return GetListSize(ctx, ns_key, key_size);
    case RedisType::kRedisSet:
      return GetSetSize(ctx, ns_key, key_size);
    case RedisType::kRedisSortedint:
      return GetSortedintSize(ctx, ns_key, key_size);
    case RedisType::kRedisZSet:
      return GetZsetSize(ctx, ns_key, key_size);
    case RedisType::kRedisStream:
      return GetStreamSize(ctx, ns_key, key_size);
    default:
      return rocksdb::Status::NotFound("Not found ", user_key);
  }
}

rocksdb::Status Disk::GetStringSize(const Slice &ns_key, uint64_t *key_size) {
  auto limit = ns_key.ToString() + static_cast<char>(0);
  auto key_range = rocksdb::Range(Slice(ns_key), Slice(limit));
  return storage_->GetDB()->GetApproximateSizes(option_, metadata_cf_handle_, &key_range, 1, key_size);
}

rocksdb::Status Disk::GetHashSize(engine::Context &ctx, const Slice &ns_key, uint64_t *key_size) {
  HashMetadata metadata(false);
  rocksdb::Status s = Database::GetMetadata(ctx, {kRedisHash}, ns_key, &metadata);
  if (!s.ok()) return s.IsNotFound() ? rocksdb::Status::OK() : s;
  return GetApproximateSizes(metadata, ns_key, storage_->GetCFHandle(ColumnFamilyID::PrimarySubkey), key_size);
}

rocksdb::Status Disk::GetSetSize(engine::Context &ctx, const Slice &ns_key, uint64_t *key_size) {
  SetMetadata metadata(false);
  rocksdb::Status s = Database::GetMetadata(ctx, {kRedisSet}, ns_key, &metadata);
  if (!s.ok()) return s.IsNotFound() ? rocksdb::Status::OK() : s;
  return GetApproximateSizes(metadata, ns_key, storage_->GetCFHandle(ColumnFamilyID::PrimarySubkey), key_size);
}

rocksdb::Status Disk::GetListSize(engine::Context &ctx, const Slice &ns_key, uint64_t *key_size) {
  ListMetadata metadata(false);
  rocksdb::Status s = Database::GetMetadata(ctx, {kRedisList}, ns_key, &metadata);
  if (!s.ok()) return s.IsNotFound() ? rocksdb::Status::OK() : s;
  std::string buf;
  PutFixed64(&buf, metadata.head);
  return GetApproximateSizes(metadata, ns_key, storage_->GetCFHandle(ColumnFamilyID::PrimarySubkey), key_size, buf);
}

rocksdb::Status Disk::GetZsetSize(engine::Context &ctx, const Slice &ns_key, uint64_t *key_size) {
  ZSetMetadata metadata(false);
  rocksdb::Status s = Database::GetMetadata(ctx, {kRedisZSet}, ns_key, &metadata);
  if (!s.ok()) return s.IsNotFound() ? rocksdb::Status::OK() : s;
  std::string score_bytes;
  PutDouble(&score_bytes, kMinScore);
  s = GetApproximateSizes(metadata, ns_key, storage_->GetCFHandle(ColumnFamilyID::SecondarySubkey), key_size,
                          score_bytes, score_bytes);
  if (!s.ok()) return s;
  return GetApproximateSizes(metadata, ns_key, storage_->GetCFHandle(ColumnFamilyID::PrimarySubkey), key_size);
}

rocksdb::Status Disk::GetBitmapSize(engine::Context &ctx, const Slice &ns_key, uint64_t *key_size) {
  BitmapMetadata metadata(false);
  rocksdb::Status s = Database::GetMetadata(ctx, {kRedisBitmap}, ns_key, &metadata);
  if (!s.ok()) return s.IsNotFound() ? rocksdb::Status::OK() : s;
  return GetApproximateSizes(metadata, ns_key, storage_->GetCFHandle(ColumnFamilyID::PrimarySubkey), key_size,
                             std::to_string(0), std::to_string(0));
}

rocksdb::Status Disk::GetSortedintSize(engine::Context &ctx, const Slice &ns_key, uint64_t *key_size) {
  SortedintMetadata metadata(false);
  rocksdb::Status s = Database::GetMetadata(ctx, {kRedisSortedint}, ns_key, &metadata);
  if (!s.ok()) return s.IsNotFound() ? rocksdb::Status::OK() : s;
  std::string start_buf;
  PutFixed64(&start_buf, 0);
  return GetApproximateSizes(metadata, ns_key, storage_->GetCFHandle(ColumnFamilyID::PrimarySubkey), key_size,
                             start_buf, start_buf);
}

rocksdb::Status Disk::GetStreamSize(engine::Context &ctx, const Slice &ns_key, uint64_t *key_size) {
  StreamMetadata metadata(false);
  rocksdb::Status s = Database::GetMetadata(ctx, {kRedisStream}, ns_key, &metadata);
  if (!s.ok()) return s.IsNotFound() ? rocksdb::Status::OK() : s;
  return GetApproximateSizes(metadata, ns_key, storage_->GetCFHandle(ColumnFamilyID::Stream), key_size);
}

}  // namespace redis
