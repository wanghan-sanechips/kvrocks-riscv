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

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "config.h"
#include "status.h"
#include "storage/batch_extractor.h"
#include "storage/redis_db.h"
#include "storage/redis_metadata.h"
#include "storage/storage.h"
#include "writer.h"

class Parser {
 public:
  explicit Parser(engine::Storage *storage, Writer *writer)
      : storage_(storage), writer_(writer), slot_id_encoded_(storage_->IsSlotIdEncoded()) {}
  ~Parser() = default;

  Status ParseFullDB();
  Status ParseWriteBatch(const std::string &batch_string);

 protected:
  engine::Storage *storage_ = nullptr;
  Writer *writer_ = nullptr;
  bool slot_id_encoded_ = false;

  Status parseSimpleKV(const Slice &ns_key, const Slice &value, uint64_t expire);
  Status parseComplexKV(const Slice &ns_key, const Metadata &metadata);
  Status parseBitmapSegment(const Slice &ns, const Slice &user_key, int index, const Slice &bitmap);
};
