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

#include "commander.h"
#include "error_constants.h"
#include "scope_exit.h"
#include "server/redis_connection.h"
#include "server/redis_reply.h"
#include "server/server.h"

namespace redis {

class CommandMulti : public Commander {
 public:
  Status Execute([[maybe_unused]] engine::Context &ctx, [[maybe_unused]] Server *srv, Connection *conn,
                 std::string *output) override {
    if (conn->IsFlagEnabled(Connection::kMultiExec)) {
      return {Status::RedisExecErr, "MULTI calls can not be nested"};
    }
    conn->ResetMultiExec();
    // Client starts into MULTI-EXEC
    conn->EnableFlag(Connection::kMultiExec);
    *output = redis::RESP_OK;
    return Status::OK();
  }
};

class CommandDiscard : public Commander {
 public:
  Status Execute([[maybe_unused]] engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    if (!conn->IsFlagEnabled(Connection::kMultiExec)) {
      return {Status::RedisExecErr, "DISCARD without MULTI"};
    }

    auto reset_watch = MakeScopeExit([srv, conn] { srv->ResetWatchedKeys(conn); });
    conn->ResetMultiExec();

    *output = redis::RESP_OK;

    return Status::OK();
  }
};

class CommandExec : public Commander {
 public:
  Status Execute([[maybe_unused]] engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    if (!conn->IsFlagEnabled(Connection::kMultiExec)) {
      return {Status::RedisExecErr, "EXEC without MULTI"};
    }

    auto reset_watch = MakeScopeExit([srv, conn] { srv->ResetWatchedKeys(conn); });
    auto reset_multiexec = MakeScopeExit([conn] { conn->ResetMultiExec(); });

    if (conn->IsMultiError()) {
      return {Status::RedisExecAbort, "Transaction discarded"};
    }

    if (srv->IsWatchedKeysModified(conn)) {
      *output = conn->NilString();
      return Status::OK();
    }

    auto storage = srv->storage;
    // Execute multi-exec commands
    conn->SetInExec();
    auto s = storage->BeginTxn();
    if (s.IsOK()) {
      conn->ExecuteCommands(conn->GetMultiExecCommands());
      // In Redis, errors happening after EXEC instead are not handled in a special way:
      // all the other commands will be executed even if some command fails during
      // the transaction.
      // So, if conn->IsMultiError(), the transaction should still be committed.
      s = storage->CommitTxn();
    }

    conn->ResetMultiExec();
    reset_multiexec.Disable();

    if (s) {
      conn->Reply(Array(conn->GetQueuedReplies()));
    } else {
      conn->Reply(Array(std::vector<std::string>(conn->GetQueuedReplies().size(), redis::Error(s))));
    }

    conn->ClearQueuedReplies();
    return Status::OK();
  }
};

class CommandWatch : public Commander {
 public:
  Status Execute([[maybe_unused]] engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    // If a conn is already marked as watched_keys_modified, we can skip the watch.
    if (srv->IsWatchedKeysModified(conn)) {
      *output = redis::RESP_OK;
      return Status::OK();
    }

    srv->WatchKey(conn, std::vector<std::string>(args_.begin() + 1, args_.end()));
    *output = redis::RESP_OK;
    return Status::OK();
  }
};

class CommandUnwatch : public Commander {
 public:
  Status Execute([[maybe_unused]] engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    srv->ResetWatchedKeys(conn);
    *output = redis::RESP_OK;
    return Status::OK();
  }
};

REDIS_REGISTER_COMMANDS(Txn, MakeCmdAttr<CommandMulti>("multi", 1, "bypass-multi", NO_KEY),
                        MakeCmdAttr<CommandDiscard>("discard", 1, "bypass-multi", NO_KEY),
                        MakeCmdAttr<CommandExec>("exec", 1, "exclusive bypass-multi slow", NO_KEY),
                        MakeCmdAttr<CommandWatch>("watch", -2, "no-multi", 1, -1, 1),
                        MakeCmdAttr<CommandUnwatch>("unwatch", 1, "no-multi", NO_KEY), )

}  // namespace redis
