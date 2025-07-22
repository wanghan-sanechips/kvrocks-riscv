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
 */

package rename

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/apache/kvrocks/tests/gocase/util"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func TestRenameString(t *testing.T) {
	srv := util.StartServer(t, map[string]string{})
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	t.Run("Rename string", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "a", "a1").Err())
		require.NoError(t, rdb.Set(ctx, "a", "hello", 0).Err())
		require.NoError(t, rdb.Rename(ctx, "a", "a1").Err())
		require.EqualValues(t, "", rdb.Get(ctx, "a").Val())
		require.EqualValues(t, "hello", rdb.Get(ctx, "a1").Val())
		require.EqualValues(t, -1, rdb.TTL(ctx, "a1").Val())

		// to-key has value
		require.NoError(t, rdb.Del(ctx, "a", "a1").Err())
		require.NoError(t, rdb.Set(ctx, "a", "hello", 0).Err())
		require.NoError(t, rdb.Set(ctx, "a1", "world", 0).Err())
		require.NoError(t, rdb.Rename(ctx, "a", "a1").Err())
		require.EqualValues(t, "", rdb.Get(ctx, "a").Val())
		require.EqualValues(t, "hello", rdb.Get(ctx, "a1").Val())
		require.EqualValues(t, -1, rdb.TTL(ctx, "a1").Val())

		// to-key has value with TTL
		require.NoError(t, rdb.Del(ctx, "a", "a1").Err())
		require.NoError(t, rdb.Set(ctx, "a", "hello", 10*time.Second).Err())
		require.NoError(t, rdb.Set(ctx, "a1", "world", 1000*time.Second).Err())
		require.NoError(t, rdb.Rename(ctx, "a", "a1").Err())
		require.EqualValues(t, "", rdb.Get(ctx, "a").Val())
		require.EqualValues(t, "hello", rdb.Get(ctx, "a1").Val())
		util.BetweenValues(t, rdb.TTL(ctx, "a1").Val(), time.Second, 10*time.Second)

		// to-key has value that not same type
		require.NoError(t, rdb.Del(ctx, "a", "a1").Err())
		require.NoError(t, rdb.Set(ctx, "a", "hello", 0).Err())
		require.NoError(t, rdb.LPush(ctx, "a1", "world").Err())
		require.NoError(t, rdb.Rename(ctx, "a", "a1").Err())
		require.EqualValues(t, "", rdb.Get(ctx, "a").Val())
		require.EqualValues(t, "hello", rdb.Get(ctx, "a1").Val())
		require.EqualValues(t, -1, rdb.TTL(ctx, "a1").Val())

		// key == newkey
		require.NoError(t, rdb.Del(ctx, "a").Err())
		require.NoError(t, rdb.Set(ctx, "a", "hello", 0).Err())
		require.NoError(t, rdb.Rename(ctx, "a", "a").Err())
		require.EqualValues(t, "hello", rdb.Get(ctx, "a").Val())

		// rename*3
		require.NoError(t, rdb.Del(ctx, "a", "a1", "a2", "a3").Err())
		require.NoError(t, rdb.Set(ctx, "a", "hello", 0).Err())
		require.NoError(t, rdb.Set(ctx, "a1", "world1", 0).Err())
		require.NoError(t, rdb.Set(ctx, "a2", "world2", 0).Err())
		require.NoError(t, rdb.Set(ctx, "a3", "world3", 0).Err())
		require.NoError(t, rdb.Rename(ctx, "a", "a1").Err())
		require.NoError(t, rdb.Rename(ctx, "a1", "a2").Err())
		require.NoError(t, rdb.Rename(ctx, "a2", "a3").Err())
		require.EqualValues(t, "", rdb.Get(ctx, "a").Val())
		require.EqualValues(t, "", rdb.Get(ctx, "a1").Val())
		require.EqualValues(t, "", rdb.Get(ctx, "a2").Val())
		require.EqualValues(t, "hello", rdb.Get(ctx, "a3").Val())
	})

	t.Run("RenameNX string", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "a", "a1").Err())
		require.NoError(t, rdb.Set(ctx, "a", "hello", 0).Err())
		require.NoError(t, rdb.Set(ctx, "a1", "world", 0).Err())
		require.EqualValues(t, false, rdb.RenameNX(ctx, "a", "a1").Val())
		require.EqualValues(t, "hello", rdb.Get(ctx, "a").Val())
		require.EqualValues(t, "world", rdb.Get(ctx, "a1").Val())

		require.NoError(t, rdb.Del(ctx, "a", "a1").Err())
		require.NoError(t, rdb.Set(ctx, "a", "hello", 0).Err())
		require.EqualValues(t, true, rdb.RenameNX(ctx, "a", "a1").Val())
		require.EqualValues(t, "", rdb.Get(ctx, "a").Val())
		require.EqualValues(t, "hello", rdb.Get(ctx, "a1").Val())

		// key == newkey
		require.NoError(t, rdb.Del(ctx, "a", "a1").Err())
		require.NoError(t, rdb.Set(ctx, "a", "hello", 0).Err())
		require.EqualValues(t, false, rdb.RenameNX(ctx, "a", "a").Val())
		require.EqualValues(t, "hello", rdb.Get(ctx, "a").Val())
	})

}

func TestRenameJSON(t *testing.T) {
	srv := util.StartServer(t, map[string]string{})
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	setCmd := "JSON.SET"
	getCmd := "JSON.GET"
	jsonA := `{"x":1,"y":2}`
	jsonB := `{"x":1}`

	t.Run("Rename json", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "a", "a1").Err())
		require.NoError(t, rdb.Do(ctx, setCmd, "a", "$", jsonA).Err())
		require.EqualValues(t, jsonA, rdb.Do(ctx, getCmd, "a").Val())
		require.NoError(t, rdb.Rename(ctx, "a", "a1").Err())
		require.EqualValues(t, nil, rdb.Do(ctx, getCmd, "a").Val())
		require.EqualValues(t, jsonA, rdb.Do(ctx, getCmd, "a1").Val())
		require.EqualValues(t, -1, rdb.TTL(ctx, "a1").Val())

		// to-key has value
		require.NoError(t, rdb.Del(ctx, "a", "a1").Err())
		require.NoError(t, rdb.Do(ctx, setCmd, "a", "$", jsonA).Err())
		require.NoError(t, rdb.Do(ctx, setCmd, "a1", "$", jsonB).Err())
		require.NoError(t, rdb.Rename(ctx, "a", "a1").Err())
		require.EqualValues(t, nil, rdb.Do(ctx, getCmd, "a").Val())
		require.EqualValues(t, jsonA, rdb.Do(ctx, getCmd, "a1").Val())
		require.EqualValues(t, -1, rdb.TTL(ctx, "a1").Val())

		// to-key has value with TTL
		require.NoError(t, rdb.Del(ctx, "a", "a1").Err())
		require.NoError(t, rdb.Do(ctx, setCmd, "a", "$", jsonA).Err())
		require.NoError(t, rdb.Expire(ctx, "a", 10*time.Second).Err())
		require.NoError(t, rdb.Do(ctx, setCmd, "a1", "$", jsonA).Err())
		require.NoError(t, rdb.Expire(ctx, "a1", 1000*time.Second).Err())
		require.NoError(t, rdb.Rename(ctx, "a", "a1").Err())
		require.EqualValues(t, nil, rdb.Do(ctx, getCmd, "a").Val())
		require.EqualValues(t, jsonA, rdb.Do(ctx, getCmd, "a1").Val())
		util.BetweenValues(t, rdb.TTL(ctx, "a1").Val(), time.Second, 10*time.Second)

		// to-key has value that not same type
		require.NoError(t, rdb.Del(ctx, "a", "a1").Err())
		require.NoError(t, rdb.Do(ctx, setCmd, "a", "$", jsonA).Err())
		require.NoError(t, rdb.LPush(ctx, "a1", "world").Err())
		require.NoError(t, rdb.Rename(ctx, "a", "a1").Err())
		require.EqualValues(t, nil, rdb.Do(ctx, getCmd, "a").Val())
		require.EqualValues(t, jsonA, rdb.Do(ctx, getCmd, "a1").Val())
		require.EqualValues(t, -1, rdb.TTL(ctx, "a1").Val())

		// key == newkey
		require.NoError(t, rdb.Del(ctx, "a").Err())
		require.NoError(t, rdb.Do(ctx, setCmd, "a", "$", jsonA).Err())
		require.NoError(t, rdb.Rename(ctx, "a", "a").Err())
		require.EqualValues(t, jsonA, rdb.Do(ctx, getCmd, "a").Val())

		// rename*3
		require.NoError(t, rdb.Del(ctx, "a", "a1", "a2", "a3").Err())
		require.NoError(t, rdb.Do(ctx, setCmd, "a", "$", jsonA).Err())
		require.NoError(t, rdb.Do(ctx, setCmd, "a1", "$", jsonB).Err())
		require.NoError(t, rdb.Do(ctx, setCmd, "a2", "$", jsonB).Err())
		require.NoError(t, rdb.Do(ctx, setCmd, "a3", "$", jsonB).Err())
		require.NoError(t, rdb.Rename(ctx, "a", "a1").Err())
		require.NoError(t, rdb.Rename(ctx, "a1", "a2").Err())
		require.NoError(t, rdb.Rename(ctx, "a2", "a3").Err())
		require.EqualValues(t, nil, rdb.Do(ctx, getCmd, "a").Val())
		require.EqualValues(t, nil, rdb.Do(ctx, getCmd, "a1").Val())
		require.EqualValues(t, nil, rdb.Do(ctx, getCmd, "a2").Val())
		require.EqualValues(t, jsonA, rdb.Do(ctx, getCmd, "a3").Val())
	})

	t.Run("RenameNX json", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "a", "a1").Err())
		require.NoError(t, rdb.Do(ctx, setCmd, "a", "$", jsonA).Err())
		require.NoError(t, rdb.Do(ctx, setCmd, "a1", "$", jsonB).Err())
		require.EqualValues(t, false, rdb.RenameNX(ctx, "a", "a1").Val())
		require.EqualValues(t, jsonA, rdb.Do(ctx, getCmd, "a").Val())
		require.EqualValues(t, jsonB, rdb.Do(ctx, getCmd, "a1").Val())

		require.NoError(t, rdb.Del(ctx, "a", "a1").Err())
		require.NoError(t, rdb.Do(ctx, setCmd, "a", "$", jsonA).Err())
		require.EqualValues(t, true, rdb.RenameNX(ctx, "a", "a1").Val())
		require.EqualValues(t, nil, rdb.Do(ctx, getCmd, "a").Val())
		require.EqualValues(t, jsonA, rdb.Do(ctx, getCmd, "a1").Val())

		// key == newkey
		require.NoError(t, rdb.Del(ctx, "a", "a1").Err())
		require.NoError(t, rdb.Do(ctx, setCmd, "a", "$", jsonA).Err())
		require.EqualValues(t, false, rdb.RenameNX(ctx, "a", "a").Val())
		require.EqualValues(t, jsonA, rdb.Do(ctx, getCmd, "a").Val())
	})

}

func TestRenameList(t *testing.T) {
	srv := util.StartServer(t, map[string]string{})
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	EqualListValues := func(t *testing.T, key string, value []string) {
		require.EqualValues(t, len(value), rdb.LLen(ctx, key).Val())
		for i := 0; i < len(value); i++ {
			require.EqualValues(t, value[i], rdb.LIndex(ctx, key, int64(i)).Val())
		}
	}

	t.Run("Rename string", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "a", "a1").Err())
		require.NoError(t, rdb.LPush(ctx, "a", "1", "2", "3").Err())
		require.NoError(t, rdb.Rename(ctx, "a", "a1").Err())
		require.EqualValues(t, "", rdb.Get(ctx, "a").Val())
		require.EqualValues(t, 3, rdb.LLen(ctx, "a1").Val())
		EqualListValues(t, "a1", []string{"3", "2", "1"})
		require.EqualValues(t, -1, rdb.TTL(ctx, "a1").Val())

		// to-key has value
		require.NoError(t, rdb.Del(ctx, "a", "a1").Err())
		require.NoError(t, rdb.LPush(ctx, "a", "1", "2", "3").Err())
		require.NoError(t, rdb.LPush(ctx, "a1", "a").Err())
		require.NoError(t, rdb.Rename(ctx, "a", "a1").Err())
		require.EqualValues(t, "", rdb.Get(ctx, "a").Val())
		EqualListValues(t, "a1", []string{"3", "2", "1"})
		require.EqualValues(t, -1, rdb.TTL(ctx, "a1").Val())

		// to-key has value with TTL
		require.NoError(t, rdb.Del(ctx, "a", "a1").Err())
		require.NoError(t, rdb.LPush(ctx, "a", "1", "2", "3").Err())
		require.NoError(t, rdb.Expire(ctx, "a", 10*time.Second).Err())
		require.NoError(t, rdb.LPush(ctx, "a1", "a").Err())
		require.NoError(t, rdb.Expire(ctx, "a1", 1000*time.Second).Err())
		require.NoError(t, rdb.Rename(ctx, "a", "a1").Err())
		require.EqualValues(t, "", rdb.Get(ctx, "a").Val())
		EqualListValues(t, "a1", []string{"3", "2", "1"})
		util.BetweenValues(t, rdb.TTL(ctx, "a1").Val(), time.Second, 10*time.Second)

		// to-key has value that not same type
		require.NoError(t, rdb.Del(ctx, "a", "a1").Err())
		require.NoError(t, rdb.LPush(ctx, "a", "1", "2", "3").Err())
		require.NoError(t, rdb.Set(ctx, "a1", "world", 0).Err())
		require.NoError(t, rdb.Rename(ctx, "a", "a1").Err())
		require.EqualValues(t, "", rdb.Get(ctx, "a").Val())
		EqualListValues(t, "a1", []string{"3", "2", "1"})
		require.EqualValues(t, -1, rdb.TTL(ctx, "a1").Val())

		// key == newkey
		require.NoError(t, rdb.Del(ctx, "a").Err())
		require.NoError(t, rdb.LPush(ctx, "a", "1", "2", "3").Err())
		require.NoError(t, rdb.Rename(ctx, "a", "a").Err())
		EqualListValues(t, "a1", []string{"3", "2", "1"})

		// rename*3
		require.NoError(t, rdb.Del(ctx, "a", "a1", "a2", "a3").Err())
		require.NoError(t, rdb.LPush(ctx, "a", "1", "2", "3").Err())
		require.NoError(t, rdb.LPush(ctx, "a1", "2").Err())
		require.NoError(t, rdb.LPush(ctx, "a2", "3").Err())
		require.NoError(t, rdb.LPush(ctx, "a3", "1").Err())
		require.NoError(t, rdb.Rename(ctx, "a", "a1").Err())
		require.NoError(t, rdb.Rename(ctx, "a1", "a2").Err())
		require.NoError(t, rdb.Rename(ctx, "a2", "a3").Err())
		require.EqualValues(t, "", rdb.Get(ctx, "a").Val())
		require.EqualValues(t, "", rdb.Get(ctx, "a1").Val())
		require.EqualValues(t, "", rdb.Get(ctx, "a2").Val())
		EqualListValues(t, "a3", []string{"3", "2", "1"})
	})

	t.Run("RenameNX string", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "a", "a1").Err())
		require.NoError(t, rdb.LPush(ctx, "a", "1", "2", "3").Err())
		require.NoError(t, rdb.LPush(ctx, "a1", "3").Err())
		require.EqualValues(t, false, rdb.RenameNX(ctx, "a", "a1").Val())
		EqualListValues(t, "a", []string{"3", "2", "1"})
		EqualListValues(t, "a1", []string{"3"})

		require.NoError(t, rdb.Del(ctx, "a", "a1").Err())
		require.NoError(t, rdb.LPush(ctx, "a", "1", "2", "3").Err())
		require.EqualValues(t, true, rdb.RenameNX(ctx, "a", "a1").Val())
		EqualListValues(t, "a1", []string{"3", "2", "1"})
		require.EqualValues(t, "", rdb.Get(ctx, "a").Val())

		// key == newkey
		require.NoError(t, rdb.Del(ctx, "a", "a1").Err())
		require.NoError(t, rdb.LPush(ctx, "a", "1", "2", "3").Err())
		require.EqualValues(t, false, rdb.RenameNX(ctx, "a", "a").Val())
		EqualListValues(t, "a", []string{"3", "2", "1"})
	})

}

func TestRenameHash(t *testing.T) {
	srv := util.StartServer(t, map[string]string{})
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	EqualListValues := func(t *testing.T, key string, value map[string]string) {
		require.EqualValues(t, len(value), rdb.HLen(ctx, key).Val())
		for subKey := range value {
			require.EqualValues(t, value[subKey], rdb.HGet(ctx, key, subKey).Val())
		}
	}

	t.Run("Rename hash", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "a", "a1").Err())
		require.NoError(t, rdb.HSet(ctx, "a", "a", "1", "b", "2", "c", "3").Err())
		require.NoError(t, rdb.Rename(ctx, "a", "a1").Err())
		require.EqualValues(t, "", rdb.Get(ctx, "a").Val())
		EqualListValues(t, "a1", map[string]string{
			"a": "1",
			"b": "2",
			"c": "3",
		})
		require.EqualValues(t, -1, rdb.TTL(ctx, "a1").Val())

		// to-key has value
		require.NoError(t, rdb.Del(ctx, "a", "a1").Err())
		require.NoError(t, rdb.HSet(ctx, "a", "a", "1", "b", "2", "c", "3").Err())
		require.NoError(t, rdb.HSet(ctx, "a1", "a", "1").Err())
		require.NoError(t, rdb.Rename(ctx, "a", "a1").Err())
		require.EqualValues(t, "", rdb.Get(ctx, "a").Val())
		EqualListValues(t, "a1", map[string]string{
			"a": "1",
			"b": "2",
			"c": "3",
		})
		require.EqualValues(t, -1, rdb.TTL(ctx, "a1").Val())

		// to-key has value with TTL
		require.NoError(t, rdb.Del(ctx, "a", "a1").Err())
		require.NoError(t, rdb.HSet(ctx, "a", "a", "1", "b", "2", "c", "3").Err())
		require.NoError(t, rdb.Expire(ctx, "a", 10*time.Second).Err())
		require.NoError(t, rdb.HSet(ctx, "a1", "a", "1").Err())
		require.NoError(t, rdb.Expire(ctx, "a1", 1000*time.Second).Err())
		require.NoError(t, rdb.Rename(ctx, "a", "a1").Err())
		require.EqualValues(t, "", rdb.Get(ctx, "a").Val())
		EqualListValues(t, "a1", map[string]string{
			"a": "1",
			"b": "2",
			"c": "3",
		})
		util.BetweenValues(t, rdb.TTL(ctx, "a1").Val(), time.Second, 10*time.Second)

		// to-key has value that not same type
		require.NoError(t, rdb.Del(ctx, "a", "a1").Err())
		require.NoError(t, rdb.HSet(ctx, "a", "a", "1", "b", "2", "c", "3").Err())
		require.NoError(t, rdb.LPush(ctx, "a1", "a", "1").Err())
		require.NoError(t, rdb.Rename(ctx, "a", "a1").Err())
		require.EqualValues(t, "", rdb.Get(ctx, "a").Val())
		EqualListValues(t, "a1", map[string]string{
			"a": "1",
			"b": "2",
			"c": "3",
		})
		require.EqualValues(t, -1, rdb.TTL(ctx, "a1").Val())

		// key == newkey
		require.NoError(t, rdb.Del(ctx, "a").Err())
		require.NoError(t, rdb.HSet(ctx, "a", "a", "1", "b", "2", "c", "3").Err())
		require.NoError(t, rdb.Rename(ctx, "a", "a").Err())
		EqualListValues(t, "a1", map[string]string{
			"a": "1",
			"b": "2",
			"c": "3",
		})

		// rename*3
		require.NoError(t, rdb.Del(ctx, "a", "a1", "a2", "a3").Err())
		require.NoError(t, rdb.HSet(ctx, "a", "a", "1", "b", "2", "c", "3").Err())
		require.NoError(t, rdb.HSet(ctx, "a1", "a", "1").Err())
		require.NoError(t, rdb.HSet(ctx, "a2", "a", "1").Err())
		require.NoError(t, rdb.HSet(ctx, "a3", "a", "1").Err())
		require.NoError(t, rdb.Rename(ctx, "a", "a1").Err())
		require.NoError(t, rdb.Rename(ctx, "a1", "a2").Err())
		require.NoError(t, rdb.Rename(ctx, "a2", "a3").Err())
		require.EqualValues(t, "", rdb.Get(ctx, "a").Val())
		require.EqualValues(t, "", rdb.Get(ctx, "a1").Val())
		require.EqualValues(t, "", rdb.Get(ctx, "a2").Val())
		EqualListValues(t, "a3", map[string]string{
			"a": "1",
			"b": "2",
			"c": "3",
		})
	})

	t.Run("RenameNX hash", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "a", "a1").Err())
		require.NoError(t, rdb.HSet(ctx, "a", "a", "1", "b", "2", "c", "3").Err())
		require.NoError(t, rdb.HSet(ctx, "a1", "a", "1").Err())
		require.EqualValues(t, false, rdb.RenameNX(ctx, "a", "a1").Val())
		EqualListValues(t, "a", map[string]string{
			"a": "1",
			"b": "2",
			"c": "3",
		})
		EqualListValues(t, "a1", map[string]string{
			"a": "1",
		})

		require.NoError(t, rdb.Del(ctx, "a", "a1").Err())
		require.NoError(t, rdb.HSet(ctx, "a", "a", "1", "b", "2", "c", "3").Err())
		require.EqualValues(t, true, rdb.RenameNX(ctx, "a", "a1").Val())
		EqualListValues(t, "a1", map[string]string{
			"a": "1",
			"b": "2",
			"c": "3",
		})
		require.EqualValues(t, "", rdb.Get(ctx, "a").Val())

		// key == newkey
		require.NoError(t, rdb.Del(ctx, "a").Err())
		require.NoError(t, rdb.HSet(ctx, "a", "a", "1", "b", "2", "c", "3").Err())
		require.EqualValues(t, false, rdb.RenameNX(ctx, "a", "a").Val())
		EqualListValues(t, "a", map[string]string{
			"a": "1",
			"b": "2",
			"c": "3",
		})
	})

}

func TestRenameSet(t *testing.T) {
	srv := util.StartServer(t, map[string]string{})
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	EqualSetValues := func(t *testing.T, key string, value []string) {
		require.EqualValues(t, len(value), rdb.SCard(ctx, key).Val())
		for index := range value {
			require.EqualValues(t, true, rdb.SIsMember(ctx, key, value[index]).Val())
		}
	}

	t.Run("Rename set", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "a", "a1").Err())
		require.NoError(t, rdb.SAdd(ctx, "a", "1", "2", "3").Err())
		require.NoError(t, rdb.Rename(ctx, "a", "a1").Err())
		require.EqualValues(t, "", rdb.Get(ctx, "a").Val())
		EqualSetValues(t, "a1", []string{"1", "2", "3"})
		require.EqualValues(t, -1, rdb.TTL(ctx, "a1").Val())

		// to-key has value
		require.NoError(t, rdb.Del(ctx, "a", "a1").Err())
		require.NoError(t, rdb.SAdd(ctx, "a", "1", "2", "3").Err())
		require.NoError(t, rdb.SAdd(ctx, "a1", "a", "1").Err())
		require.NoError(t, rdb.Rename(ctx, "a", "a1").Err())
		require.EqualValues(t, "", rdb.Get(ctx, "a").Val())
		EqualSetValues(t, "a1", []string{"1", "2", "3"})
		require.EqualValues(t, -1, rdb.TTL(ctx, "a1").Val())

		// to-key has value with TTL
		require.NoError(t, rdb.Del(ctx, "a", "a1").Err())
		require.NoError(t, rdb.SAdd(ctx, "a", "1", "2", "3").Err())
		require.NoError(t, rdb.Expire(ctx, "a", 10*time.Second).Err())
		require.NoError(t, rdb.SAdd(ctx, "a1", "1").Err())
		require.NoError(t, rdb.Expire(ctx, "a1", 1000*time.Second).Err())
		require.NoError(t, rdb.Rename(ctx, "a", "a1").Err())
		require.EqualValues(t, "", rdb.Get(ctx, "a").Val())
		EqualSetValues(t, "a1", []string{"1", "2", "3"})
		util.BetweenValues(t, rdb.TTL(ctx, "a1").Val(), time.Second, 10*time.Second)

		// to-key has value that not same type
		require.NoError(t, rdb.Del(ctx, "a", "a1").Err())
		require.NoError(t, rdb.SAdd(ctx, "a", "1", "2", "3").Err())
		require.NoError(t, rdb.LPush(ctx, "a1", "1").Err())
		require.NoError(t, rdb.Rename(ctx, "a", "a1").Err())
		require.EqualValues(t, "", rdb.Get(ctx, "a").Val())
		EqualSetValues(t, "a1", []string{"1", "2", "3"})
		require.EqualValues(t, -1, rdb.TTL(ctx, "a1").Val())

		// key == newkey
		require.NoError(t, rdb.Del(ctx, "a").Err())
		require.NoError(t, rdb.SAdd(ctx, "a", "1", "2", "3").Err())
		require.NoError(t, rdb.Rename(ctx, "a", "a").Err())
		EqualSetValues(t, "a1", []string{"1", "2", "3"})

		// rename*3
		require.NoError(t, rdb.Del(ctx, "a", "a1", "a2", "a3").Err())
		require.NoError(t, rdb.SAdd(ctx, "a", "1", "2", "3").Err())
		require.NoError(t, rdb.SAdd(ctx, "a1", "1").Err())
		require.NoError(t, rdb.SAdd(ctx, "a2", "a2", "1").Err())
		require.NoError(t, rdb.SAdd(ctx, "a3", "a3", "1").Err())
		require.NoError(t, rdb.Rename(ctx, "a", "a1").Err())
		require.NoError(t, rdb.Rename(ctx, "a1", "a2").Err())
		require.NoError(t, rdb.Rename(ctx, "a2", "a3").Err())
		require.EqualValues(t, "", rdb.Get(ctx, "a").Val())
		require.EqualValues(t, "", rdb.Get(ctx, "a1").Val())
		require.EqualValues(t, "", rdb.Get(ctx, "a2").Val())
		EqualSetValues(t, "a3", []string{"1", "2", "3"})
	})

	t.Run("RenameNX set", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "a", "a1").Err())
		require.NoError(t, rdb.SAdd(ctx, "a", "1", "2", "3").Err())
		require.NoError(t, rdb.SAdd(ctx, "a1", "1").Err())
		require.EqualValues(t, false, rdb.RenameNX(ctx, "a", "a1").Val())
		EqualSetValues(t, "a", []string{"1", "2", "3"})
		EqualSetValues(t, "a1", []string{"1"})

		require.NoError(t, rdb.Del(ctx, "a", "a1").Err())
		require.NoError(t, rdb.SAdd(ctx, "a", "1", "2", "3").Err())
		require.EqualValues(t, true, rdb.RenameNX(ctx, "a", "a1").Val())
		EqualSetValues(t, "a1", []string{"1", "2", "3"})
		require.EqualValues(t, "", rdb.Get(ctx, "a").Val())

		// key == newkey
		require.NoError(t, rdb.Del(ctx, "a").Err())
		require.NoError(t, rdb.SAdd(ctx, "a", "1", "2", "3").Err())
		require.EqualValues(t, false, rdb.RenameNX(ctx, "a", "a").Val())
		EqualSetValues(t, "a", []string{"1", "2", "3"})

	})

}

func TestRenameZset(t *testing.T) {
	srv := util.StartServer(t, map[string]string{})
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	EqualZSetValues := func(t *testing.T, key string, value map[string]int) {
		require.EqualValues(t, len(value), rdb.ZCard(ctx, key).Val())
		for subKey := range value {
			score := value[subKey]
			require.EqualValues(t, []string{subKey}, rdb.ZRangeByScore(ctx, key,
				&redis.ZRangeBy{Max: strconv.Itoa(score), Min: strconv.Itoa(score)}).Val())
			require.EqualValues(t, float64(score), rdb.ZScore(ctx, key, subKey).Val())
		}
	}

	zMember := []redis.Z{{Member: "a", Score: 1}, {Member: "b", Score: 2}, {Member: "c", Score: 3}}
	zMember2 := []redis.Z{{Member: "a", Score: 2}}

	t.Run("Rename zset", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "a", "a1").Err())
		require.NoError(t, rdb.ZAdd(ctx, "a", zMember...).Err())
		require.NoError(t, rdb.Rename(ctx, "a", "a1").Err())
		require.EqualValues(t, "", rdb.Get(ctx, "a").Val())
		EqualZSetValues(t, "a1", map[string]int{
			"a": 1,
			"b": 2,
			"c": 3,
		})
		require.EqualValues(t, -1, rdb.TTL(ctx, "a1").Val())

		// to-key has value
		require.NoError(t, rdb.Del(ctx, "a", "a1").Err())
		require.NoError(t, rdb.ZAdd(ctx, "a", zMember...).Err())
		require.NoError(t, rdb.ZAdd(ctx, "a1", zMember2...).Err())
		require.NoError(t, rdb.Rename(ctx, "a", "a1").Err())
		require.EqualValues(t, "", rdb.Get(ctx, "a").Val())
		EqualZSetValues(t, "a1", map[string]int{
			"a": 1,
			"b": 2,
			"c": 3,
		})
		require.EqualValues(t, -1, rdb.TTL(ctx, "a1").Val())

		// to-key has value with TTL
		require.NoError(t, rdb.Del(ctx, "a", "a1").Err())
		require.NoError(t, rdb.ZAdd(ctx, "a", zMember...).Err())
		require.NoError(t, rdb.Expire(ctx, "a", 10*time.Second).Err())
		require.NoError(t, rdb.ZAdd(ctx, "a1", zMember2...).Err())
		require.NoError(t, rdb.Expire(ctx, "a1", 1000*time.Second).Err())
		require.NoError(t, rdb.Rename(ctx, "a", "a1").Err())
		require.EqualValues(t, "", rdb.Get(ctx, "a").Val())
		EqualZSetValues(t, "a1", map[string]int{
			"a": 1,
			"b": 2,
			"c": 3,
		})
		util.BetweenValues(t, rdb.TTL(ctx, "a1").Val(), time.Second, 10*time.Second)

		// to-key has value that not same type
		require.NoError(t, rdb.Del(ctx, "a", "a1").Err())
		require.NoError(t, rdb.ZAdd(ctx, "a", zMember...).Err())
		require.NoError(t, rdb.LPush(ctx, "a1", 1, 2, 3).Err())
		require.NoError(t, rdb.Rename(ctx, "a", "a1").Err())
		require.EqualValues(t, "", rdb.Get(ctx, "a").Val())
		EqualZSetValues(t, "a1", map[string]int{
			"a": 1,
			"b": 2,
			"c": 3,
		})
		require.EqualValues(t, -1, rdb.TTL(ctx, "a1").Val())

		// key == newkey
		require.NoError(t, rdb.Del(ctx, "a").Err())
		require.NoError(t, rdb.ZAdd(ctx, "a", zMember...).Err())
		require.NoError(t, rdb.Rename(ctx, "a", "a").Err())
		EqualZSetValues(t, "a1", map[string]int{
			"a": 1,
			"b": 2,
			"c": 3,
		})
		// rename*3
		require.NoError(t, rdb.Del(ctx, "a", "a1", "a2", "a3").Err())
		require.NoError(t, rdb.ZAdd(ctx, "a", zMember...).Err())
		require.NoError(t, rdb.ZAdd(ctx, "a1", zMember2...).Err())
		require.NoError(t, rdb.ZAdd(ctx, "a2", zMember2...).Err())
		require.NoError(t, rdb.ZAdd(ctx, "a3", zMember2...).Err())
		require.NoError(t, rdb.Rename(ctx, "a", "a1").Err())
		require.NoError(t, rdb.Rename(ctx, "a1", "a2").Err())
		require.NoError(t, rdb.Rename(ctx, "a2", "a3").Err())
		require.EqualValues(t, "", rdb.Get(ctx, "a").Val())
		require.EqualValues(t, "", rdb.Get(ctx, "a1").Val())
		require.EqualValues(t, "", rdb.Get(ctx, "a2").Val())
		EqualZSetValues(t, "a3", map[string]int{
			"a": 1,
			"b": 2,
			"c": 3,
		})

	})

	t.Run("RenameNX zset", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "a", "a1").Err())
		require.NoError(t, rdb.ZAdd(ctx, "a", zMember...).Err())
		require.NoError(t, rdb.ZAdd(ctx, "a1", zMember2...).Err())
		require.EqualValues(t, false, rdb.RenameNX(ctx, "a", "a1").Val())
		EqualZSetValues(t, "a", map[string]int{
			"a": 1,
			"b": 2,
			"c": 3,
		})
		EqualZSetValues(t, "a1", map[string]int{
			"a": 2,
		})

		require.NoError(t, rdb.Del(ctx, "a", "a1").Err())
		require.NoError(t, rdb.ZAdd(ctx, "a", zMember...).Err())
		require.EqualValues(t, true, rdb.RenameNX(ctx, "a", "a1").Val())
		EqualZSetValues(t, "a1", map[string]int{
			"a": 1,
			"b": 2,
			"c": 3,
		})
		require.EqualValues(t, "", rdb.Get(ctx, "a").Val())

		// key == newkey
		require.NoError(t, rdb.Del(ctx, "a").Err())
		require.NoError(t, rdb.ZAdd(ctx, "a", zMember...).Err())
		require.EqualValues(t, false, rdb.RenameNX(ctx, "a", "a").Val())
		EqualZSetValues(t, "a", map[string]int{
			"a": 1,
			"b": 2,
			"c": 3,
		})

	})

}

func TestRenameBitmap(t *testing.T) {
	srv := util.StartServer(t, map[string]string{})
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	EqualBitSetValues := func(t *testing.T, key string, value []int64) {
		for i := 0; i < len(value); i++ {
			require.EqualValues(t, int64(value[i]), rdb.Do(ctx, "BITPOS", key, 1, value[i]/8).Val())
		}
	}

	SetBits := func(t *testing.T, key string, value []int64) {
		for i := 0; i < len(value); i++ {
			require.NoError(t, rdb.Do(ctx, "SETBIT", key, value[i], 1).Err())
		}
	}
	bitSetA := []int64{16, 1024 * 8 * 2, 1024 * 8 * 12}
	bitSetB := []int64{1}

	t.Run("Rename Bitmap", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "a", "a1").Err())
		SetBits(t, "a", bitSetA)
		require.NoError(t, rdb.Rename(ctx, "a", "a1").Err())
		require.EqualValues(t, "", rdb.Get(ctx, "a").Val())
		EqualBitSetValues(t, "a1", bitSetA)
		require.EqualValues(t, -1, rdb.TTL(ctx, "a1").Val())

		// newkey has value with TTL
		require.NoError(t, rdb.Del(ctx, "a", "a1").Err())
		SetBits(t, "a", bitSetA)
		SetBits(t, "a1", bitSetB)
		require.NoError(t, rdb.Expire(ctx, "a", 10*time.Second).Err())
		require.NoError(t, rdb.Expire(ctx, "a1", 1000*time.Second).Err())
		require.NoError(t, rdb.Rename(ctx, "a", "a1").Err())
		require.EqualValues(t, "", rdb.Get(ctx, "a").Val())
		EqualBitSetValues(t, "a1", bitSetA)
		util.BetweenValues(t, rdb.TTL(ctx, "a1").Val(), time.Second, 10*time.Second)

		// newkey has value that not same type
		require.NoError(t, rdb.Del(ctx, "a", "a1").Err())
		SetBits(t, "a", bitSetA)
		require.NoError(t, rdb.LPush(ctx, "a1", "a").Err())
		require.NoError(t, rdb.Rename(ctx, "a", "a1").Err())
		require.EqualValues(t, "", rdb.Get(ctx, "a").Val())
		EqualBitSetValues(t, "a1", bitSetA)
		require.EqualValues(t, -1, rdb.TTL(ctx, "a1").Val())

		// key == newkey
		require.NoError(t, rdb.Del(ctx, "a").Err())
		SetBits(t, "a", bitSetA)
		require.NoError(t, rdb.Rename(ctx, "a", "a").Err())
		EqualBitSetValues(t, "a", bitSetA)

		// rename*3
		require.NoError(t, rdb.Del(ctx, "a", "a1", "a2", "a3").Err())
		SetBits(t, "a", bitSetA)
		SetBits(t, "a1", bitSetB)
		SetBits(t, "a2", bitSetB)
		SetBits(t, "a3", bitSetB)
		require.NoError(t, rdb.Rename(ctx, "a", "a1").Err())
		require.NoError(t, rdb.Rename(ctx, "a1", "a2").Err())
		require.NoError(t, rdb.Rename(ctx, "a2", "a3").Err())
		require.EqualValues(t, "", rdb.Get(ctx, "a").Val())
		require.EqualValues(t, "", rdb.Get(ctx, "a1").Val())
		require.EqualValues(t, "", rdb.Get(ctx, "a2").Val())
		EqualBitSetValues(t, "a3", bitSetA)
	})

	t.Run("RenameNX Bitmap", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "a", "a1").Err())
		SetBits(t, "a", bitSetA)
		SetBits(t, "a1", bitSetB)
		require.EqualValues(t, false, rdb.RenameNX(ctx, "a", "a1").Val())
		EqualBitSetValues(t, "a", bitSetA)
		EqualBitSetValues(t, "a1", bitSetB)

		require.NoError(t, rdb.Del(ctx, "a", "a1").Err())
		SetBits(t, "a", bitSetA)
		require.EqualValues(t, true, rdb.RenameNX(ctx, "a", "a1").Val())
		EqualBitSetValues(t, "a1", bitSetA)
		require.EqualValues(t, "", rdb.Get(ctx, "a").Val())

		// key == newkey
		require.NoError(t, rdb.Del(ctx, "a", "a1").Err())
		SetBits(t, "a", bitSetA)
		require.EqualValues(t, false, rdb.RenameNX(ctx, "a", "a").Val())
		EqualBitSetValues(t, "a", bitSetA)

	})

}

func TestRenameSint(t *testing.T) {
	srv := util.StartServer(t, map[string]string{})
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	EqualSIntValues := func(t *testing.T, key string, value []int) {
		require.EqualValues(t, len(value), rdb.Do(ctx, "SICARD", key).Val())
		for i := 0; i < len(value); i++ {
			require.EqualValues(t, []interface{}{int64(1)}, rdb.Do(ctx, "SIEXISTS", key, value[i]).Val())
		}
	}

	t.Run("Rename SInt", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "a", "a1").Err())
		require.NoError(t, rdb.Do(ctx, "SIADD", "a", 3, 4, 5, 123, 245).Err())
		require.NoError(t, rdb.Rename(ctx, "a", "a1").Err())
		require.EqualValues(t, "", rdb.Get(ctx, "a").Val())
		EqualSIntValues(t, "a1", []int{3, 4, 5, 123, 245})
		require.EqualValues(t, -1, rdb.TTL(ctx, "a1").Val())

		// to-key has value with TTL
		require.NoError(t, rdb.Del(ctx, "a", "a1").Err())
		require.NoError(t, rdb.Do(ctx, "SIADD", "a", 3, 4, 5, 123, 245).Err())
		require.NoError(t, rdb.Expire(ctx, "a", 10*time.Second).Err())
		require.NoError(t, rdb.Do(ctx, "SIADD", "a1", 99).Err())
		require.NoError(t, rdb.Expire(ctx, "a1", 1000*time.Second).Err())
		require.NoError(t, rdb.Rename(ctx, "a", "a1").Err())
		require.EqualValues(t, "", rdb.Get(ctx, "a").Val())
		EqualSIntValues(t, "a1", []int{3, 4, 5, 123, 245})
		util.BetweenValues(t, rdb.TTL(ctx, "a1").Val(), time.Second, 10*time.Second)

		// to-key has value that not same type
		require.NoError(t, rdb.Del(ctx, "a", "a1").Err())
		require.NoError(t, rdb.Do(ctx, "SIADD", "a", 3, 4, 5, 123, 245).Err())
		require.NoError(t, rdb.LPush(ctx, "a1", "a").Err())
		require.NoError(t, rdb.Rename(ctx, "a", "a1").Err())
		require.EqualValues(t, "", rdb.Get(ctx, "a").Val())
		EqualSIntValues(t, "a1", []int{3, 4, 5, 123, 245})
		require.EqualValues(t, -1, rdb.TTL(ctx, "a1").Val())

		// key == newkey
		require.NoError(t, rdb.Del(ctx, "a").Err())
		require.NoError(t, rdb.Do(ctx, "SIADD", "a", 3, 4, 5, 123, 245).Err())
		require.NoError(t, rdb.Rename(ctx, "a", "a").Err())
		EqualSIntValues(t, "a1", []int{3, 4, 5, 123, 245})

		// rename*3
		require.NoError(t, rdb.Del(ctx, "a", "a1", "a2", "a3").Err())
		require.NoError(t, rdb.Do(ctx, "SIADD", "a", 3, 4, 5, 123, 245).Err())
		require.NoError(t, rdb.Do(ctx, "SIADD", "a1", 85).Err())
		require.NoError(t, rdb.Do(ctx, "SIADD", "a2", 77, 0).Err())
		require.NoError(t, rdb.Do(ctx, "SIADD", "a3", 111, 222, 333).Err())
		require.NoError(t, rdb.Rename(ctx, "a", "a1").Err())
		require.NoError(t, rdb.Rename(ctx, "a1", "a2").Err())
		require.NoError(t, rdb.Rename(ctx, "a2", "a3").Err())
		require.EqualValues(t, "", rdb.Get(ctx, "a").Val())
		require.EqualValues(t, "", rdb.Get(ctx, "a1").Val())
		require.EqualValues(t, "", rdb.Get(ctx, "a2").Val())
		EqualSIntValues(t, "a3", []int{3, 4, 5, 123, 245})
	})

	t.Run("RenameNX SInt", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "a", "a1").Err())
		require.NoError(t, rdb.Do(ctx, "SIADD", "a", 3, 4, 5, 123, 245).Err())
		require.NoError(t, rdb.Do(ctx, "SIADD", "a1", 99).Err())
		require.EqualValues(t, false, rdb.RenameNX(ctx, "a", "a1").Val())
		EqualSIntValues(t, "a", []int{3, 4, 5, 123, 245})
		EqualSIntValues(t, "a1", []int{99})

		require.NoError(t, rdb.Del(ctx, "a", "a1").Err())
		require.NoError(t, rdb.Do(ctx, "SIADD", "a", 3, 4, 5, 123, 245).Err())
		require.EqualValues(t, true, rdb.RenameNX(ctx, "a", "a1").Val())
		EqualSIntValues(t, "a1", []int{3, 4, 5, 123, 245})
		require.EqualValues(t, "", rdb.Get(ctx, "a").Val())

		// key == newkey
		require.NoError(t, rdb.Del(ctx, "a", "a1").Err())
		require.NoError(t, rdb.Do(ctx, "SIADD", "a", 3, 4, 5, 123, 245).Err())
		require.EqualValues(t, false, rdb.RenameNX(ctx, "a", "a").Val())
		EqualSIntValues(t, "a", []int{3, 4, 5, 123, 245})

	})

}

func TestRenameBloom(t *testing.T) {
	srv := util.StartServer(t, map[string]string{})
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	bfAdd := "BF.ADD"
	bfExists := "BF.EXISTS"

	t.Run("Rename Bloom", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "a", "a1").Err())
		require.NoError(t, rdb.Do(ctx, bfAdd, "a", "hello").Err())
		require.NoError(t, rdb.Rename(ctx, "a", "a1").Err())
		require.EqualValues(t, "", rdb.Get(ctx, "a").Val())
		require.EqualValues(t, 1, rdb.Do(ctx, bfExists, "a1", "hello").Val())
		require.EqualValues(t, -1, rdb.TTL(ctx, "a1").Val())

		// to-key has value with TTL
		require.NoError(t, rdb.Del(ctx, "a", "a1").Err())
		require.NoError(t, rdb.Do(ctx, bfAdd, "a", "hello").Err())
		require.NoError(t, rdb.Expire(ctx, "a", 10*time.Second).Err())
		require.NoError(t, rdb.Do(ctx, bfAdd, "a1", "world").Err())
		require.NoError(t, rdb.Expire(ctx, "a1", 1000*time.Second).Err())
		require.NoError(t, rdb.Rename(ctx, "a", "a1").Err())
		require.EqualValues(t, "", rdb.Get(ctx, "a").Val())
		require.EqualValues(t, 1, rdb.Do(ctx, bfExists, "a1", "hello").Val())
		require.EqualValues(t, 0, rdb.Do(ctx, bfExists, "a1", "world").Val())
		util.BetweenValues(t, rdb.TTL(ctx, "a1").Val(), time.Second, 10*time.Second)

		// to-key has value that not same type
		require.NoError(t, rdb.Del(ctx, "a", "a1").Err())
		require.NoError(t, rdb.Do(ctx, bfAdd, "a", "hello").Err())
		require.NoError(t, rdb.LPush(ctx, "a1", "a").Err())
		require.NoError(t, rdb.Rename(ctx, "a", "a1").Err())
		require.EqualValues(t, "", rdb.Get(ctx, "a").Val())
		require.EqualValues(t, 1, rdb.Do(ctx, bfExists, "a1", "hello").Val())
		require.EqualValues(t, -1, rdb.TTL(ctx, "a1").Val())

		// key == newkey
		require.NoError(t, rdb.Del(ctx, "a").Err())
		require.NoError(t, rdb.Do(ctx, bfAdd, "a", "hello").Err())
		require.NoError(t, rdb.Rename(ctx, "a", "a").Err())
		require.EqualValues(t, 1, rdb.Do(ctx, bfExists, "a", "hello").Val())

		// rename*3
		require.NoError(t, rdb.Del(ctx, "a", "a1", "a2", "a3").Err())
		require.NoError(t, rdb.Do(ctx, bfAdd, "a", "hello").Err())
		require.NoError(t, rdb.Do(ctx, bfAdd, "a1", "world1").Err())
		require.NoError(t, rdb.Do(ctx, bfAdd, "a2", "world2").Err())
		require.NoError(t, rdb.Do(ctx, bfAdd, "a3", "world3").Err())
		require.NoError(t, rdb.Rename(ctx, "a", "a1").Err())
		require.NoError(t, rdb.Rename(ctx, "a1", "a2").Err())
		require.NoError(t, rdb.Rename(ctx, "a2", "a3").Err())
		require.EqualValues(t, "", rdb.Get(ctx, "a").Val())
		require.EqualValues(t, "", rdb.Get(ctx, "a1").Val())
		require.EqualValues(t, "", rdb.Get(ctx, "a2").Val())
		require.EqualValues(t, 1, rdb.Do(ctx, bfExists, "a3", "hello").Val())
	})

	t.Run("RenameNX Bloom", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "a", "a1").Err())
		require.NoError(t, rdb.Do(ctx, bfAdd, "a", "hello").Err())
		require.NoError(t, rdb.Do(ctx, bfAdd, "a1", "world").Err())
		require.EqualValues(t, false, rdb.RenameNX(ctx, "a", "a1").Val())
		require.EqualValues(t, 1, rdb.Do(ctx, bfExists, "a", "hello").Val())
		require.EqualValues(t, 1, rdb.Do(ctx, bfExists, "a1", "world").Val())

		require.NoError(t, rdb.Del(ctx, "a", "a1").Err())
		require.NoError(t, rdb.Do(ctx, bfAdd, "a", "hello").Err())
		require.EqualValues(t, true, rdb.RenameNX(ctx, "a", "a1").Val())
		require.EqualValues(t, 1, rdb.Do(ctx, bfExists, "a1", "hello").Val())
		require.EqualValues(t, "", rdb.Get(ctx, "a").Val())

		// key == newkey
		require.NoError(t, rdb.Del(ctx, "a", "a1").Err())
		require.NoError(t, rdb.Do(ctx, bfAdd, "a", "hello").Err())
		require.EqualValues(t, false, rdb.RenameNX(ctx, "a", "a").Val())
		require.EqualValues(t, 1, rdb.Do(ctx, bfExists, "a", "hello").Val())
	})
}

func TestRenameStream(t *testing.T) {
	srv := util.StartServer(t, map[string]string{})
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	XADD := "XADD"
	XREAD := "XREAD"
	t.Run("Rename Stream", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "a", "a1").Err())
		require.NoError(t, rdb.Do(ctx, XADD, "a", "*", "a", "hello").Err())
		require.NoError(t, rdb.Rename(ctx, "a", "a1").Err())
		require.EqualValues(t, "", rdb.Get(ctx, "a").Val())
		require.Contains(t, rdb.Do(ctx, XREAD, "STREAMS", "a1", "0").String(), "hello")
		require.EqualValues(t, -1, rdb.TTL(ctx, "a1").Val())

		// to-key has value with TTL
		require.NoError(t, rdb.Del(ctx, "a", "a1").Err())
		require.NoError(t, rdb.Do(ctx, XADD, "a", "*", "a", "hello").Err())
		require.NoError(t, rdb.Expire(ctx, "a", 10*time.Second).Err())
		require.NoError(t, rdb.Do(ctx, XADD, "a1", "*", "a", "world").Err())
		require.NoError(t, rdb.Expire(ctx, "a1", 1000*time.Second).Err())
		require.NoError(t, rdb.Rename(ctx, "a", "a1").Err())
		require.EqualValues(t, "", rdb.Get(ctx, "a").Val())
		require.Contains(t, rdb.Do(ctx, XREAD, "STREAMS", "a1", "0").String(), "hello")
		util.BetweenValues(t, rdb.TTL(ctx, "a1").Val(), time.Second, 10*time.Second)

		// to-key has value that not same type
		require.NoError(t, rdb.Del(ctx, "a", "a1").Err())
		require.NoError(t, rdb.Do(ctx, XADD, "a", "*", "a", "hello").Err())
		require.NoError(t, rdb.LPush(ctx, "a1", "a").Err())
		require.NoError(t, rdb.Rename(ctx, "a", "a1").Err())
		require.EqualValues(t, "", rdb.Get(ctx, "a").Val())
		require.Contains(t, rdb.Do(ctx, XREAD, "STREAMS", "a1", "0").String(), "hello")
		require.EqualValues(t, -1, rdb.TTL(ctx, "a1").Val())

		// key == newkey
		require.NoError(t, rdb.Del(ctx, "a").Err())
		require.NoError(t, rdb.Do(ctx, XADD, "a", "*", "a", "hello").Err())
		require.NoError(t, rdb.Rename(ctx, "a", "a").Err())
		require.Contains(t, rdb.Do(ctx, XREAD, "STREAMS", "a", "0").String(), "hello")

		// rename*3
		require.NoError(t, rdb.Del(ctx, "a", "a1", "a2", "a3").Err())
		require.NoError(t, rdb.Do(ctx, XADD, "a", "*", "a", "hello").Err())
		require.NoError(t, rdb.Do(ctx, XADD, "a1", "*", "a", "world1").Err())
		require.NoError(t, rdb.Do(ctx, XADD, "a2", "*", "a", "world2").Err())
		require.NoError(t, rdb.Do(ctx, XADD, "a3", "*", "a", "world3").Err())
		require.NoError(t, rdb.Rename(ctx, "a", "a1").Err())
		require.NoError(t, rdb.Rename(ctx, "a1", "a2").Err())
		require.NoError(t, rdb.Rename(ctx, "a2", "a3").Err())
		require.EqualValues(t, "", rdb.Get(ctx, "a").Val())
		require.EqualValues(t, "", rdb.Get(ctx, "a1").Val())
		require.EqualValues(t, "", rdb.Get(ctx, "a2").Val())
		require.Contains(t, rdb.Do(ctx, XREAD, "STREAMS", "a3", "0").String(), "hello")
	})

	t.Run("RenameNX Stream", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "a", "a1").Err())
		require.NoError(t, rdb.Do(ctx, XADD, "a", "*", "a", "hello").Err())
		require.NoError(t, rdb.Do(ctx, XADD, "a1", "*", "a", "world").Err())
		require.EqualValues(t, false, rdb.RenameNX(ctx, "a", "a1").Val())
		require.Contains(t, rdb.Do(ctx, XREAD, "STREAMS", "a", "0").String(), "hello")
		require.Contains(t, rdb.Do(ctx, XREAD, "STREAMS", "a1", "0").String(), "world")

		require.NoError(t, rdb.Del(ctx, "a", "a1").Err())
		require.NoError(t, rdb.Do(ctx, XADD, "a", "*", "a", "hello").Err())
		require.EqualValues(t, true, rdb.RenameNX(ctx, "a", "a1").Val())
		require.Contains(t, rdb.Do(ctx, XREAD, "STREAMS", "a1", "0").String(), "hello")
		require.EqualValues(t, "", rdb.Get(ctx, "a").Val())

		// key == newkey
		require.NoError(t, rdb.Del(ctx, "a", "a1").Err())
		require.NoError(t, rdb.Do(ctx, XADD, "a", "*", "a", "hello").Err())
		require.EqualValues(t, false, rdb.RenameNX(ctx, "a", "a").Val())
		require.Contains(t, rdb.Do(ctx, XREAD, "STREAMS", "a", "0").String(), "hello")
	})
}

func TestRenameError(t *testing.T) {
	srv := util.StartServer(t, map[string]string{})
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	t.Run("Rename from empty key", func(t *testing.T) {
		require.Error(t, rdb.Rename(ctx, ".empty", "a").Err())
		require.Error(t, rdb.RenameNX(ctx, ".empty", "a").Err())
	})

}
