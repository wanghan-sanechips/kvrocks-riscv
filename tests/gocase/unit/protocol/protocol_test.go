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

package protocol

import (
	"context"
	"testing"

	"github.com/redis/go-redis/v9"

	"github.com/apache/kvrocks/tests/gocase/util"
	"github.com/stretchr/testify/require"
)

func TestProtocolNetwork(t *testing.T) {
	srv := util.StartServer(t, map[string]string{})
	defer srv.Close()

	t.Run("empty bulk array command", func(t *testing.T) {
		c := srv.NewTCPClient()
		defer func() { require.NoError(t, c.Close()) }()
		require.NoError(t, c.Write("*-1\r\n"))
		require.NoError(t, c.Write("*0\r\n"))
		require.NoError(t, c.Write("\r\n"))
		require.NoError(t, c.Write("*1\r\n$4\r\nPING\r\n"))
		c.MustRead(t, "+PONG")
	})

	t.Run("empty inline command", func(t *testing.T) {
		c := srv.NewTCPClient()
		defer func() { require.NoError(t, c.Close()) }()
		require.NoError(t, c.Write(" \r\n"))
		require.NoError(t, c.Write("*1\r\n$4\r\nPING\r\n"))
		c.MustRead(t, "+PONG")
	})

	t.Run("out of range multibulk length", func(t *testing.T) {
		c := srv.NewTCPClient()
		defer func() { require.NoError(t, c.Close()) }()
		require.NoError(t, c.Write("*20000000\r\n"))
		c.MustMatch(t, "invalid multibulk length")
	})

	t.Run("wrong multibulk payload header", func(t *testing.T) {
		c := srv.NewTCPClient()
		defer func() { require.NoError(t, c.Close()) }()
		require.NoError(t, c.Write("*3\r\n$3\r\nSET\r\n$1\r\nx\r\nfoo\r\n"))
		c.MustMatch(t, "expected '\\$'")
	})

	t.Run("negative multibulk payload length", func(t *testing.T) {
		c := srv.NewTCPClient()
		defer func() { require.NoError(t, c.Close()) }()
		require.NoError(t, c.Write("*3\r\n$3\r\nSET\r\n$1\r\nx\r\n$-10\r\n"))
		c.MustMatch(t, "invalid bulk length")
	})

	t.Run("out of range multibulk payload length", func(t *testing.T) {
		c := srv.NewTCPClient()
		defer func() { require.NoError(t, c.Close()) }()
		require.NoError(t, c.Write("*3\r\n$3\r\nSET\r\n$1\r\nx\r\n$2000000000\r\n"))
		c.MustMatch(t, "invalid bulk length")
	})

	t.Run("non-number multibulk payload length", func(t *testing.T) {
		c := srv.NewTCPClient()
		defer func() { require.NoError(t, c.Close()) }()
		require.NoError(t, c.Write("*3\r\n$3\r\nSET\r\n$1\r\nx\r\n$foo\r\n"))
		c.MustMatch(t, "invalid bulk length")
	})

	t.Run("multibulk request not followed by bulk arguments", func(t *testing.T) {
		c := srv.NewTCPClient()
		defer func() { require.NoError(t, c.Close()) }()
		require.NoError(t, c.Write("*1\r\nfoo\r\n"))
		c.MustMatch(t, "expected '\\$'")
	})

	t.Run("generic wrong number of args", func(t *testing.T) {
		rdb := srv.NewClient()
		defer func() { require.NoError(t, rdb.Close()) }()
		v := rdb.Do(context.Background(), "ping", "x", "y")
		require.EqualError(t, v.Err(), "ERR wrong number of arguments")
	})

	t.Run("empty array parsed", func(t *testing.T) {
		c := srv.NewTCPClient()
		defer func() { require.NoError(t, c.Close()) }()
		require.NoError(t, c.Write("*-1\r\n*3\r\n$3\r\nset\r\n$3\r\nkey\r\n$3\r\nval\r\n"))
		c.MustRead(t, "+OK")
	})

	t.Run("allow only LF protocol separator", func(t *testing.T) {
		c := srv.NewTCPClient()
		defer func() { require.NoError(t, c.Close()) }()
		require.NoError(t, c.Write("set foo 123\n"))
		c.MustRead(t, "+OK")
	})

	t.Run("inline protocol with quoted string", func(t *testing.T) {
		c := srv.NewTCPClient()
		LF := "\n"
		defer func() { require.NoError(t, c.Close()) }()
		require.NoError(t, c.Write("RPUSH my_list a 'b c' d"+LF))
		c.MustRead(t, ":3")
		require.NoError(t, c.Write(`RPUSH my_list "foo \x61\x62"`+LF))
		c.MustRead(t, ":4")
		require.NoError(t, c.Write(`RPUSH my_list "bar \"\g\t\n\q"`+LF))
		c.MustRead(t, ":5")
		require.NoError(t, c.Write(`RPUSH my_list ' a b' "c d e " `+LF))
		c.MustRead(t, ":7")

		rdb := srv.NewClient()
		defer func() { require.NoError(t, rdb.Close()) }()
		values, err := rdb.LRange(context.Background(), "my_list", 0, -1).Result()
		require.NoError(t, err)
		require.Equal(t, []string{"a", "b c", "d", "foo ab", "bar \"g\t\nq", " a b", "c d e "}, values)
	})

	t.Run("mix LF/CRLF protocol separator", func(t *testing.T) {
		c := srv.NewTCPClient()
		defer func() { require.NoError(t, c.Close()) }()
		require.NoError(t, c.Write("*-1\r\nset foo 123\nget foo\r\n*3\r\n$3\r\nset\r\n$3\r\nkey\r\n$3\r\nval\r\n"))
		for _, res := range []string{"+OK", "$3", "123", "+OK"} {
			c.MustRead(t, res)
		}
	})

	t.Run("invalid LF in multi bulk protocol", func(t *testing.T) {
		c := srv.NewTCPClient()
		defer func() { require.NoError(t, c.Close()) }()
		require.NoError(t, c.Write("*3\n$3\r\nset\r\n$3\r\nkey\r\n$3\r\nval\r\n"))
		c.MustMatch(t, "invalid multibulk length")
	})

	t.Run("command type should return the simple string", func(t *testing.T) {
		c := srv.NewTCPClient()
		defer func() { require.NoError(t, c.Close()) }()
		require.NoError(t, c.Write("set foo bar\n"))
		c.MustRead(t, "+OK")
		require.NoError(t, c.Write("type foo\n"))
		c.MustRead(t, "+string")
	})
}

func TestProtocolRESP2(t *testing.T) {
	srv := util.StartServer(t, map[string]string{
		"resp3-enabled": "no",
	})
	defer srv.Close()

	c := srv.NewTCPClient()
	rdb := srv.NewClient()
	defer func() {
		require.NoError(t, c.Close())
		require.NoError(t, rdb.Close())
	}()

	t.Run("debug protocol string", func(t *testing.T) {
		types := map[string][]string{
			"string":   {"$11", "Hello World"},
			"integer":  {":12345"},
			"double":   {"$5", "3.141"},
			"array":    {"*3", ":0", ":1", ":2"},
			"set":      {"*3", ":0", ":1", ":2"},
			"map":      {"*6", ":0", ":0", ":1", ":1", ":2", ":0"},
			"bignum":   {"$37", "1234567999999999999999999999999999999"},
			"true":     {":1"},
			"false":    {":0"},
			"null":     {"$-1"},
			"attrib":   {"|1", "$14", "key-popularity", "*2", "$7", "key:123", ":90"},
			"verbatim": {"$15", "verbatim string"},
		}
		for typ, expected := range types {
			args := []string{"DEBUG", "PROTOCOL", typ}
			require.NoError(t, c.WriteArgs(args...))
			for _, line := range expected {
				c.MustRead(t, line)
			}
		}
	})

	t.Run("multi bulk strings with null string", func(t *testing.T) {
		require.NoError(t, c.WriteArgs("HSET", "hash", "f1", "v1"))
		c.MustRead(t, ":1")

		require.NoError(t, c.WriteArgs("HMGET", "hash", "f1", "f2"))
		c.MustRead(t, "*2")
		c.MustRead(t, "$2")
		c.MustRead(t, "v1")
		c.MustRead(t, "$-1")
	})

	t.Run("null array", func(t *testing.T) {
		require.NoError(t, c.WriteArgs("ZRANK", "no-exists-zset", "m0", "WITHSCORE"))
		c.MustRead(t, "*-1")
	})

	t.Run("command ZRANGE should be always return an array of strings", func(t *testing.T) {
		rdb.ZAddArgs(context.Background(), "zset", redis.ZAddArgs{
			Members: []redis.Z{{1, "one"}, {2, "two"}, {3, "three"}},
		})

		require.NoError(t, c.WriteArgs("ZRANGE", "zset", "0", "-1"))
		c.MustRead(t, "*3")
		c.MustRead(t, "$3")
		c.MustRead(t, "one")
		c.MustRead(t, "$3")
		c.MustRead(t, "two")
		c.MustRead(t, "$5")
		c.MustRead(t, "three")

		require.NoError(t, c.WriteArgs("ZRANGE", "zset", "0", "-1", "WITHSCORES"))
		c.MustRead(t, "*6")
		c.MustRead(t, "$3")
		c.MustRead(t, "one")
		c.MustRead(t, "$1")
		c.MustRead(t, "1")
		c.MustRead(t, "$3")
		c.MustRead(t, "two")
		c.MustRead(t, "$1")
		c.MustRead(t, "2")
		c.MustRead(t, "$5")
		c.MustRead(t, "three")
		c.MustRead(t, "$1")
		c.MustRead(t, "3")
	})
}

func handshakeWithRESP3(t *testing.T, c *util.TCPClient) {
	require.NoError(t, c.WriteArgs("HELLO", "3"))
	values := []string{"%6",
		"$6", "server", "$5", "redis",
		"$7", "version", "$5", "4.0.0",
		"$5", "proto", ":3",
		"$4", "mode", "$10", "standalone",
		"$4", "role", "$6", "master",
		"$7", "modules", "_",
	}
	for _, line := range values {
		c.MustRead(t, line)
	}
}

func TestProtocolRESP3(t *testing.T) {
	srv := util.StartServer(t, map[string]string{
		"resp3-enabled": "yes",
	})
	defer srv.Close()

	c := srv.NewTCPClient()
	rdb := srv.NewClient()
	defer func() {
		require.NoError(t, c.Close())
		require.NoError(t, rdb.Close())
	}()
	handshakeWithRESP3(t, c)

	t.Run("debug protocol string", func(t *testing.T) {

		types := map[string][]string{
			"string":   {"$11", "Hello World"},
			"integer":  {":12345"},
			"double":   {",3.141"},
			"array":    {"*3", ":0", ":1", ":2"},
			"set":      {"~3", ":0", ":1", ":2"},
			"map":      {"%3", ":0", "#f", ":1", "#t", ":2", "#f"},
			"bignum":   {"(1234567999999999999999999999999999999"},
			"true":     {"#t"},
			"false":    {"#f"},
			"null":     {"_"},
			"attrib":   {"|1", "$14", "key-popularity", "*2", "$7", "key:123", ":90"},
			"verbatim": {"=19", "txt:verbatim string"},
		}
		for typ, expected := range types {
			args := []string{"DEBUG", "PROTOCOL", typ}
			require.NoError(t, c.WriteArgs(args...))
			for _, line := range expected {
				c.MustRead(t, line)
			}
		}
	})

	t.Run("multi bulk strings with null", func(t *testing.T) {
		require.NoError(t, c.WriteArgs("HSET", "hash", "f1", "v1"))
		c.MustRead(t, ":1")

		require.NoError(t, c.WriteArgs("HMGET", "hash", "f1", "f2"))
		c.MustRead(t, "*2")
		c.MustRead(t, "$2")
		c.MustRead(t, "v1")
		c.MustRead(t, "_")
	})

	t.Run("should return PUSH type", func(t *testing.T) {
		// use a new client to avoid affecting other tests
		require.NoError(t, c.WriteArgs("SUBSCRIBE", "test-channel"))
		c.MustRead(t, ">3")
		c.MustRead(t, "$9")
		c.MustRead(t, "subscribe")
		c.MustRead(t, "$12")
		c.MustRead(t, "test-channel")
		c.MustRead(t, ":1")
	})

	t.Run("null array", func(t *testing.T) {
		require.NoError(t, c.WriteArgs("ZRANK", "no-exists-zset", "m0", "WITHSCORE"))
		c.MustRead(t, "_")
	})

	t.Run("command ZRANGE should return an array of arrays if with score", func(t *testing.T) {
		rdb.ZAddArgs(context.Background(), "zset", redis.ZAddArgs{
			Members: []redis.Z{{1, "one"}, {2, "two"}, {3, "three"}},
		})

		// should return an array of strings if without score
		require.NoError(t, c.WriteArgs("ZRANGE", "zset", "0", "-1"))
		c.MustRead(t, "*3")
		c.MustRead(t, "$3")
		c.MustRead(t, "one")
		c.MustRead(t, "$3")
		c.MustRead(t, "two")
		c.MustRead(t, "$5")
		c.MustRead(t, "three")

		// should return an array of arrays if with score,
		// and the score should be a double type
		require.NoError(t, c.WriteArgs("ZRANGE", "zset", "0", "-1", "WITHSCORES"))
		c.MustRead(t, "*3")
		c.MustRead(t, "*2")
		c.MustRead(t, "$3")
		c.MustRead(t, "one")
		c.MustRead(t, ",1")
		c.MustRead(t, "*2")
		c.MustRead(t, "$3")
		c.MustRead(t, "two")
		c.MustRead(t, ",2")
		c.MustRead(t, "*2")
		c.MustRead(t, "$5")
		c.MustRead(t, "three")
		c.MustRead(t, ",3")
	})
}
