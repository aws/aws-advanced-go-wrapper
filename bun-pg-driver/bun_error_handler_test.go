/*
  Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

  Licensed under the Apache License, Version 2.0 (the "License").
  You may not use this file except in compliance with the License.
  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
*/

package bun_pg_driver

import (
	"context"
	"database/sql/driver"
	"fmt"
	"io"
	"net"
	"syscall"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/uptrace/bun/driver/pgdriver"
)

func TestBunPgErrorHandler_IsNetworkError(t *testing.T) {
	h := &BunPgErrorHandler{}

	t.Run("network error message patterns", func(t *testing.T) {
		assert.True(t, h.IsNetworkError(fmt.Errorf("unexpected EOF")))
		assert.True(t, h.IsNetworkError(fmt.Errorf("write: broken pipe")))
	})

	// An in-flight read whose socket was aborted by EFM or the connection
	// tracker surfaces as net.ErrClosed, and is the only failover signal
	// available when the network path is blackholed. Matched by sentinel, not
	// by message text - an unwrapped string that merely reads the same is not
	// the sentinel and stays unclassified.
	t.Run("net.ErrClosed (in-flight abort) IS a network error", func(t *testing.T) {
		assert.True(t, h.IsNetworkError(net.ErrClosed))
		assert.True(t, h.IsNetworkError(fmt.Errorf("read tcp: %w", net.ErrClosed)))
		assert.True(t, h.IsNetworkError(&net.OpError{Op: "read", Net: "tcp", Err: net.ErrClosed}))
		assert.False(t, h.IsNetworkError(fmt.Errorf("read: use of closed network connection")))
	})

	t.Run("bare io.EOF is a network error", func(t *testing.T) {
		// This is the error an Aurora failover actually produces under pgdriver: the server closes
		// the connection between messages and pgdriver reports io.EOF bare, not wrapped as
		// io.ErrUnexpectedEOF. Confirmed against a live cluster - before this was classified,
		// failover never triggered and callers saw a raw "EOF" instead of
		// Failover.connectionChangedError.
		assert.True(t, h.IsNetworkError(io.EOF))
		assert.True(t, h.IsNetworkError(fmt.Errorf("reading from server: %w", io.EOF)))
	})

	t.Run("non-network errors", func(t *testing.T) {
		assert.False(t, h.IsNetworkError(fmt.Errorf("unique constraint violation")))
		assert.False(t, h.IsNetworkError(fmt.Errorf("serialization failure")))
	})

	t.Run("caller cancellation is not a network error", func(t *testing.T) {
		assert.False(t, h.IsNetworkError(context.Canceled))
		assert.False(t, h.IsNetworkError(fmt.Errorf("query aborted: %w", context.Canceled)))
		assert.False(t, h.IsNetworkError(context.DeadlineExceeded))
		assert.False(t, h.IsNetworkError(fmt.Errorf("read timed out: %w", context.DeadlineExceeded)))
	})

	t.Run("driver.ErrBadConn is not a network error", func(t *testing.T) {
		assert.False(t, h.IsNetworkError(driver.ErrBadConn))
		assert.False(t, h.IsNetworkError(fmt.Errorf("wrapped: %w", driver.ErrBadConn)))
	})
}

func TestBunPgErrorHandler_TypedTransportSentinels(t *testing.T) {
	h := &BunPgErrorHandler{}
	assert.True(t, h.IsNetworkError(fmt.Errorf("read tcp: %w", syscall.ECONNRESET)))
	assert.True(t, h.IsNetworkError(fmt.Errorf("write tcp: %w", syscall.EPIPE)))
	assert.True(t, h.IsNetworkError(fmt.Errorf("reading: %w", io.ErrUnexpectedEOF)))
}

func TestBunPgErrorHandler_IsLoginError(t *testing.T) {
	h := &BunPgErrorHandler{}

	t.Run("login error code in message", func(t *testing.T) {
		assert.True(t, h.IsLoginError(fmt.Errorf("password authentication failed 28P01")))
		assert.True(t, h.IsLoginError(fmt.Errorf("authorization failed 28000")))
	})

	t.Run("non-login errors", func(t *testing.T) {
		assert.False(t, h.IsLoginError(fmt.Errorf("connection refused")))
		assert.False(t, h.IsLoginError(fmt.Errorf("timeout")))
	})
}

func TestGetSQLState(t *testing.T) {
	h := &BunPgErrorHandler{}

	t.Run("returns empty for non-pgdriver errors", func(t *testing.T) {
		assert.Equal(t, "", h.getSQLStateFromError(fmt.Errorf("not a pg error")))
	})

	t.Run("pgdriver.Error type compiles", func(t *testing.T) {
		// pgdriver.Error has unexported fields so we can't construct one with
		// a specific SQLSTATE. This verifies the type assertion path compiles.
		var _ pgdriver.Error
	})
}

func TestBunPgErrorHandler_NetworkErrorsList(t *testing.T) {
	assert.Contains(t, NetworkErrors, "53")
	assert.Contains(t, NetworkErrors, "08")
	assert.NotContains(t, AccessErrors, "08004")
}

func TestBunPgErrorHandler_ReadOnlyConst(t *testing.T) {
	assert.Equal(t, "25006", PgReadOnlyErrorState)
}
