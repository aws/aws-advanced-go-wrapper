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

package test

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"net"
	"regexp"
	"strings"
	"syscall"
	"testing"

	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/property_util"
	pgx_driver "github.com/aws/aws-advanced-go-wrapper/pgx-driver"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
)

func TestPrepareDsn(t *testing.T) {
	driverDialect := &pgx_driver.PgxDriverDialect{}

	properties := map[string]string{
		property_util.USER.Name:     "user",
		property_util.PASSWORD.Name: "password",
		property_util.PORT.Name:     "5432",
		property_util.HOST.Name:     "host",
		property_util.DATABASE.Name: "dbName",
		property_util.PLUGINS.Name:  "test",
		"monitoring-user":           "monitor-user",
	}

	dsn := driverDialect.PrepareDsn(properties, nil)
	res, _ := regexp.MatchString("^\\w+=\\w+( \\w+=\\w+)*$", dsn)
	assert.True(t, res)
	assert.True(t, strings.Contains(dsn, fmt.Sprintf("%s=user", property_util.USER.Name)))
	assert.True(t, strings.Contains(dsn, fmt.Sprintf("%s=password", property_util.PASSWORD.Name)))
	assert.True(t, strings.Contains(dsn, fmt.Sprintf("%s=5432", property_util.PORT.Name)))
	assert.True(t, strings.Contains(dsn, fmt.Sprintf("%s=host", property_util.HOST.Name)))
	assert.True(t, strings.Contains(dsn, fmt.Sprintf("%s=dbName", property_util.DATABASE.Name)))
	assert.False(t, strings.Contains(dsn, fmt.Sprintf("%s=test", property_util.PLUGINS.Name)))
	assert.False(t, strings.Contains(dsn, "monitor-user"))
}

func TestPgxErrorHandler(t *testing.T) {
	errorHandler := &pgx_driver.PgxErrorHandler{}
	for _, message := range pgx_driver.PgNetworkErrorMessages {
		err := errors.New(message)
		assert.True(t, errorHandler.IsNetworkError(err))
		assert.False(t, errorHandler.IsLoginError(err))
	}
	for _, code := range pgx_driver.NetworkErrors {
		err := &pgconn.PgError{Code: code}
		assert.True(t, errorHandler.IsNetworkError(err))
		assert.False(t, errorHandler.IsLoginError(err))
	}
	for _, code := range pgx_driver.AccessErrors {
		err := &pgconn.PgError{Code: code}
		assert.False(t, errorHandler.IsNetworkError(err))
		assert.True(t, errorHandler.IsLoginError(err))
	}
}

func TestPgxErrorHandler_CallerCancellationAndStaleConn(t *testing.T) {
	errorHandler := &pgx_driver.PgxErrorHandler{}

	t.Run("context.Canceled is not a network error", func(t *testing.T) {
		assert.False(t, errorHandler.IsNetworkError(context.Canceled))
		assert.False(t, errorHandler.IsNetworkError(fmt.Errorf("query aborted: %w", context.Canceled)))
	})

	t.Run("context.DeadlineExceeded is not a network error", func(t *testing.T) {
		assert.False(t, errorHandler.IsNetworkError(context.DeadlineExceeded))
		assert.False(t, errorHandler.IsNetworkError(fmt.Errorf("read timed out: %w", context.DeadlineExceeded)))
	})

	t.Run("driver.ErrBadConn is not a network error", func(t *testing.T) {
		assert.False(t, errorHandler.IsNetworkError(driver.ErrBadConn))
		assert.False(t, errorHandler.IsNetworkError(fmt.Errorf("wrapped: %w", driver.ErrBadConn)))
	})
}

// net.ErrClosed and pgconn.ErrConnClosed both describe a closed connection but
// mean opposite things. ErrConnClosed is a use-after-close: the operation never
// reached the wire. net.ErrClosed is an abort of a read already in flight, which
// in this wrapper only happens when EFM or the connection tracker deliberately
// kills a connection to a host it judged unhealthy - the only failover signal
// available when the network path is blackholed and no RST/FIN/EOF ever arrives.
func TestPgxErrorHandler_ClosedConnectionDistinction(t *testing.T) {
	h := &pgx_driver.PgxErrorHandler{}

	t.Run("net.ErrClosed (in-flight abort) IS a network error", func(t *testing.T) {
		assert.True(t, h.IsNetworkError(net.ErrClosed))
		assert.True(t, h.IsNetworkError(fmt.Errorf("read tcp: %w", net.ErrClosed)))
		// The shape pgx actually produces when the socket is closed mid-read.
		assert.True(t, h.IsNetworkError(&net.OpError{
			Op: "read", Net: "tcp", Err: net.ErrClosed,
		}))
	})

	t.Run("pgconn.ErrConnClosed (use-after-close) is NOT a network error", func(t *testing.T) {
		assert.True(t, errors.Is(pgconn.ErrConnClosed, pgconn.ErrConnClosed))
		assert.False(t, h.IsNetworkError(pgconn.ErrConnClosed))
		assert.False(t, h.IsNetworkError(fmt.Errorf("query failed: %w", pgconn.ErrConnClosed)))
	})
}

// Pins the invariant that dealWithError's !errors.Is(err, lastErrorDealtWith)
// guard (failover_plugin.go) does not swallow consecutive EFM aborts. Each abort
// produces a distinct *net.OpError, so errors.Is between two of them must be
// false - unlike a bare sentinel, which compares equal to itself and would make
// the second and every later occurrence be silently skipped.
func TestPgxErrorHandler_DistinctOpErrorsAreNotEqual(t *testing.T) {
	first := &net.OpError{Op: "read", Net: "tcp", Err: net.ErrClosed}
	second := &net.OpError{Op: "read", Net: "tcp", Err: net.ErrClosed}

	assert.False(t, errors.Is(second, first))
	assert.True(t, errors.Is(first, net.ErrClosed))
	assert.True(t, errors.Is(second, net.ErrClosed))
}

func TestPgxErrorHandler_TypedTransportSentinels(t *testing.T) {
	h := &pgx_driver.PgxErrorHandler{}
	assert.True(t, h.IsNetworkError(fmt.Errorf("read tcp 10.0.0.1:5432->10.0.0.2:5432: %w", syscall.ECONNRESET)))
	assert.True(t, h.IsNetworkError(fmt.Errorf("write tcp: %w", syscall.EPIPE)))
	assert.True(t, h.IsNetworkError(fmt.Errorf("reading message: %w", io.ErrUnexpectedEOF)))
}

func TestPgxErrorHandler_SqlStatePrefixMatching(t *testing.T) {
	h := &pgx_driver.PgxErrorHandler{}
	for _, code := range []string{"08006", "08003", "08000", "08001", "08004", "58030", "53300", "53000", "F0000", "57P01", "57P03"} {
		assert.True(t, h.IsNetworkError(&pgconn.PgError{Code: code}), "expected network error for %s", code)
	}
	for _, code := range []string{"57014", "40001", "40P01", "XX000", "25006"} {
		assert.False(t, h.IsNetworkError(&pgconn.PgError{Code: code}), "expected NOT network error for %s", code)
	}
}

func TestPgxErrorHandler_IsReadOnlyError(t *testing.T) {
	h := &pgx_driver.PgxErrorHandler{}
	assert.True(t, h.IsReadOnlyError(&pgconn.PgError{Code: "25006"}))
	assert.False(t, h.IsReadOnlyError(&pgconn.PgError{Code: "08006"}))
	assert.False(t, h.IsReadOnlyError(fmt.Errorf("some error")))
	assert.False(t, h.IsNetworkError(&pgconn.PgError{Code: "25006"}))
}

func TestPgxDriverDialect_IsReadOnlyError(t *testing.T) {
	d := pgx_driver.NewPgxDriverDialect()
	assert.True(t, d.IsReadOnlyError(&pgconn.PgError{Code: "25006"}))
	assert.False(t, d.IsReadOnlyError(&pgconn.PgError{Code: "08006"}))
	assert.False(t, d.IsNetworkError(&pgconn.PgError{Code: "25006"}))
}
