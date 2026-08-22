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
	"errors"
	"io"
	"net"
	"slices"
	"strings"
	"syscall"

	"github.com/uptrace/bun/driver/pgdriver"
)

var AccessErrors = []string{
	"28P01",
	"28000",
}

var NetworkErrors = []string{
	"53",
	"57P01",
	"57P02",
	"57P03",
	"58",
	"08",
	"99",
	"F0",
}

var PgNetworkErrorMessages = []string{
	"unexpected EOF",
	"broken pipe",
}

// PgReadOnlyErrorState is the SQLSTATE for a write to a read-only connection.
const PgReadOnlyErrorState = "25006"

type BunPgErrorHandler struct{}

// IsReadOnlyError reports whether err is a write to a read-only connection.
func (h *BunPgErrorHandler) IsReadOnlyError(err error) bool {
	return h.getSQLStateFromError(err) == PgReadOnlyErrorState
}

func (h *BunPgErrorHandler) IsNetworkError(err error) bool {
	// Caller-initiated cancellation / deadline is not a DB network failure.
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	// driver.ErrBadConn is database/sql's signal that a cached conn is stale;
	// database/sql discards it and retries on a fresh connection. Genuine
	// server/network faults surface as SQLSTATE 08xxx/57P01 or raw I/O errors.
	if errors.Is(err, driver.ErrBadConn) {
		return false
	}

	// net.ErrClosed means a read that was ALREADY IN FLIGHT had its socket
	// closed out from under it by another goroutine. In this wrapper that only
	// happens when we deliberately abort a connection whose host we just judged
	// unhealthy: EFM's monitor (plugins/efm/host_monitor.go) or the connection
	// tracker (plugins/connection_tracker/opened_connection_tracker.go). A
	// blackholed network path (hung instance, partition, security-group change)
	// yields no RST/FIN/EOF, so this is the only failover signal available.
	if errors.Is(err, net.ErrClosed) {
		return true
	}

	// Typed transport faults; substrings below are a fallback.
	if errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.EPIPE) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}

	sqlState := h.getSQLStateFromError(err)
	if sqlState != "" && slices.ContainsFunc(NetworkErrors, func(prefix string) bool {
		return strings.HasPrefix(sqlState, prefix)
	}) {
		return true
	}

	for _, networkError := range PgNetworkErrorMessages {
		if strings.Contains(err.Error(), networkError) {
			return true
		}
	}
	return false
}

func (h *BunPgErrorHandler) IsLoginError(err error) bool {
	sqlState := h.getSQLStateFromError(err)
	if sqlState != "" && slices.Contains(AccessErrors, sqlState) {
		return true
	}

	for _, accessError := range AccessErrors {
		if strings.Contains(err.Error(), accessError) {
			return true
		}
	}
	return false
}

func (h *BunPgErrorHandler) getSQLStateFromError(err error) string {
	var pgErr pgdriver.Error
	if errors.As(err, &pgErr) {
		return pgErr.Field('C')
	}
	return ""
}
