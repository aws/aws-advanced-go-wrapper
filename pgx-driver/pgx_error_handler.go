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

package pgx_driver

import (
	"context"
	"database/sql/driver"
	"errors"
	"io"
	"slices"
	"strings"
	"syscall"

	"github.com/jackc/pgx/v5/pgconn"
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

type PgxErrorHandler struct {
}

// IsReadOnlyError reports whether err is a write to a read-only connection.
func (p *PgxErrorHandler) IsReadOnlyError(err error) bool {
	return p.getSQLStateFromError(err) == PgReadOnlyErrorState
}

func (p *PgxErrorHandler) IsNetworkError(err error) bool {
	// Caller-initiated cancellation / deadline is not a DB network failure.
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	// driver.ErrBadConn is database/sql's stale-conn signal; it discards the
	// cached conn and retries on a fresh one. Not a network fault.
	if errors.Is(err, driver.ErrBadConn) {
		return false
	}

	// Typed transport faults; substrings below are a fallback.
	if errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.EPIPE) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}

	sqlState := p.getSQLStateFromError(err)
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

func (p *PgxErrorHandler) IsLoginError(err error) bool {
	sqlState := p.getSQLStateFromError(err)
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

func (p *PgxErrorHandler) getSQLStateFromError(err error) string {
	var pgErr *pgconn.PgError
	ok := errors.As(err, &pgErr)
	if ok {
		return pgErr.SQLState()
	}
	return ""
}
