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

package driver_infrastructure

import (
	"errors"
	"slices"
	"strings"

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
	"XX",
	"unexpected EOF",
	"use of closed network connection",
}

var PgNetworkErrorMessages = []string{
	"unexpected EOF",
	"use of closed network connection",
	"broken pipe",
}

type PgxErrorHandler struct {
}

func (p *PgxErrorHandler) IsNetworkError(err error) bool {
	sqlState := p.getSQLStateFromError(err)

	if sqlState != "" && slices.Contains(NetworkErrors, sqlState) {
		return true
	}

	for _, networkError := range NetworkErrors {
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
