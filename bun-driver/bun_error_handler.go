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

package bun_driver

import (
	"errors"
	"slices"
	"strings"

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
	"use of closed network connection",
	"broken pipe",
	"bad connection",
	"context deadline exceeded",
}

type BunPgErrorHandler struct{}

func (h *BunPgErrorHandler) IsNetworkError(err error) bool {
	sqlState := h.getSQLStateFromError(err)
	if sqlState != "" && slices.Contains(NetworkErrors, sqlState) {
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
