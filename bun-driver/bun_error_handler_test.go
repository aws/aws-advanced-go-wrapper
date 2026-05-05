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
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/uptrace/bun/driver/pgdriver"
)

func TestBunPgErrorHandler_IsNetworkError(t *testing.T) {
	h := &BunPgErrorHandler{}

	t.Run("network error message patterns", func(t *testing.T) {
		assert.True(t, h.IsNetworkError(fmt.Errorf("unexpected EOF")))
		assert.True(t, h.IsNetworkError(fmt.Errorf("read: use of closed network connection")))
		assert.True(t, h.IsNetworkError(fmt.Errorf("write: broken pipe")))
		assert.True(t, h.IsNetworkError(fmt.Errorf("bad connection")))
		assert.True(t, h.IsNetworkError(fmt.Errorf("context deadline exceeded")))
	})

	t.Run("non-network errors", func(t *testing.T) {
		assert.False(t, h.IsNetworkError(fmt.Errorf("unique constraint violation")))
		assert.False(t, h.IsNetworkError(fmt.Errorf("serialization failure")))
	})
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
