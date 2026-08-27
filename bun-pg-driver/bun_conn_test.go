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
	"database/sql/driver"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uptrace/bun/driver/pgdriver"
)

func TestUnderlyingDriverIsNamedValueChecker(t *testing.T) {
	_, bareIsChecker := any((*pgdriver.Conn)(nil)).(driver.NamedValueChecker)
	assert.False(t, bareIsChecker, "if pgdriver gains CheckNamedValue this wrapper can be removed")

	var wrapped any = &bunPgConn{}
	_, ok := wrapped.(driver.NamedValueChecker)
	assert.True(t, ok)

	for name, satisfied := range map[string]bool{
		"Conn":               func() bool { _, ok := wrapped.(driver.Conn); return ok }(),
		"ConnPrepareContext": func() bool { _, ok := wrapped.(driver.ConnPrepareContext); return ok }(),
		"ConnBeginTx":        func() bool { _, ok := wrapped.(driver.ConnBeginTx); return ok }(),
		"ExecerContext":      func() bool { _, ok := wrapped.(driver.ExecerContext); return ok }(),
		"QueryerContext":     func() bool { _, ok := wrapped.(driver.QueryerContext); return ok }(),
		"Pinger":             func() bool { _, ok := wrapped.(driver.Pinger); return ok }(),
		"Validator":          func() bool { _, ok := wrapped.(driver.Validator); return ok }(),
		"SessionResetter":    func() bool { _, ok := wrapped.(driver.SessionResetter); return ok }(),
	} {
		assert.True(t, satisfied, "wrapped conn lost driver.%s", name)
	}
}

func TestCheckNamedValueConvertsToDriverValue(t *testing.T) {
	c := &bunPgConn{}

	tests := []struct {
		name     string
		in       any
		expected any
	}{
		{"int becomes int64", 42, int64(42)},
		{"int32 becomes int64", int32(7), int64(7)},
		{"string is unchanged", "hello", "hello"},
		{"bool is unchanged", true, true},
		{"float64 is unchanged", 1.5, 1.5},
		{"nil is unchanged", nil, nil},
		{"bytes are unchanged", []byte("abc"), []byte("abc")},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			namedValue := &driver.NamedValue{Ordinal: 1, Value: test.in}
			require.NoError(t, c.CheckNamedValue(namedValue))
			assert.Equal(t, test.expected, namedValue.Value)
		})
	}

	t.Run("time survives", func(t *testing.T) {
		now := time.Now()
		namedValue := &driver.NamedValue{Ordinal: 1, Value: now}
		require.NoError(t, c.CheckNamedValue(namedValue))
		assert.Equal(t, now, namedValue.Value)
	})

	t.Run("named parameters are refused", func(t *testing.T) {
		err := c.CheckNamedValue(&driver.NamedValue{Name: "val", Ordinal: 1, Value: 1})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "named parameter")
	})

	t.Run("unconvertible values report an error", func(t *testing.T) {
		assert.Error(t, c.CheckNamedValue(&driver.NamedValue{Ordinal: 1, Value: struct{ A int }{1}}))
	})
}

func TestUnderlyingDriverWrapsBothOpenPaths(t *testing.T) {
	d := NewUnderlyingDriver()

	var asDriver any = d
	_, isDriver := asDriver.(driver.Driver)
	assert.True(t, isDriver)
	_, isDriverContext := asDriver.(driver.DriverContext)
	assert.True(t, isDriverContext, "pgdriver implements DriverContext, so the wrapper must too")

	connector, err := d.OpenConnector("postgres://user:pass@localhost:5432/db?sslmode=disable")
	require.NoError(t, err)
	assert.Equal(t, d, connector.Driver(), "connector must report the wrapping driver")

	assert.True(t, BunPgDriverDialect{}.IsDialect(d))
}
