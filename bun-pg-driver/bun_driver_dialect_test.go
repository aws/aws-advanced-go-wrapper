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

	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/driver_infrastructure"
	"github.com/stretchr/testify/assert"
	"github.com/uptrace/bun/driver/pgdriver"
)

func TestIsDialect(t *testing.T) {
	dialect := NewBunPgDriverDialect()

	t.Run("matches pgdriver.Driver value", func(t *testing.T) {
		drv := pgdriver.NewDriver()
		assert.True(t, dialect.IsDialect(drv))
	})

	t.Run("does not match other drivers", func(t *testing.T) {
		assert.False(t, dialect.IsDialect(&BunPgDriver{}))
	})
}

func TestRowParser(t *testing.T) {
	rp := BunPgDriverDialect{}.GetRowParser()

	t.Run("ParseString from string", func(t *testing.T) {
		v, ok := rp.ParseString(driver.Value("instance-1"))
		assert.True(t, ok)
		assert.Equal(t, "instance-1", v)
	})

	t.Run("ParseString from []byte", func(t *testing.T) {
		v, ok := rp.ParseString(driver.Value([]byte("instance-1")))
		assert.True(t, ok)
		assert.Equal(t, "instance-1", v)
	})

	t.Run("ParseBool", func(t *testing.T) {
		v, ok := rp.ParseBool(driver.Value(true))
		assert.True(t, ok)
		assert.True(t, v)
	})

	t.Run("ParseFloat64", func(t *testing.T) {
		f, ok := rp.ParseFloat64(driver.Value(3.5))
		assert.True(t, ok)
		assert.Equal(t, 3.5, f)
	})

	t.Run("ParseTime", func(t *testing.T) {
		now := time.Date(2026, 4, 24, 17, 0, 0, 0, time.UTC)
		got, ok := rp.ParseTime(driver.Value(now))
		assert.True(t, ok)
		assert.True(t, got.Equal(now))
	})

	t.Run("ParseInt64", func(t *testing.T) {
		v, ok := rp.ParseInt64(driver.Value(int64(7)))
		assert.True(t, ok)
		assert.Equal(t, int64(7), v)
	})

	t.Run("returns false on incompatible types", func(t *testing.T) {
		_, ok := rp.ParseInt64(driver.Value("not a number"))
		assert.False(t, ok)
		_, ok = rp.ParseTime(driver.Value("not a time"))
		assert.False(t, ok)
		_, ok = rp.ParseString(driver.Value(42))
		assert.False(t, ok)
		_, ok = rp.ParseBool(driver.Value("not a bool"))
		assert.False(t, ok)
		_, ok = rp.ParseFloat64(driver.Value("not a number"))
		assert.False(t, ok)
	})
}

func TestPropertyResolver(t *testing.T) {
	pr := BunPgDriverDialect{}.GetPropertyResolver()

	// pgdriver does accept these in the DSN, so the wrapper can bound its own connections.
	assert.Equal(t, "connect_timeout", pr.GetPropertyName(driver_infrastructure.ConnectTimeout))
	assert.Equal(t, "read_timeout", pr.GetPropertyName(driver_infrastructure.SocketTimeout))

	// pgdriver parses a bare integer as seconds, matching pgx's connect_timeout handling.
	assert.Equal(t, "5", pr.FormatValue(driver_infrastructure.ConnectTimeout, 5000))
	// Must never round down to zero: pgdriver maps a value of zero or less to an
	// already-expired deadline rather than to "no timeout".
	assert.Equal(t, "1", pr.FormatValue(driver_infrastructure.SocketTimeout, 5))

	assert.Empty(t, pr.CreateProps())
	assert.Equal(t,
		map[string]string{"connect_timeout": "3", "read_timeout": "30"},
		pr.CreateProps(
			driver_infrastructure.WithProperty(driver_infrastructure.ConnectTimeout, 3000),
			driver_infrastructure.WithProperty(driver_infrastructure.SocketTimeout, 30000)))

	// A non-positive request is omitted rather than emitted as 0, which pgdriver would read as an
	// already-expired deadline.
	assert.Empty(t, pr.CreateProps(driver_infrastructure.WithProperty(driver_infrastructure.SocketTimeout, 0)))
}
