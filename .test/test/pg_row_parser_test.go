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
	"database/sql/driver"
	"testing"
	"time"

	pgx_driver "github.com/aws/aws-advanced-go-wrapper/pgx-driver"
	"github.com/stretchr/testify/assert"
)

func TestPgRowParser_ParseString(t *testing.T) {
	parser := pgx_driver.PgxDriverDialect{}.GetRowParser()

	// Valid string
	val, ok := parser.ParseString("hello")
	assert.True(t, ok)
	assert.Equal(t, "hello", val)

	// Empty string
	val, ok = parser.ParseString("")
	assert.True(t, ok)
	assert.Equal(t, "", val)

	// Non-string type
	val, ok = parser.ParseString(int64(42))
	assert.False(t, ok)
	assert.Equal(t, "", val)

	// Nil
	val, ok = parser.ParseString(nil)
	assert.False(t, ok)
	assert.Equal(t, "", val)
}

func TestPgRowParser_ParseBool(t *testing.T) {
	parser := pgx_driver.PgxDriverDialect{}.GetRowParser()

	// True
	val, ok := parser.ParseBool(true)
	assert.True(t, ok)
	assert.True(t, val)

	// False
	val, ok = parser.ParseBool(false)
	assert.True(t, ok)
	assert.False(t, val)

	// Non-bool type
	val, ok = parser.ParseBool("true")
	assert.False(t, ok)
	assert.False(t, val)

	// Int64 (not supported by PG parser)
	val, ok = parser.ParseBool(int64(1))
	assert.False(t, ok)
	assert.False(t, val)
}

func TestPgRowParser_ParseFloat64(t *testing.T) {
	parser := pgx_driver.PgxDriverDialect{}.GetRowParser()

	// Valid float
	val, ok := parser.ParseFloat64(3.14)
	assert.True(t, ok)
	assert.InDelta(t, 3.14, val, 0.001)

	// Zero
	val, ok = parser.ParseFloat64(0.0)
	assert.True(t, ok)
	assert.Equal(t, 0.0, val)

	// Non-float type
	val, ok = parser.ParseFloat64("3.14")
	assert.False(t, ok)
	assert.Equal(t, 0.0, val)
}

func TestPgRowParser_ParseTime(t *testing.T) {
	parser := pgx_driver.PgxDriverDialect{}.GetRowParser()

	// Valid time
	now := time.Now()
	val, ok := parser.ParseTime(now)
	assert.True(t, ok)
	assert.Equal(t, now, val)

	// Non-time type
	val, ok = parser.ParseTime("2024-01-01")
	assert.False(t, ok)
	assert.True(t, val.IsZero())
}

func TestPgRowParser_ParseInt64(t *testing.T) {
	parser := pgx_driver.PgxDriverDialect{}.GetRowParser()

	// Valid int64
	val, ok := parser.ParseInt64(int64(42))
	assert.True(t, ok)
	assert.Equal(t, int64(42), val)

	// Zero
	val, ok = parser.ParseInt64(int64(0))
	assert.True(t, ok)
	assert.Equal(t, int64(0), val)

	// Non-int64 type
	val, ok = parser.ParseInt64("42")
	assert.False(t, ok)
	assert.Equal(t, int64(0), val)
}

func TestPgxDriverDialect_GetRowParser(t *testing.T) {
	dialect := &pgx_driver.PgxDriverDialect{}
	parser := dialect.GetRowParser()
	assert.NotNil(t, parser)
	assert.Equal(t, pgx_driver.PgxDriverDialect{}.GetRowParser(), parser)
}

// Test PG row parser with mock rows to simulate topology query parsing.
func TestPgRowParser_TopologyRowParsing(t *testing.T) {
	parser := pgx_driver.PgxDriverDialect{}.GetRowParser()

	// Simulate a topology row: server_id (string), is_writer (bool), cpu (float64), lag (float64), last_update_timestamp (time.Time)
	now := time.Now()
	row := []driver.Value{"instance-1", true, 25.5, 0.5, now}

	serverId, ok := parser.ParseString(row[0])
	assert.True(t, ok)
	assert.Equal(t, "instance-1", serverId)

	isWriter, ok := parser.ParseBool(row[1])
	assert.True(t, ok)
	assert.True(t, isWriter)

	cpu, ok := parser.ParseFloat64(row[2])
	assert.True(t, ok)
	assert.InDelta(t, 25.5, cpu, 0.001)

	lag, ok := parser.ParseFloat64(row[3])
	assert.True(t, ok)
	assert.InDelta(t, 0.5, lag, 0.001)

	timestamp, ok := parser.ParseTime(row[4])
	assert.True(t, ok)
	assert.Equal(t, now, timestamp)
}

// Test PG row parser with reader row.
func TestPgRowParser_ReaderRowParsing(t *testing.T) {
	parser := pgx_driver.PgxDriverDialect{}.GetRowParser()

	now := time.Now()
	row := []driver.Value{"reader-instance", false, 10.0, 2.5, now}

	serverId, ok := parser.ParseString(row[0])
	assert.True(t, ok)
	assert.Equal(t, "reader-instance", serverId)

	isWriter, ok := parser.ParseBool(row[1])
	assert.True(t, ok)
	assert.False(t, isWriter)

	cpu, ok := parser.ParseFloat64(row[2])
	assert.True(t, ok)
	assert.InDelta(t, 10.0, cpu, 0.001)

	lag, ok := parser.ParseFloat64(row[3])
	assert.True(t, ok)
	assert.InDelta(t, 2.5, lag, 0.001)

	timestamp, ok := parser.ParseTime(row[4])
	assert.True(t, ok)
	assert.Equal(t, now, timestamp)
}
