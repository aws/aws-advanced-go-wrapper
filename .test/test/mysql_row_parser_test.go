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

	"github.com/aws/aws-advanced-go-wrapper/awssql/driver_infrastructure"
	"github.com/stretchr/testify/assert"
)

func TestMySQLRowParser_ParseString(t *testing.T) {
	parser := driver_infrastructure.MySQLRowParser

	// Native string
	val, ok := parser.ParseString("hello")
	assert.True(t, ok)
	assert.Equal(t, "hello", val)

	// []uint8 (MySQL driver returns strings as byte slices)
	val, ok = parser.ParseString([]uint8("world"))
	assert.True(t, ok)
	assert.Equal(t, "world", val)

	// Empty string
	val, ok = parser.ParseString("")
	assert.True(t, ok)
	assert.Equal(t, "", val)

	// Empty []uint8
	val, ok = parser.ParseString([]uint8(""))
	assert.True(t, ok)
	assert.Equal(t, "", val)

	// int64 type (MySQL may return numeric values as int64)
	val, ok = parser.ParseString(int64(42))
	assert.True(t, ok)
	assert.Equal(t, "42", val)

	// Nil
	val, ok = parser.ParseString(nil)
	assert.False(t, ok)
	assert.Equal(t, "", val)
}

func TestMySQLRowParser_ParseBool(t *testing.T) {
	parser := driver_infrastructure.MySQLRowParser

	// Native bool true
	val, ok := parser.ParseBool(true)
	assert.True(t, ok)
	assert.True(t, val)

	// Native bool false
	val, ok = parser.ParseBool(false)
	assert.True(t, ok)
	assert.False(t, val)

	// int64(1) -> true (MySQL driver returns booleans as int64)
	val, ok = parser.ParseBool(int64(1))
	assert.True(t, ok)
	assert.True(t, val)

	// int64(0) -> false
	val, ok = parser.ParseBool(int64(0))
	assert.True(t, ok)
	assert.False(t, val)

	// int64(2) -> false (MySQL ParseBool only treats 1 as true)
	val, ok = parser.ParseBool(int64(2))
	assert.True(t, ok)
	assert.False(t, val)

	// Non-bool type
	val, ok = parser.ParseBool("true")
	assert.False(t, ok)
	assert.False(t, val)
}

func TestMySQLRowParser_ParseFloat64(t *testing.T) {
	parser := driver_infrastructure.MySQLRowParser

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

func TestMySQLRowParser_ParseTime(t *testing.T) {
	parser := driver_infrastructure.MySQLRowParser

	// Native time.Time
	now := time.Now()
	val, ok := parser.ParseTime(now)
	assert.True(t, ok)
	assert.Equal(t, now, val)

	// []uint8 timestamp string (MySQL driver format)
	timeStr := "2024-06-15 10:30:45.123456"
	val, ok = parser.ParseTime([]uint8(timeStr))
	assert.True(t, ok)
	assert.Equal(t, 2024, val.Year())
	assert.Equal(t, time.June, val.Month())
	assert.Equal(t, 15, val.Day())
	assert.Equal(t, 10, val.Hour())
	assert.Equal(t, 30, val.Minute())
	assert.Equal(t, 45, val.Second())

	// Invalid []uint8 timestamp
	val, ok = parser.ParseTime([]uint8("not-a-timestamp"))
	assert.False(t, ok)
	assert.True(t, val.IsZero())

	// Non-time type
	val, ok = parser.ParseTime("2024-01-01")
	assert.False(t, ok)
	assert.True(t, val.IsZero())

	// Nil
	val, ok = parser.ParseTime(nil)
	assert.False(t, ok)
	assert.True(t, val.IsZero())
}

func TestMySQLRowParser_ParseInt64(t *testing.T) {
	parser := driver_infrastructure.MySQLRowParser

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

func TestMySQLDatabaseDialect_GetRowParser(t *testing.T) {
	dialect := &driver_infrastructure.MySQLDatabaseDialect{}
	parser := dialect.GetRowParser()
	assert.NotNil(t, parser)
	assert.Equal(t, driver_infrastructure.MySQLRowParser, parser)
}

// Test MySQL row parser with mock rows to simulate Aurora MySQL topology query parsing.
func TestMySQLRowParser_AuroraTopologyRowParsing(t *testing.T) {
	parser := driver_infrastructure.MySQLRowParser

	// Aurora MySQL topology row: server_id ([]uint8), is_writer (int64), cpu (float64), lag (float64), last_update_timestamp ([]uint8)
	row := []driver.Value{
		[]uint8("instance-1"),
		int64(1),
		25.5,
		0.5,
		[]uint8("2024-06-15 10:30:45.123456"),
	}

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
	assert.Equal(t, 2024, timestamp.Year())
}

// Test MySQL row parser with reader row.
func TestMySQLRowParser_ReaderRowParsing(t *testing.T) {
	parser := driver_infrastructure.MySQLRowParser

	row := []driver.Value{
		[]uint8("reader-instance"),
		int64(0),
		10.0,
		2.5,
		[]uint8("2024-06-15 10:30:45.000000"),
	}

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
	assert.Equal(t, 2024, timestamp.Year())
}

// Test MySQL row parser with Multi-AZ topology row (id, endpoint).
func TestMySQLRowParser_MultiAzTopologyRowParsing(t *testing.T) {
	parser := driver_infrastructure.MySQLRowParser

	row := []driver.Value{
		[]uint8("db-ABCDEFGHIJKLMN"),
		[]uint8("instance-1.xyz.us-east-2.rds.amazonaws.com"),
	}

	id, ok := parser.ParseString(row[0])
	assert.True(t, ok)
	assert.Equal(t, "db-ABCDEFGHIJKLMN", id)

	endpoint, ok := parser.ParseString(row[1])
	assert.True(t, ok)
	assert.Equal(t, "instance-1.xyz.us-east-2.rds.amazonaws.com", endpoint)
}

// Test MySQL ParseTime with various timestamp formats.
func TestMySQLRowParser_ParseTime_Formats(t *testing.T) {
	parser := driver_infrastructure.MySQLRowParser

	// Full precision
	val, ok := parser.ParseTime([]uint8("2024-12-25 23:59:59.999999"))
	assert.True(t, ok)
	assert.Equal(t, 12, int(val.Month()))
	assert.Equal(t, 25, val.Day())

	// No microseconds
	val, ok = parser.ParseTime([]uint8("2024-01-01 00:00:00.000000"))
	assert.True(t, ok)
	assert.Equal(t, 2024, val.Year())
}
