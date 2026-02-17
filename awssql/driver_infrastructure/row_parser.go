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
	"database/sql/driver"
	"time"
)

// RowParser handles database-specific type conversions from driver.Value to Go types.
// This abstracts the differences between how PG and MySQL drivers return data.
type RowParser interface {
	// ParseString extracts a string from a driver.Value
	ParseString(val driver.Value) (string, bool)
	// ParseBool extracts a boolean from a driver.Value
	ParseBool(val driver.Value) (bool, bool)
	// ParseFloat64 extracts a float64 from a driver.Value
	ParseFloat64(val driver.Value) (float64, bool)
	// ParseTime extracts a time.Time from a driver.Value
	ParseTime(val driver.Value) (time.Time, bool)
	// ParseInt64 extracts an int64 from a driver.Value
	ParseInt64(val driver.Value) (int64, bool)
}

// Package-level singleton row parsers
var (
	PgRowParser    RowParser = &pgRowParser{}
	MySQLRowParser RowParser = &mySQLRowParser{}
)

// pgRowParser handles PostgreSQL-specific type conversions.
// pgx driver returns native Go types directly.
type pgRowParser struct{}

func (p *pgRowParser) ParseString(val driver.Value) (string, bool) {
	s, ok := val.(string)
	return s, ok
}

func (p *pgRowParser) ParseBool(val driver.Value) (bool, bool) {
	b, ok := val.(bool)
	return b, ok
}

func (p *pgRowParser) ParseFloat64(val driver.Value) (float64, bool) {
	f, ok := val.(float64)
	return f, ok
}

func (p *pgRowParser) ParseTime(val driver.Value) (time.Time, bool) {
	t, ok := val.(time.Time)
	return t, ok
}

func (p *pgRowParser) ParseInt64(val driver.Value) (int64, bool) {
	i, ok := val.(int64)
	return i, ok
}

// mySQLRowParser handles MySQL-specific type conversions.
// go-sql-driver/mysql returns []uint8 for strings and int64 for booleans.
type mySQLRowParser struct{}

func (m *mySQLRowParser) ParseString(val driver.Value) (string, bool) {
	switch v := val.(type) {
	case string:
		return v, true
	case []uint8:
		return string(v), true
	default:
		return "", false
	}
}

func (m *mySQLRowParser) ParseBool(val driver.Value) (bool, bool) {
	switch v := val.(type) {
	case bool:
		return v, true
	case int64:
		return v == 1, true
	default:
		return false, false
	}
}

func (m *mySQLRowParser) ParseFloat64(val driver.Value) (float64, bool) {
	f, ok := val.(float64)
	return f, ok
}

func (m *mySQLRowParser) ParseTime(val driver.Value) (time.Time, bool) {
	switch v := val.(type) {
	case time.Time:
		return v, true
	case []uint8:
		t, err := time.Parse("2006-01-02 15:04:05.999999", string(v))
		if err != nil {
			return time.Time{}, false
		}
		return t, true
	default:
		return time.Time{}, false
	}
}

func (m *mySQLRowParser) ParseInt64(val driver.Value) (int64, bool) {
	i, ok := val.(int64)
	return i, ok
}
