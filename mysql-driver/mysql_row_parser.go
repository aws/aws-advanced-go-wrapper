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

package mysql_driver

import (
	"database/sql/driver"
	"log/slog"
	"strconv"
	"time"

	"github.com/aws/aws-advanced-go-wrapper/awssql/driver_infrastructure"
	"github.com/aws/aws-advanced-go-wrapper/awssql/error_util"
)

var defaultRowParser driver_infrastructure.RowParser = &mySQLRowParser{}

type mySQLRowParser struct{}

func (m *mySQLRowParser) ParseString(val driver.Value) (string, bool) {
	if val == nil {
		return "", true
	}
	switch v := val.(type) {
	case string:
		return v, true
	case []uint8:
		return string(v), true
	case int64:
		return strconv.FormatInt(v, 10), true
	default:
		slog.Debug(error_util.GetMessage("mySQLRowParser.unexpectedStringValue", val))
		return "", false
	}
}

func (m *mySQLRowParser) ParseBool(val driver.Value) (bool, bool) {
	if val == nil {
		return false, true
	}
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
	if val == nil {
		return 0, true
	}
	f, ok := val.(float64)
	return f, ok
}

func (m *mySQLRowParser) ParseTime(val driver.Value) (time.Time, bool) {
	if val == nil {
		return time.Time{}, true
	}
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
	if val == nil {
		return 0, true
	}
	i, ok := val.(int64)
	return i, ok
}
