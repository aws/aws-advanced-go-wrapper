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
	"database/sql/driver"
	"time"

	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/driver_infrastructure"
)

var defaultRowParser driver_infrastructure.RowParser = &pgxRowParser{}

type pgxRowParser struct{}

func (p *pgxRowParser) ParseString(val driver.Value) (string, bool) {
	s, ok := val.(string)
	return s, ok
}

func (p *pgxRowParser) ParseBool(val driver.Value) (bool, bool) {
	b, ok := val.(bool)
	return b, ok
}

func (p *pgxRowParser) ParseFloat64(val driver.Value) (float64, bool) {
	f, ok := val.(float64)
	return f, ok
}

func (p *pgxRowParser) ParseTime(val driver.Value) (time.Time, bool) {
	t, ok := val.(time.Time)
	return t, ok
}

func (p *pgxRowParser) ParseInt64(val driver.Value) (int64, bool) {
	i, ok := val.(int64)
	return i, ok
}
