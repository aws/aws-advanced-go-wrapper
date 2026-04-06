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
	"strconv"

	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/driver_infrastructure"
)

var defaultPropertyResolver driver_infrastructure.DriverPropertyResolver = &pgxPropertyResolver{}

type pgxPropertyResolver struct{}

func (p *pgxPropertyResolver) GetPropertyName(key driver_infrastructure.DriverPropertyKey) string {
	switch key {
	case driver_infrastructure.ConnectTimeout:
		return "connect_timeout"
	case driver_infrastructure.SocketTimeout:
		return "" // pgx does not support a DSN-level socket timeout; use context instead.
	default:
		return ""
	}
}

// FormatValue converts a millisecond value to the driver-specific string format.
// pgx connect_timeout expects an integer number of seconds.
func (p *pgxPropertyResolver) FormatValue(_ driver_infrastructure.DriverPropertyKey, valueMs int) string {
	seconds := valueMs / 1000
	if seconds < 1 && valueMs > 0 {
		seconds = 1
	}
	return strconv.Itoa(seconds)
}

func (p *pgxPropertyResolver) CreateProps(opts ...driver_infrastructure.DriverPropertyOption) map[string]string {
	props := make(map[string]string)
	for _, opt := range opts {
		name := p.GetPropertyName(opt.Key)
		if name != "" {
			props[name] = p.FormatValue(opt.Key, opt.Value)
		}
	}
	return props
}
