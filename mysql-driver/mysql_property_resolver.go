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
	"fmt"

	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/driver_infrastructure"
)

var defaultPropertyResolver driver_infrastructure.DriverPropertyResolver = &mysqlPropertyResolver{}

type mysqlPropertyResolver struct{}

func (m *mysqlPropertyResolver) GetPropertyName(key driver_infrastructure.DriverPropertyKey) string {
	switch key {
	case driver_infrastructure.ConnectTimeout:
		return "timeout"
	case driver_infrastructure.SocketTimeout:
		return "readTimeout"
	default:
		return ""
	}
}

// FormatValue converts a millisecond value to the driver-specific string format.
// mysql timeout parameters accept Go duration strings (e.g. "5s", "500ms").
func (m *mysqlPropertyResolver) FormatValue(_ driver_infrastructure.DriverPropertyKey, valueMs int) string {
	if valueMs%1000 == 0 {
		return fmt.Sprintf("%ds", valueMs/1000)
	}
	return fmt.Sprintf("%dms", valueMs)
}

func (m *mysqlPropertyResolver) CreateProps(opts ...driver_infrastructure.DriverPropertyOption) map[string]string {
	props := make(map[string]string)
	for _, opt := range opts {
		name := m.GetPropertyName(opt.Key)
		if name != "" {
			props[name] = m.FormatValue(opt.Key, opt.Value)
		}
	}
	return props
}
