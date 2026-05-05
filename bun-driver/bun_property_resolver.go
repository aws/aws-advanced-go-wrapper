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
	"strconv"

	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/driver_infrastructure"
)

var defaultPropertyResolver driver_infrastructure.DriverPropertyResolver = &bunPgPropertyResolver{}

// bunPgPropertyResolver returns empty property names for all keys so the
// wrapper skips DSN-based timeout injection. bun's pgdriver configures
// timeouts via pgdriver.WithReadTimeout / pgdriver.WithWriteTimeout options
// rather than DSN parameters.
type bunPgPropertyResolver struct{}

func (p *bunPgPropertyResolver) GetPropertyName(_ driver_infrastructure.DriverPropertyKey) string {
	return ""
}

func (p *bunPgPropertyResolver) FormatValue(_ driver_infrastructure.DriverPropertyKey, valueMs int) string {
	return strconv.Itoa(valueMs)
}

func (p *bunPgPropertyResolver) CreateProps(_ ...driver_infrastructure.DriverPropertyOption) map[string]string {
	return make(map[string]string)
}
