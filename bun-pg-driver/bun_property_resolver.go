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
	"strconv"

	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/driver_infrastructure"
)

var defaultPropertyResolver driver_infrastructure.DriverPropertyResolver = &bunPgPropertyResolver{}

// pgdriver DSN parameter names for the timeouts the wrapper needs to control.
const (
	BUN_PG_CONNECT_TIMEOUT_PARAM = "connect_timeout"
	BUN_PG_SOCKET_TIMEOUT_PARAM  = "read_timeout"
)

// bunPgPropertyResolver maps the wrapper's timeout keys onto bun's DSN parameters.
//
// pgdriver does accept these in the DSN - parseDSN reads connect_timeout,
// dial_timeout, read_timeout, write_timeout and timeout - so the wrapper can set
// them the same way it does for pgx and MySQL. Without this the wrapper cannot
// bound its own monitoring connections at all, and pgdriver's defaults apply: a
// 10 second read deadline that silently truncates any longer query.
type bunPgPropertyResolver struct{}

func (p *bunPgPropertyResolver) GetPropertyName(key driver_infrastructure.DriverPropertyKey) string {
	switch key {
	case driver_infrastructure.ConnectTimeout:
		return BUN_PG_CONNECT_TIMEOUT_PARAM
	case driver_infrastructure.SocketTimeout:
		return BUN_PG_SOCKET_TIMEOUT_PARAM
	}
	return ""
}

// FormatValue converts milliseconds to the whole seconds pgdriver expects.
//
// A bare integer is parsed as seconds. It must not round down to zero: pgdriver
// maps any value of zero or less to an already-expired deadline rather than to
// "no timeout", so a sub-second setting would make every read fail immediately.
func (p *bunPgPropertyResolver) FormatValue(_ driver_infrastructure.DriverPropertyKey, valueMs int) string {
	if valueMs <= 0 {
		return "0"
	}
	seconds := valueMs / 1000
	if seconds < 1 {
		seconds = 1
	}
	return strconv.Itoa(seconds)
}

func (p *bunPgPropertyResolver) CreateProps(opts ...driver_infrastructure.DriverPropertyOption) map[string]string {
	props := make(map[string]string)
	for _, opt := range opts {
		name := p.GetPropertyName(opt.Key)
		if name == "" {
			continue
		}
		// A non-positive request means "unset" here rather than "expire immediately";
		// leaving the parameter out lets pgdriver's own default stand.
		if opt.Value <= 0 {
			continue
		}
		props[name] = p.FormatValue(opt.Key, opt.Value)
	}
	return props
}
