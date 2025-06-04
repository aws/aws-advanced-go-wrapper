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
	"fmt"
	"github.com/aws/aws-advanced-go-wrapper/awssql/property_util"
	"github.com/aws/aws-advanced-go-wrapper/pgx-driver"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPrepareDsn(t *testing.T) {
	driverDialect := &pgx_driver.PgxDriverDialect{}

	properties := map[string]string{
		property_util.USER.Name:     "user",
		property_util.PASSWORD.Name: "password",
		property_util.PORT.Name:     "5432",
		property_util.HOST.Name:     "host",
		property_util.DATABASE.Name: "dbName",
		property_util.PLUGINS.Name:  "test",
		"monitoring-user":           "monitor-user",
	}

	dsn := driverDialect.PrepareDsn(properties, nil)
	res, _ := regexp.MatchString("^\\w+=\\w+( \\w+=\\w+)*$", dsn)
	assert.True(t, res)
	assert.True(t, strings.Contains(dsn, fmt.Sprintf("%s=user", property_util.USER.Name)))
	assert.True(t, strings.Contains(dsn, fmt.Sprintf("%s=password", property_util.PASSWORD.Name)))
	assert.True(t, strings.Contains(dsn, fmt.Sprintf("%s=5432", property_util.PORT.Name)))
	assert.True(t, strings.Contains(dsn, fmt.Sprintf("%s=host", property_util.HOST.Name)))
	assert.True(t, strings.Contains(dsn, fmt.Sprintf("%s=dbName", property_util.DATABASE.Name)))
	assert.False(t, strings.Contains(dsn, fmt.Sprintf("%s=test", property_util.PLUGINS.Name)))
	assert.False(t, strings.Contains(dsn, "monitor-user"))
}
