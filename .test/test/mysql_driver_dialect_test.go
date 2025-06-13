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
	"github.com/aws/aws-advanced-go-wrapper/awssql/property_util"
	"github.com/aws/aws-advanced-go-wrapper/mysql-driver"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPrepareDsnWithoutUser(t *testing.T) {
	driverDialect := &mysql_driver.MySQLDriverDialect{}

	properties := map[string]string{
		property_util.PASSWORD.Name: "password",
		property_util.PORT.Name:     "3306",
		property_util.HOST.Name:     "host",
		property_util.DATABASE.Name: "dbName",
		property_util.NET.Name:      "tcp",
		property_util.PLUGINS.Name:  "test",
		"monitoring-user":           "monitor-user",
	}

	dsn := driverDialect.PrepareDsn(properties, nil)
	assert.Equal(t, "tcp(host:3306)/dbName", dsn)
}

func TestPrepareDsnWithoutNet(t *testing.T) {
	driverDialect := &mysql_driver.MySQLDriverDialect{}

	properties := map[string]string{
		property_util.USER.Name:     "user",
		property_util.PASSWORD.Name: "password",
		property_util.PORT.Name:     "3306",
		property_util.HOST.Name:     "host",
		property_util.DATABASE.Name: "dbName",
		property_util.PLUGINS.Name:  "test",
	}

	dsn := driverDialect.PrepareDsn(properties, nil)
	assert.Equal(t, "user:password@(host:3306)/dbName", dsn)
}

func TestPrepareDsnWithEscapedDatabase(t *testing.T) {
	driverDialect := &mysql_driver.MySQLDriverDialect{}

	properties := map[string]string{
		property_util.USER.Name:     "user",
		property_util.PASSWORD.Name: "password",
		property_util.PORT.Name:     "3306",
		property_util.HOST.Name:     "host",
		property_util.DATABASE.Name: "dbName/name",
		property_util.NET.Name:      "tcp",
		property_util.PLUGINS.Name:  "test",
	}

	dsn := driverDialect.PrepareDsn(properties, nil)
	assert.Equal(t, "user:password@tcp(host:3306)/dbName%2Fname", dsn)
}

func TestPrepareDsnWithoutPasswordOrPort(t *testing.T) {
	driverDialect := &mysql_driver.MySQLDriverDialect{}

	properties := map[string]string{
		property_util.USER.Name:     "user",
		property_util.HOST.Name:     "host",
		property_util.DATABASE.Name: "dbName",
		property_util.NET.Name:      "tcp",
		property_util.PLUGINS.Name:  "test",
	}

	dsn := driverDialect.PrepareDsn(properties, nil)
	assert.Equal(t, "user@tcp(host)/dbName", dsn)
}

func TestPrepareDsnWithoutHost(t *testing.T) {
	driverDialect := &mysql_driver.MySQLDriverDialect{}

	properties := map[string]string{
		property_util.USER.Name:     "user",
		property_util.PASSWORD.Name: "password",
		property_util.DATABASE.Name: "dbName",
		property_util.NET.Name:      "tcp",
		property_util.PLUGINS.Name:  "test",
	}

	dsn := driverDialect.PrepareDsn(properties, nil)
	assert.Equal(t, "user:password@tcp/dbName", dsn)
}
