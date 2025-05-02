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
	"awssql/host_info_util"
	"awssql/property_util"
	"awssql/utils"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetHostsFromDsnWithPgxDsnUrl(t *testing.T) {
	dsn := "postgres://someUser:somePassword@localhost:5432/pgx_test?sslmode=disable&foo=bar"
	hosts, err := utils.GetHostsFromDsn(dsn, true)

	if err != nil {
		t.Errorf(`Unexpected error when calling GetHostsFromDsn: %s, Error: %q`, dsn, err)
	}

	assert.Equal(t, 1, len(hosts))
	assert.Equal(t, "localhost", hosts[0].Host)
	assert.Equal(t, 5432, hosts[0].Port)
	assert.Equal(t, host_info_util.AVAILABLE, hosts[0].Availability)
	assert.Equal(t, host_info_util.WRITER, hosts[0].Role)
	assert.Equal(t, host_info_util.HOST_DEFAULT_WEIGHT, hosts[0].Weight)
}

func TestParseDsnPgxUrl(t *testing.T) {
	dsn := "postgres://someUser:somePassword@localhost:5432/pgx_test?sslmode=disable&foo=bar&customEndpoint=https://someendpoint.com"
	props, err := utils.ParseDsn(dsn)

	if err != nil {
		t.Errorf(`Unexpected error when calling ParseDsn: %s, Error: %q`, dsn, err)
	}

	assert.Equal(t, "postgresql", props[property_util.DRIVER_PROTOCOL.Name])
	assert.Equal(t, "someUser", props[property_util.USER.Name])
	assert.Equal(t, "somePassword", props[property_util.PASSWORD.Name])
	assert.Equal(t, "localhost", props[property_util.HOST.Name])
	assert.Equal(t, "5432", props[property_util.PORT.Name])
	assert.Equal(t, "pgx_test", props[property_util.DATABASE.Name])
	assert.Equal(t, "disable", props["sslmode"])
	assert.Equal(t, "bar", props["foo"])
	assert.Equal(t, "https://someendpoint.com", props["customEndpoint"])
}

func TestParseDsnPgxUrlWithoutParams(t *testing.T) {
	dsn := "postgres://someUser:somePassword@localhost:5432/pgx_test"
	props, err := utils.ParseDsn(dsn)

	if err != nil {
		t.Errorf(`Unexpected error when calling ParseDsn: %s, Error: %q`, dsn, err)
	}

	assert.Equal(t, "postgresql", props[property_util.DRIVER_PROTOCOL.Name])
	assert.Equal(t, "someUser", props[property_util.USER.Name])
	assert.Equal(t, "somePassword", props[property_util.PASSWORD.Name])
	assert.Equal(t, "localhost", props[property_util.HOST.Name])
	assert.Equal(t, "5432", props[property_util.PORT.Name])
	assert.Equal(t, "pgx_test", props[property_util.DATABASE.Name])
}

func TestParseDsnPgxKeyValue(t *testing.T) {
	dsn := "user=someUser password=somePassword host=localhost port=5432 database=pgx_test sslmode=disable foo=bar"
	props, err := utils.ParseDsn(dsn)

	if err != nil {
		t.Errorf(`Unexpected error when calling ParseDsn: %s, Error: %q`, dsn, err)
	}

	assert.Equal(t, "postgresql", props[property_util.DRIVER_PROTOCOL.Name])
	assert.Equal(t, "someUser", props[property_util.USER.Name])
	assert.Equal(t, "somePassword", props[property_util.PASSWORD.Name])
	assert.Equal(t, "localhost", props[property_util.HOST.Name])
	assert.Equal(t, "5432", props[property_util.PORT.Name])
	assert.Equal(t, "pgx_test", props[property_util.DATABASE.Name])
	assert.Equal(t, "disable", props["sslmode"])
	assert.Equal(t, "bar", props["foo"])
}

func TestParseDsnMySql(t *testing.T) {
	dsn := "someUser:somePassword@tcp(mydatabase.com:3306)/myDatabase?foo=bar&pop=snap&customEndpoint=https://someendpoint.com"
	props, err := utils.ParseDsn(dsn)

	if err != nil {
		t.Errorf(`Unexpected error when calling ParseDsn: %s, Error: %q`, dsn, err)
	}

	assert.Equal(t, "mysql", props[property_util.DRIVER_PROTOCOL.Name])
	assert.Equal(t, "someUser", props[property_util.USER.Name])
	assert.Equal(t, "somePassword", props[property_util.PASSWORD.Name])
	assert.Equal(t, "mydatabase.com", props[property_util.HOST.Name])
	assert.Equal(t, "3306", props[property_util.PORT.Name])
	assert.Equal(t, "myDatabase", props[property_util.DATABASE.Name])
	assert.Equal(t, "bar", props["foo"])
	assert.Equal(t, "snap", props["pop"])
	assert.Equal(t, "https://someendpoint.com", props["customEndpoint"])
}

func TestParseDsnMySqlWithoutParams(t *testing.T) {
	dsn := "someUser:somePassword@tcp(mydatabase.com:3306)/myDatabase"
	props, err := utils.ParseDsn(dsn)

	if err != nil {
		t.Errorf(`Unexpected error when calling ParseDsn: %s, Error: %q`, dsn, err)
	}

	assert.Equal(t, "mysql", props[property_util.DRIVER_PROTOCOL.Name])
	assert.Equal(t, "someUser", props[property_util.USER.Name])
	assert.Equal(t, "somePassword", props[property_util.PASSWORD.Name])
	assert.Equal(t, "mydatabase.com", props[property_util.HOST.Name])
	assert.Equal(t, "3306", props[property_util.PORT.Name])
	assert.Equal(t, "myDatabase", props[property_util.DATABASE.Name])
}
