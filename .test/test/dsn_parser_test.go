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
	"strings"
	"testing"

	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/host_info_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/property_util"
	"github.com/stretchr/testify/assert"
)

func TestGetHostsFromDsnWithPgxDsnUrl(t *testing.T) {
	dsn := "postgres://someUser:somePassword@localhost:5432/pgx_test?sslmode=disable&foo=bar&customEndpoint=https://someendpoint.com:3456"
	hosts, err := property_util.GetHostsFromDsn(dsn, true)

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
	dsn := "postgres://someUser:somePassword@localhost:5432/pgx_test?sslmode=disable&foo=bar&customEndpoint=https://someendpoint.com:3456"
	props, err := property_util.ParseDsn(dsn)

	if err != nil {
		t.Errorf(`Unexpected error when calling ParseDsn: %s, Error: %q`, dsn, err)
	}

	assert.Equal(t, "postgresql", property_util.DRIVER_PROTOCOL.Get(props))
	assert.Equal(t, "someUser", property_util.USER.Get(props))
	assert.Equal(t, "somePassword", property_util.PASSWORD.Get(props))
	assert.Equal(t, "localhost", property_util.HOST.Get(props))
	assert.Equal(t, "5432", property_util.PORT.Get(props))
	assert.Equal(t, "pgx_test", property_util.DATABASE.Get(props))
	assert.Equal(t, "disable", GetValueOrEmptyString(props, "sslmode"))
	assert.Equal(t, "bar", GetValueOrEmptyString(props, "foo"))
	assert.Equal(t, "https://someendpoint.com:3456", GetValueOrEmptyString(props, "customEndpoint"))
}

func TestParseDsnPgxUrlNoPort(t *testing.T) {
	dsn := "postgres://someUser:somePassword@localhost/pgx_test"
	props, err := property_util.ParseDsn(dsn)

	if err != nil {
		t.Errorf(`Unexpected error when calling ParseDsn: %s, Error: %q`, dsn, err)
	}

	assert.Equal(t, "postgresql", property_util.DRIVER_PROTOCOL.Get(props))
	assert.Equal(t, "someUser", property_util.USER.Get(props))
	assert.Equal(t, "somePassword", property_util.PASSWORD.Get(props))
	assert.Equal(t, "localhost", property_util.HOST.Get(props))
	assert.Equal(t, "pgx_test", property_util.DATABASE.Get(props))
}

func TestParseDsnPgxUrlNoDb(t *testing.T) {
	dsn := "postgres://someUser:somePassword@localhost:5432"
	props, err := property_util.ParseDsn(dsn)

	if err != nil {
		t.Errorf(`Unexpected error when calling ParseDsn: %s, Error: %q`, dsn, err)
	}

	assert.Equal(t, "postgresql", property_util.DRIVER_PROTOCOL.Get(props))
	assert.Equal(t, "someUser", property_util.USER.Get(props))
	assert.Equal(t, "somePassword", property_util.PASSWORD.Get(props))
	assert.Equal(t, "5432", property_util.PORT.Get(props))
	assert.Equal(t, "localhost", property_util.HOST.Get(props))

	dsnTrailingSlash := "postgres://someUser:somePassword@localhost:5432/"
	props, err = property_util.ParseDsn(dsnTrailingSlash)

	if err != nil {
		t.Errorf(`Unexpected error when calling ParseDsn: %s, Error: %q`, dsn, err)
	}

	assert.Equal(t, "postgresql", property_util.DRIVER_PROTOCOL.Get(props))
	assert.Equal(t, "someUser", property_util.USER.Get(props))
	assert.Equal(t, "somePassword", property_util.PASSWORD.Get(props))
	assert.Equal(t, "5432", property_util.PORT.Get(props))
	assert.Equal(t, "localhost", property_util.HOST.Get(props))
}

func TestParseDsnPgxUrlNoPortNoDb(t *testing.T) {
	dsn := "postgres://someUser:somePassword@localhost"
	props, err := property_util.ParseDsn(dsn)

	if err != nil {
		t.Errorf(`Unexpected error when calling ParseDsn: %s, Error: %q`, dsn, err)
	}

	assert.Equal(t, "postgresql", property_util.DRIVER_PROTOCOL.Get(props))
	assert.Equal(t, "someUser", property_util.USER.Get(props))
	assert.Equal(t, "somePassword", property_util.PASSWORD.Get(props))
	assert.Equal(t, "localhost", property_util.HOST.Get(props))

	dsnWithTrailingSlash := "postgres://someUser:somePassword@localhost/"
	props, err = property_util.ParseDsn(dsnWithTrailingSlash)

	if err != nil {
		t.Errorf(`Unexpected error when calling ParseDsn: %s, Error: %q`, dsn, err)
	}

	assert.Equal(t, "postgresql", property_util.DRIVER_PROTOCOL.Get(props))
	assert.Equal(t, "someUser", property_util.USER.Get(props))
	assert.Equal(t, "somePassword", property_util.PASSWORD.Get(props))
	assert.Equal(t, "localhost", property_util.HOST.Get(props))
}

func TestParseDsnPgxUrlWithoutParams(t *testing.T) {
	dsn := "postgres://someUser:somePassword@localhost:5432/pgx_test"
	props, err := property_util.ParseDsn(dsn)

	if err != nil {
		t.Errorf(`Unexpected error when calling ParseDsn: %s, Error: %q`, dsn, err)
	}

	assert.Equal(t, "postgresql", property_util.DRIVER_PROTOCOL.Get(props))
	assert.Equal(t, "someUser", property_util.USER.Get(props))
	assert.Equal(t, "somePassword", property_util.PASSWORD.Get(props))
	assert.Equal(t, "localhost", property_util.HOST.Get(props))
	assert.Equal(t, "5432", property_util.PORT.Get(props))
	assert.Equal(t, "pgx_test", property_util.DATABASE.Get(props))
}

func TestParseDsnPgxUrlWithTrailingSpace(t *testing.T) {
	dsn := "postgres://someUser:somePassword@localhost:5432/pgx_test?sslmode=disable&foo=bar&customEndpoint=https://someendpoint.com:3456&randomNum=4    "
	props, err := property_util.ParseDsn(dsn)

	if err != nil {
		t.Errorf(`Unexpected error when calling ParseDsn: %s, Error: %q`, dsn, err)
	}

	assert.Equal(t, "postgresql", property_util.DRIVER_PROTOCOL.Get(props))
	assert.Equal(t, "someUser", property_util.USER.Get(props))
	assert.Equal(t, "somePassword", property_util.PASSWORD.Get(props))
	assert.Equal(t, "localhost", property_util.HOST.Get(props))
	assert.Equal(t, "5432", property_util.PORT.Get(props))
	assert.Equal(t, "pgx_test", property_util.DATABASE.Get(props))
	assert.Equal(t, "disable", GetValueOrEmptyString(props, "sslmode"))
	assert.Equal(t, "bar", GetValueOrEmptyString(props, "foo"))
	assert.Equal(t, "https://someendpoint.com:3456", GetValueOrEmptyString(props, "customEndpoint"))
	assert.Equal(t, "4", GetValueOrEmptyString(props, "randomNum"))
}

func TestParsePgxUrlEndpointWithTrailingDot(t *testing.T) {
	dsnWithTrailingDot := "postgres://someUser:somePassword@mydatabase.com.:5432/pgx_test?foo=bar&pop=snap&customEndpoint=https://someendpoint.com:3456"
	props, err := property_util.ParseDsn(dsnWithTrailingDot)

	if err != nil {
		t.Errorf(`Unexpected error when calling ParseDsn: %s, Error: %q`, dsnWithTrailingDot, err)
	}

	assert.Equal(t, "postgresql", property_util.DRIVER_PROTOCOL.Get(props))
	assert.Equal(t, "someUser", property_util.USER.Get(props))
	assert.Equal(t, "somePassword", property_util.PASSWORD.Get(props))
	assert.Equal(t, "mydatabase.com.", property_util.HOST.Get(props))
	assert.Equal(t, "5432", property_util.PORT.Get(props))
	assert.Equal(t, "pgx_test", property_util.DATABASE.Get(props))
	assert.Equal(t, "bar", GetValueOrEmptyString(props, "foo"))
	assert.Equal(t, "snap", GetValueOrEmptyString(props, "pop"))
	assert.Equal(t, "https://someendpoint.com:3456", GetValueOrEmptyString(props, "customEndpoint"))
}

func TestParseDsnPgxKeyValue(t *testing.T) {
	dsn := "user=someUser password=somePassword host=localhost port=5432 database=pgx_test sslmode=disable foo=bar customEndpoint=https://someendpoint.com:3456"
	props, err := property_util.ParseDsn(dsn)

	if err != nil {
		t.Errorf(`Unexpected error when calling ParseDsn: %s, Error: %q`, dsn, err)
	}

	assert.Equal(t, "postgresql", property_util.DRIVER_PROTOCOL.Get(props))
	assert.Equal(t, "someUser", property_util.USER.Get(props))
	assert.Equal(t, "somePassword", property_util.PASSWORD.Get(props))
	assert.Equal(t, "localhost", property_util.HOST.Get(props))
	assert.Equal(t, "5432", property_util.PORT.Get(props))
	assert.Equal(t, "pgx_test", property_util.DATABASE.Get(props))
	assert.Equal(t, "disable", GetValueOrEmptyString(props, "sslmode"))
	assert.Equal(t, "bar", GetValueOrEmptyString(props, "foo"))
	assert.Equal(t, "https://someendpoint.com:3456", GetValueOrEmptyString(props, "customEndpoint"))
}

func TestParseDsnPgxKeyValueWithTrailingSpace(t *testing.T) {
	dsn := "user=someUser password=somePassword host=localhost port=5432 database=pgx_test sslmode=disable foo=bar customEndpoint=https://someendpoint.com:3456    "
	props, err := property_util.ParseDsn(dsn)

	if err != nil {
		t.Errorf(`Unexpected error when calling ParseDsn: %s, Error: %q`, dsn, err)
	}

	assert.Equal(t, "postgresql", property_util.DRIVER_PROTOCOL.Get(props))
	assert.Equal(t, "someUser", property_util.USER.Get(props))
	assert.Equal(t, "somePassword", property_util.PASSWORD.Get(props))
	assert.Equal(t, "localhost", property_util.HOST.Get(props))
	assert.Equal(t, "5432", property_util.PORT.Get(props))
	assert.Equal(t, "pgx_test", property_util.DATABASE.Get(props))
	assert.Equal(t, "disable", GetValueOrEmptyString(props, "sslmode"))
	assert.Equal(t, "bar", GetValueOrEmptyString(props, "foo"))
	assert.Equal(t, "https://someendpoint.com:3456", GetValueOrEmptyString(props, "customEndpoint"))
}

func TestParseDsnPgxKeyValueWithPathInParams(t *testing.T) {
	dsn := "user=someUser password=somePassword host=localhost port=5432 database=pgx_test sslmode=verify-full sslrootcert=/Users/myuser/mywork/root.pem"
	props, err := property_util.ParseDsn(dsn)

	if err != nil {
		t.Errorf(`Unexpected error when calling ParseDsn: %s, Error: %q`, dsn, err)
	}

	assert.Equal(t, "postgresql", property_util.DRIVER_PROTOCOL.Get(props))
	assert.Equal(t, "someUser", property_util.USER.Get(props))
	assert.Equal(t, "somePassword", property_util.PASSWORD.Get(props))
	assert.Equal(t, "localhost", property_util.HOST.Get(props))
	assert.Equal(t, "5432", property_util.PORT.Get(props))
	assert.Equal(t, "pgx_test", property_util.DATABASE.Get(props))
	assert.Equal(t, "verify-full", GetValueOrEmptyString(props, "sslmode"))
	assert.Equal(t, "/Users/myuser/mywork/root.pem", GetValueOrEmptyString(props, "sslrootcert"))
}

func TestParsePgxKeyValueEndpointWithTrailingDot(t *testing.T) {
	dsnWithTrailingDot := "user=someUser password=somePassword host=mydatabase.com. port=5432 database=pgx_test foo=bar pop=snap customEndpoint=https://someendpoint.com:3456"
	props, err := property_util.ParseDsn(dsnWithTrailingDot)

	if err != nil {
		t.Errorf(`Unexpected error when calling ParseDsn: %s, Error: %q`, dsnWithTrailingDot, err)
	}

	assert.Equal(t, "postgresql", property_util.DRIVER_PROTOCOL.Get(props))
	assert.Equal(t, "someUser", property_util.USER.Get(props))
	assert.Equal(t, "somePassword", property_util.PASSWORD.Get(props))
	assert.Equal(t, "mydatabase.com.", property_util.HOST.Get(props))
	assert.Equal(t, "5432", property_util.PORT.Get(props))
	assert.Equal(t, "pgx_test", property_util.DATABASE.Get(props))
	assert.Equal(t, "bar", GetValueOrEmptyString(props, "foo"))
	assert.Equal(t, "snap", GetValueOrEmptyString(props, "pop"))
	assert.Equal(t, "https://someendpoint.com:3456", GetValueOrEmptyString(props, "customEndpoint"))
}

func TestParsePgxKeyValueEndpointWithTrailingSlash(t *testing.T) {
	dsn := "user=someUser password=somePassword host=mydatabase.com/ port=5432 database=pgx_test foo=bar pop=snap customEndpoint=https://someendpoint.com:3456"
	props, err := property_util.ParseDsn(dsn)

	if err != nil {
		t.Errorf(`Unexpected error when calling ParseDsn: %s, Error: %q`, dsn, err)
	}

	assert.Equal(t, "postgresql", property_util.DRIVER_PROTOCOL.Get(props))
	assert.Equal(t, "someUser", property_util.USER.Get(props))
	assert.Equal(t, "somePassword", property_util.PASSWORD.Get(props))
	assert.Equal(t, "mydatabase.com/", property_util.HOST.Get(props))
	assert.Equal(t, "5432", property_util.PORT.Get(props))
	assert.Equal(t, "pgx_test", property_util.DATABASE.Get(props))
	assert.Equal(t, "bar", GetValueOrEmptyString(props, "foo"))
	assert.Equal(t, "snap", GetValueOrEmptyString(props, "pop"))
	assert.Equal(t, "https://someendpoint.com:3456", GetValueOrEmptyString(props, "customEndpoint"))
}

func TestParsePgxKeyValueEndpointWithTrailingSlashDot(t *testing.T) {
	dsn := "user=someUser password=somePassword host=mydatabase.com/. port=5432 database=pgx_test foo=bar pop=snap customEndpoint=https://someendpoint.com:3456"
	props, err := property_util.ParseDsn(dsn)

	if err != nil {
		t.Errorf(`Unexpected error when calling ParseDsn: %s, Error: %q`, dsn, err)
	}

	assert.Equal(t, "postgresql", property_util.DRIVER_PROTOCOL.Get(props))
	assert.Equal(t, "someUser", property_util.USER.Get(props))
	assert.Equal(t, "somePassword", property_util.PASSWORD.Get(props))
	assert.Equal(t, "mydatabase.com/.", property_util.HOST.Get(props))
	assert.Equal(t, "5432", property_util.PORT.Get(props))
	assert.Equal(t, "pgx_test", property_util.DATABASE.Get(props))
	assert.Equal(t, "bar", GetValueOrEmptyString(props, "foo"))
	assert.Equal(t, "snap", GetValueOrEmptyString(props, "pop"))
	assert.Equal(t, "https://someendpoint.com:3456", GetValueOrEmptyString(props, "customEndpoint"))
}

func TestParseDsnMySql(t *testing.T) {
	dsn := "someUser:somePassword@tcp(mydatabase.com:3306)/myDatabase?foo=bar&pop=snap&customEndpoint=https://someendpoint.com:3456"
	props, err := property_util.ParseDsn(dsn)

	if err != nil {
		t.Errorf(`Unexpected error when calling ParseDsn: %s, Error: %q`, dsn, err)
	}

	assert.Equal(t, "mysql", property_util.DRIVER_PROTOCOL.Get(props))
	assert.Equal(t, "someUser", property_util.USER.Get(props))
	assert.Equal(t, "somePassword", property_util.PASSWORD.Get(props))
	assert.Equal(t, "mydatabase.com", property_util.HOST.Get(props))
	assert.Equal(t, "3306", property_util.PORT.Get(props))
	assert.Equal(t, "myDatabase", property_util.DATABASE.Get(props))
	assert.Equal(t, "bar", GetValueOrEmptyString(props, "foo"))
	assert.Equal(t, "snap", GetValueOrEmptyString(props, "pop"))
	assert.Equal(t, "https://someendpoint.com:3456", GetValueOrEmptyString(props, "customEndpoint"))
}

func TestParseDsnMySqlWithoutParams(t *testing.T) {
	dsn := "someUser:somePassword@tcp(mydatabase.com:3306)/myDatabase"
	props, err := property_util.ParseDsn(dsn)

	if err != nil {
		t.Errorf(`Unexpected error when calling ParseDsn: %s, Error: %q`, dsn, err)
	}

	assert.Equal(t, "tcp", property_util.NET.Get(props))
	assert.Equal(t, "mysql", property_util.DRIVER_PROTOCOL.Get(props))
	assert.Equal(t, "someUser", property_util.USER.Get(props))
	assert.Equal(t, "somePassword", property_util.PASSWORD.Get(props))
	assert.Equal(t, "mydatabase.com", property_util.HOST.Get(props))
	assert.Equal(t, "3306", property_util.PORT.Get(props))
	assert.Equal(t, "myDatabase", property_util.DATABASE.Get(props))
}

func TestParseDsnMySqlNoUserNoPassword(t *testing.T) {
	dsn := "tcp(mydatabase.com:3306)/myDatabase"
	props, err := property_util.ParseDsn(dsn)

	if err != nil {
		t.Errorf(`Unexpected error when calling ParseDsn: %s, Error: %q`, dsn, err)
	}

	assert.Equal(t, "tcp", property_util.NET.Get(props))
	assert.Equal(t, "mysql", property_util.DRIVER_PROTOCOL.Get(props))
	assert.Equal(t, "", property_util.USER.Get(props))
	assert.Equal(t, "", property_util.PASSWORD.Get(props))
	assert.Equal(t, "mydatabase.com", property_util.HOST.Get(props))
	assert.Equal(t, "3306", property_util.PORT.Get(props))
	assert.Equal(t, "myDatabase", property_util.DATABASE.Get(props))
}

func TestParseDsnMySqlWithNoPort(t *testing.T) {
	dsn := "someUser:somePassword@tcp(mydatabase.com)/myDatabase"
	props, err := property_util.ParseDsn(dsn)

	if err != nil {
		t.Errorf(`Unexpected error when calling ParseDsn: %s, Error: %q`, dsn, err)
	}

	assert.Equal(t, "mysql", property_util.DRIVER_PROTOCOL.Get(props))
	assert.Equal(t, "someUser", property_util.USER.Get(props))
	assert.Equal(t, "somePassword", property_util.PASSWORD.Get(props))
	assert.Equal(t, "mydatabase.com", property_util.HOST.Get(props))
	assert.Equal(t, "myDatabase", property_util.DATABASE.Get(props))
}

func TestParseDsnMySqlWithNoDb(t *testing.T) {
	dsn := "someUser:somePassword@tcp(mydatabase.com:3306)/"
	props, err := property_util.ParseDsn(dsn)

	if err != nil {
		t.Errorf(`Unexpected error when calling ParseDsn: %s, Error: %q`, dsn, err)
	}

	assert.Equal(t, "mysql", property_util.DRIVER_PROTOCOL.Get(props))
	assert.Equal(t, "someUser", property_util.USER.Get(props))
	assert.Equal(t, "somePassword", property_util.PASSWORD.Get(props))
	assert.Equal(t, "mydatabase.com", property_util.HOST.Get(props))
	assert.Equal(t, "3306", property_util.PORT.Get(props))
}

func TestParseMySqlWithTrailingSpace(t *testing.T) {
	dsn := "someUser:somePassword@tcp(mydatabase.com:3306)/myDatabase?foo=bar&pop=snap&numTest=4   "
	props, err := property_util.ParseDsn(dsn)

	if err != nil {
		t.Errorf(`Unexpected error when calling ParseDsn: %s, Error: %q`, dsn, err)
	}

	assert.Equal(t, "mysql", property_util.DRIVER_PROTOCOL.Get(props))
	assert.Equal(t, "someUser", property_util.USER.Get(props))
	assert.Equal(t, "somePassword", property_util.PASSWORD.Get(props))
	assert.Equal(t, "mydatabase.com", property_util.HOST.Get(props))
	assert.Equal(t, "3306", property_util.PORT.Get(props))
	assert.Equal(t, "myDatabase", property_util.DATABASE.Get(props))
	assert.Equal(t, "bar", GetValueOrEmptyString(props, "foo"))
	assert.Equal(t, "snap", GetValueOrEmptyString(props, "pop"))
	assert.Equal(t, "4", GetValueOrEmptyString(props, "numTest"))
}

func TestParseMySqlEndpointWithTrailingDot(t *testing.T) {
	dsnWithTrailingDot := "someUser:somePassword@tcp(mydatabase.com.:3306)/myDatabase?foo=bar&pop=snap&customEndpoint=https://someendpoint.com:3456"
	props, err := property_util.ParseDsn(dsnWithTrailingDot)

	if err != nil {
		t.Errorf(`Unexpected error when calling ParseDsn: %s, Error: %q`, dsnWithTrailingDot, err)
	}

	assert.Equal(t, "mysql", property_util.DRIVER_PROTOCOL.Get(props))
	assert.Equal(t, "someUser", property_util.USER.Get(props))
	assert.Equal(t, "somePassword", property_util.PASSWORD.Get(props))
	assert.Equal(t, "mydatabase.com.", property_util.HOST.Get(props))
	assert.Equal(t, "3306", property_util.PORT.Get(props))
	assert.Equal(t, "myDatabase", property_util.DATABASE.Get(props))
	assert.Equal(t, "bar", GetValueOrEmptyString(props, "foo"))
	assert.Equal(t, "snap", GetValueOrEmptyString(props, "pop"))
	assert.Equal(t, "https://someendpoint.com:3456", GetValueOrEmptyString(props, "customEndpoint"))
}

func TestParseMySqlEndpointWithTrailingSlash(t *testing.T) {
	dsnWithTrailingSlash := "someUser:somePassword@tcp(mydatabase.com/:3306)/myDatabase?foo=bar&pop=snap&customEndpoint=https://someendpoint.com:3456"
	props, err := property_util.ParseDsn(dsnWithTrailingSlash)

	if err != nil {
		t.Errorf(`Unexpected error when calling ParseDsn: %s, Error: %q`, dsnWithTrailingSlash, err)
	}

	assert.Equal(t, "mysql", property_util.DRIVER_PROTOCOL.Get(props))
	assert.Equal(t, "someUser", property_util.USER.Get(props))
	assert.Equal(t, "somePassword", property_util.PASSWORD.Get(props))
	assert.Equal(t, "mydatabase.com/", property_util.HOST.Get(props))
	assert.Equal(t, "3306", property_util.PORT.Get(props))
	assert.Equal(t, "myDatabase", property_util.DATABASE.Get(props))
	assert.Equal(t, "bar", GetValueOrEmptyString(props, "foo"))
	assert.Equal(t, "snap", GetValueOrEmptyString(props, "pop"))
	assert.Equal(t, "https://someendpoint.com:3456", GetValueOrEmptyString(props, "customEndpoint"))
}

func TestParseMySqlEndpointWithTrailingSlashDot(t *testing.T) {
	dsn := "someUser:somePassword@tcp(mydatabase.com/.:3306)/myDatabase?foo=bar&pop=snap&customEndpoint=https://someendpoint.com:3456"
	props, err := property_util.ParseDsn(dsn)

	if err != nil {
		t.Errorf(`Unexpected error when calling ParseDsn: %s, Error: %q`, dsn, err)
	}

	assert.Equal(t, "mysql", property_util.DRIVER_PROTOCOL.Get(props))
	assert.Equal(t, "someUser", property_util.USER.Get(props))
	assert.Equal(t, "somePassword", property_util.PASSWORD.Get(props))
	assert.Equal(t, "mydatabase.com/.", property_util.HOST.Get(props))
	assert.Equal(t, "3306", property_util.PORT.Get(props))
	assert.Equal(t, "myDatabase", property_util.DATABASE.Get(props))
	assert.Equal(t, "bar", GetValueOrEmptyString(props, "foo"))
	assert.Equal(t, "snap", GetValueOrEmptyString(props, "pop"))
	assert.Equal(t, "https://someendpoint.com:3456", GetValueOrEmptyString(props, "customEndpoint"))
}

func TestParseMySqlDsnWithIamToken(t *testing.T) {
	iamToken := "mydatabase.com:3306/?Action=connect&DBUser=someUser%"
	dsn := fmt.Sprintf("someUser:%s@tcp(mydatabase.com:3306)/myDatabase?foo=bar&pop=snap&customEndpoint=https://someendpoint.com:3456",
		iamToken)
	props, err := property_util.ParseDsn(dsn)

	if err != nil {
		t.Errorf(`Unexpected error when calling ParseDsn: %s, Error: %q`, dsn, err)
	}

	assert.Equal(t, "mysql", property_util.DRIVER_PROTOCOL.Get(props))
	assert.Equal(t, "someUser", property_util.USER.Get(props))
	assert.Equal(t, iamToken, property_util.PASSWORD.Get(props))
	assert.Equal(t, "mydatabase.com", property_util.HOST.Get(props))
	assert.Equal(t, "3306", property_util.PORT.Get(props))
	assert.Equal(t, "myDatabase", property_util.DATABASE.Get(props))
	assert.Equal(t, "bar", GetValueOrEmptyString(props, "foo"))
	assert.Equal(t, "snap", GetValueOrEmptyString(props, "pop"))
	assert.Equal(t, "https://someendpoint.com:3456", GetValueOrEmptyString(props, "customEndpoint"))
}

func TestGetHostsFromDsnWithMultipleHosts(t *testing.T) {
	testDsns := []string{
		"user=someUser password=somePassword host=host1,host2%s database=pgx_test",
		"postgres://someUser:somePassword@host1,host2%s/pgx_test",
		"someUser:somePassword@tcp(host1,host2%s)/myDatabase",
	}
	strBeforePort := []string{" port=", ":", ":"}

	for i, testDsn := range testDsns {
		GetHostsFromDsnWithMultipleHostsNoPort(testDsn, t)
		GetHostsFromDsnWithMultipleHostsOnePort(testDsn, strBeforePort[i], t)
		GetHostsFromDsnWithMultipleHostsMultiplePorts(testDsn, strBeforePort[i], t)
	}
}

func GetHostsFromDsnWithMultipleHostsNoPort(dsn string, t *testing.T) {
	dsn = fmt.Sprintf(dsn, "")
	hosts, err := property_util.GetHostsFromDsn(dsn, true)
	if err != nil {
		t.Errorf(`Unexpected error when calling GetHostsFromDsn: %s, Error: %q`, dsn, err)
	}
	assert.Equal(t, 2, len(hosts))
	assert.Equal(t, host_info_util.HOST_NO_PORT, hosts[0].Port)
	assert.Equal(t, host_info_util.HOST_NO_PORT, hosts[1].Port)
	assert.Equal(t, "host1", hosts[0].Host)
	assert.Equal(t, "host2", hosts[1].Host)
}

func GetHostsFromDsnWithMultipleHostsOnePort(dsn string, strBeforePort string, t *testing.T) {
	dsn = fmt.Sprintf(dsn, strBeforePort+"1234")
	hosts, err := property_util.GetHostsFromDsn(dsn, true)
	if err != nil {
		t.Errorf(`Unexpected error when calling GetHostsFromDsn: %s, Error: %q`, dsn, err)
	}
	assert.Equal(t, 2, len(hosts))
	assert.Equal(t, 1234, hosts[0].Port)
	assert.Equal(t, 1234, hosts[1].Port)
	assert.Equal(t, "host1", hosts[0].Host)
	assert.Equal(t, "host2", hosts[1].Host)
}

func GetHostsFromDsnWithMultipleHostsMultiplePorts(dsn string, strBeforePort string, t *testing.T) {
	dsn = fmt.Sprintf(dsn, strBeforePort+"1234,5678")
	_, err := property_util.GetHostsFromDsn(dsn, true)
	if err == nil {
		t.Errorf("GetHostsFromDsn should throw an error with an invalid value for the port parameter")
	} else {
		assert.True(t, strings.Contains(err.Error(), "port"))
	}
}

func TestParseHostPortPair_ValidWriterWithPort(t *testing.T) {
	hostInfo, err := property_util.ParseHostPortPair("test.cluster-abc.us-west-2.rds.amazonaws.com:5432", 3306)
	assert.NoError(t, err)
	assert.Equal(t, "test.cluster-abc.us-west-2.rds.amazonaws.com", hostInfo.Host)
	assert.Equal(t, 5432, hostInfo.Port)
	assert.Equal(t, host_info_util.WRITER, hostInfo.Role)
}

func TestParseHostPortPair_ValidReaderWithPort(t *testing.T) {
	hostInfo, err := property_util.ParseHostPortPair("test.cluster-ro-abc.us-west-2.rds.amazonaws.com:5433", 3306)
	assert.NoError(t, err)
	assert.Equal(t, "test.cluster-ro-abc.us-west-2.rds.amazonaws.com", hostInfo.Host)
	assert.Equal(t, 5433, hostInfo.Port)
	assert.Equal(t, host_info_util.READER, hostInfo.Role)
}

func TestParseHostPortPair_NoPortProvided(t *testing.T) {
	hostInfo, err := property_util.ParseHostPortPair("test.cluster-ro-abc.us-west-2.rds.amazonaws.com", 3306)
	assert.NoError(t, err)
	assert.Equal(t, "test.cluster-ro-abc.us-west-2.rds.amazonaws.com", hostInfo.Host)
	assert.Equal(t, 3306, hostInfo.Port)
	assert.Equal(t, host_info_util.READER, hostInfo.Role)
}

func TestParseHostPortPair_InvalidPort(t *testing.T) {
	hostInfo, err := property_util.ParseHostPortPair("invalid-host:abc", 3306)
	assert.Nil(t, hostInfo)
	assert.Error(t, err)
}

func TestParseDatabaseFromDsn_PgxUrl(t *testing.T) {
	dsn := "postgres://user:pass@localhost:5432/mydb"
	db, err := property_util.ParseDatabaseFromDsn(dsn)
	assert.NoError(t, err)
	assert.Equal(t, "mydb", db)
}

func TestParseUserFromDsn_PgxUrl(t *testing.T) {
	dsn := "postgres://myuser:mypassword@localhost:5432/mydb"
	user, err := property_util.ParseUserFromDsn(dsn)
	assert.NoError(t, err)
	assert.Equal(t, "myuser", user)
}

func TestParsePasswordFromDsn_PgxUrl(t *testing.T) {
	dsn := "postgres://myuser:mypassword@localhost:5432/mydb"
	pass, err := property_util.ParsePasswordFromDsn(dsn)
	assert.NoError(t, err)
	assert.Equal(t, "mypassword", pass)
}

func TestParseDatabaseFromDsn_MySQL(t *testing.T) {
	dsn := "myuser:mypassword@tcp(localhost:3306)/mydb"
	db, err := property_util.ParseDatabaseFromDsn(dsn)
	assert.NoError(t, err)
	assert.Equal(t, "mydb", db)
}

func TestParseUserFromDsn_MySQL(t *testing.T) {
	dsn := "myuser:mypassword@tcp(localhost:3306)/mydb"
	user, err := property_util.ParseUserFromDsn(dsn)
	assert.NoError(t, err)
	assert.Equal(t, "myuser", user)
}

func TestParsePasswordFromDsn_MySQL(t *testing.T) {
	dsn := "myuser:mypassword@tcp(localhost:3306)/mydb"
	pass, err := property_util.ParsePasswordFromDsn(dsn)
	assert.NoError(t, err)
	assert.Equal(t, "mypassword", pass)
}

func TestGetProtocol_PgxUrl(t *testing.T) {
	dsn := "postgres://user:pass@localhost:5432/db"
	protocol, err := property_util.GetProtocol(dsn)
	assert.NoError(t, err)
	assert.Equal(t, property_util.PGX_DRIVER_PROTOCOL, protocol)
}

func TestGetProtocol_PgxKeyValue(t *testing.T) {
	dsn := "user=postgres password=secret host=localhost dbname=mydb"
	protocol, err := property_util.GetProtocol(dsn)
	assert.NoError(t, err)
	assert.Equal(t, property_util.PGX_DRIVER_PROTOCOL, protocol)
}

func TestGetProtocol_MySQL(t *testing.T) {
	dsn := "myuser:mypassword@tcp(localhost:3306)/mydb"
	protocol, err := property_util.GetProtocol(dsn)
	assert.NoError(t, err)
	assert.Equal(t, property_util.MYSQL_DRIVER_PROTOCOL, protocol)
}

func TestGetProtocol_Invalid(t *testing.T) {
	dsn := "user=postgres password=secret host=localhost dbname=mydb"
	protocol, err := property_util.GetProtocol(dsn)
	assert.NoError(t, err)
	assert.Equal(t, property_util.PGX_DRIVER_PROTOCOL, protocol)

	dsn = "myuser:mypassword@tcp(localhost:3306)/mydb"
	protocol, err = property_util.GetProtocol(dsn)
	assert.NoError(t, err)
	assert.Equal(t, property_util.MYSQL_DRIVER_PROTOCOL, protocol)

	dsn = "postgres://user:pass@localhost:5432/db"
	protocol, err = property_util.GetProtocol(dsn)
	assert.NoError(t, err)
	assert.Equal(t, property_util.PGX_DRIVER_PROTOCOL, protocol)
}

// TestGetProtocolSupportedFormats covers every connection-string form the two
// target drivers document, so that a change to one detection pattern cannot
// quietly claim a DSN belonging to the other format.
//
// The PostgreSQL cases come from the keyword/value and URL examples in the pgx
// ParseConfig documentation, the parameter keywords it lists, and the grammar its
// keyword/value parser implements. The MySQL cases come from the go-sql-driver
// "DSN (Data Source Name)" section and its Examples.
func TestGetProtocolSupportedFormats(t *testing.T) {
	testCases := []struct {
		name     string
		dsn      string
		protocol string
	}{
		// PostgreSQL URL form.
		{"pg url", "postgres://jack:secret@pg.example.com:5432/mydb?sslmode=verify-ca", property_util.PGX_DRIVER_PROTOCOL},
		{"pg url alternate scheme", "postgresql://jack:secret@pg.example.com:5432/mydb", property_util.PGX_DRIVER_PROTOCOL},
		{"pg url multiple hosts", "postgres://jack:secret@foo.example.com:5432,bar.example.com:5432/mydb", property_util.PGX_DRIVER_PROTOCOL},
		{"pg url no port", "postgres://jack:secret@pg.example.com/mydb", property_util.PGX_DRIVER_PROTOCOL},
		{"pg url no database", "postgres://jack:secret@pg.example.com:5432", property_util.PGX_DRIVER_PROTOCOL},
		{"pg url no credentials", "postgres://pg.example.com:5432/mydb", property_util.PGX_DRIVER_PROTOCOL},

		// PostgreSQL keyword/value form.
		{"pg keyword value", "user=jack password=secret host=pg.example.com port=5432 dbname=mydb sslmode=verify-ca", property_util.PGX_DRIVER_PROTOCOL},
		{"pg keyword value single keyword", "dbname=mydb", property_util.PGX_DRIVER_PROTOCOL},
		{"pg keyword value host only", "host=localhost", property_util.PGX_DRIVER_PROTOCOL},
		{"pg keyword value multiple hosts", "host=host1,host2 port=5432,5433 dbname=mydb", property_util.PGX_DRIVER_PROTOCOL},
		{"pg keyword value unix socket directory", "host=/var/run/postgresql dbname=mydb", property_util.PGX_DRIVER_PROTOCOL},
		{"pg keyword value file paths", "host=h sslcert=/tmp/c.pem sslkey=/tmp/k.pem sslrootcert=/tmp/r.pem passfile=/tmp/pgpass", property_util.PGX_DRIVER_PROTOCOL},
		{"pg keyword value service file", "host=h service=svc servicefile=/tmp/svc", property_util.PGX_DRIVER_PROTOCOL},
		{"pg keyword value timeouts and attrs", "host=h connect_timeout=10 target_session_attrs=read-write", property_util.PGX_DRIVER_PROTOCOL},
		{"pg keyword value protocol versions", "host=h min_protocol_version=3.0 max_protocol_version=3.2", property_util.PGX_DRIVER_PROTOCOL},
		{"pg keyword value auth parameters", "host=h channel_binding=require require_auth=scram-sha-256 sslnegotiation=postgres", property_util.PGX_DRIVER_PROTOCOL},
		{"pg keyword value kerberos", "host=h krbsrvname=postgres krbspn=spn sslsni=1", property_util.PGX_DRIVER_PROTOCOL},
		{"pg keyword value application name", "host=h application_name=myapp", property_util.PGX_DRIVER_PROTOCOL},
		{"pg keyword value runtime parameter", "host=h dbname=d plpgsql.check_asserts=on", property_util.PGX_DRIVER_PROTOCOL},
		{"pg keyword value monitoring prefix", "host=h connect_timeout=30 monitoring-connect_timeout=10", property_util.PGX_DRIVER_PROTOCOL},
		{"pg keyword value blue green monitoring prefix", "host=h blue-green-monitoring-connect_timeout=10", property_util.PGX_DRIVER_PROTOCOL},
		{"pg keyword value limitless monitor prefix", "host=h limitless-router-monitor-connect_timeout=10", property_util.PGX_DRIVER_PROTOCOL},

		// PostgreSQL keyword/value grammar details.
		{"pg quoted value with space", "host=localhost password='my secret' dbname=mydb", property_util.PGX_DRIVER_PROTOCOL},
		{"pg backslash escaped space", `host=localhost password=my\ secret dbname=mydb`, property_util.PGX_DRIVER_PROTOCOL},
		{"pg escaped quote in value", `host=localhost password='it\'s' dbname=mydb`, property_util.PGX_DRIVER_PROTOCOL},
		{"pg whitespace around equals", "host = localhost dbname = mydb", property_util.PGX_DRIVER_PROTOCOL},
		{"pg leading and trailing whitespace", "  host=localhost dbname=mydb  ", property_util.PGX_DRIVER_PROTOCOL},
		{"pg empty value", "host=localhost password=", property_util.PGX_DRIVER_PROTOCOL},
		{"pg empty quoted value", "host=localhost password=''", property_util.PGX_DRIVER_PROTOCOL},
		{"pg at sign in value", "host=localhost password=p@ss dbname=mydb", property_util.PGX_DRIVER_PROTOCOL},
		{"pg url in value", "host=localhost customEndpoint=https://endpoint.example.com:3456", property_util.PGX_DRIVER_PROTOCOL},

		// MySQL form.
		{"mysql fullest form", "username:password@protocol(address)/dbname?param=value", property_util.MYSQL_DRIVER_PROTOCOL},
		{"mysql database only", "/mydb", property_util.MYSQL_DRIVER_PROTOCOL},
		{"mysql no database", "/", property_util.MYSQL_DRIVER_PROTOCOL},
		{"mysql escaped database", "/dbname%2Fwithslash", property_util.MYSQL_DRIVER_PROTOCOL},
		{"mysql tcp with port", "user:password@tcp(localhost:5555)/mydb?tls=skip-verify&autocommit=true", property_util.MYSQL_DRIVER_PROTOCOL},
		{"mysql tcp without port", "user:password@tcp(localhost)/mydb", property_util.MYSQL_DRIVER_PROTOCOL},
		{"mysql tcp without address", "user:password@tcp/mydb?charset=utf8mb4,utf8", property_util.MYSQL_DRIVER_PROTOCOL},
		{"mysql default protocol", "user:password@/mydb?sql_mode=TRADITIONAL", property_util.MYSQL_DRIVER_PROTOCOL},
		{"mysql user without password", "user@unix(/path/to/socket)/mydb", property_util.MYSQL_DRIVER_PROTOCOL},
		{"mysql unix socket", "root:pw@unix(/tmp/mysql.sock)/myDatabase?loc=Local", property_util.MYSQL_DRIVER_PROTOCOL},
		{"mysql unix socket instance path", "user:password@unix(/cloudsql/project-id:region-name:instance-name)/mydb", property_util.MYSQL_DRIVER_PROTOCOL},
		{"mysql ipv6 literal", "user:password@tcp([de:ad:be:ef::ca:fe]:80)/mydb?timeout=90s", property_util.MYSQL_DRIVER_PROTOCOL},
		{"mysql rds endpoint", "id:password@tcp(mydb.cluster-abc.us-east-2.rds.amazonaws.com:3306)/mydb", property_util.MYSQL_DRIVER_PROTOCOL},
		{"mysql multiple hosts", "user:password@tcp(host1,host2:3306)/mydb", property_util.MYSQL_DRIVER_PROTOCOL},
		{"mysql no credentials", "tcp(localhost:3306)/mydb", property_util.MYSQL_DRIVER_PROTOCOL},
		{"mysql empty dsn", "", property_util.MYSQL_DRIVER_PROTOCOL},

		// MySQL credentials need no escaping, which is what issue #587 broke.
		{"mysql space in password", "user:pa ss@tcp(localhost:3306)/mydb?parseTime=true", property_util.MYSQL_DRIVER_PROTOCOL},
		{"mysql space in user", "us er:pass@tcp(localhost:3306)/mydb?parseTime=true", property_util.MYSQL_DRIVER_PROTOCOL},
		{"mysql space in password without parameters", "user:pa ss@tcp(localhost:3306)/mydb", property_util.MYSQL_DRIVER_PROTOCOL},
		{"mysql tab in password", "user:pa\tss@tcp(localhost:3306)/mydb", property_util.MYSQL_DRIVER_PROTOCOL},
		{"mysql space in database", "user:pass@tcp(localhost:3306)/my db", property_util.MYSQL_DRIVER_PROTOCOL},
		{"mysql space in parameter value", "user:pass@tcp(localhost:3306)/mydb?comment=a b", property_util.MYSQL_DRIVER_PROTOCOL},
		{"mysql equals in password", "user:pa=ss@tcp(localhost:3306)/mydb", property_util.MYSQL_DRIVER_PROTOCOL},
		{"mysql at sign in password", "user:p@ss@tcp(localhost:3306)/mydb", property_util.MYSQL_DRIVER_PROTOCOL},
		{"mysql slash in password", "user:pa/ss@tcp(localhost:3306)/mydb", property_util.MYSQL_DRIVER_PROTOCOL},
		{"mysql iam token as password", "user:mydb.example.com:3306/?Action=connect&DBUser=user%@tcp(mydb.example.com:3306)/mydb", property_util.MYSQL_DRIVER_PROTOCOL},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			protocol, err := property_util.GetProtocol(testCase.dsn)
			assert.NoError(t, err)
			assert.Equal(t, testCase.protocol, protocol)

			props, err := property_util.ParseDsn(testCase.dsn)
			assert.NoError(t, err)
			assert.Equal(t, testCase.protocol, property_util.DRIVER_PROTOCOL.Get(props))
		})
	}
}

func TestParseDsnMySqlWithSpaceInPassword(t *testing.T) {
	dsn := "user:pa ss@tcp(myhost:3306)/db?parseTime=true"
	props, err := property_util.ParseDsn(dsn)

	assert.NoError(t, err)
	assert.Equal(t, property_util.MYSQL_DRIVER_PROTOCOL, property_util.DRIVER_PROTOCOL.Get(props))
	assert.Equal(t, "user", property_util.USER.Get(props))
	assert.Equal(t, "pa ss", property_util.PASSWORD.Get(props))
	assert.Equal(t, "myhost", property_util.HOST.Get(props))
	assert.Equal(t, "3306", property_util.PORT.Get(props))
	assert.Equal(t, "db", property_util.DATABASE.Get(props))
	assert.Equal(t, "true", GetValueOrEmptyString(props, "parseTime"))
}

func TestMaskSensitiveInfoFromDsnMySqlWithSpaceInPassword(t *testing.T) {
	maskedDsn := property_util.MaskSensitiveInfoFromDsn("user:pa ss@tcp(myhost:3306)/db?parseTime=true")

	assert.Equal(t, "user:***@tcp(myhost:3306)/db?parseTime=true", maskedDsn)
}

func TestParseDsnKeywordValueUnterminatedQuoteEndingInBackslash(t *testing.T) {
	// A backslash as the final byte used to slice past the end of the value.
	_, err := property_util.ParseDsn(`host=h password='secret\`)

	assert.Error(t, err)
}
