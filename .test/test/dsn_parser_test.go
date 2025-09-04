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

	"github.com/aws/aws-advanced-go-wrapper/awssql/host_info_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/property_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils"

	"github.com/stretchr/testify/assert"
)

func TestGetHostsFromDsnWithPgxDsnUrl(t *testing.T) {
	dsn := "postgres://someUser:somePassword@localhost:5432/pgx_test?sslmode=disable&foo=bar&customEndpoint=https://someendpoint.com:3456"
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
	dsn := "postgres://someUser:somePassword@localhost:5432/pgx_test?sslmode=disable&foo=bar&customEndpoint=https://someendpoint.com:3456"
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
	assert.Equal(t, "https://someendpoint.com:3456", props["customEndpoint"])
}

func TestParseDsnPgxUrlNoPort(t *testing.T) {
	dsn := "postgres://someUser:somePassword@localhost/pgx_test"
	props, err := utils.ParseDsn(dsn)

	if err != nil {
		t.Errorf(`Unexpected error when calling ParseDsn: %s, Error: %q`, dsn, err)
	}

	assert.Equal(t, "postgresql", props[property_util.DRIVER_PROTOCOL.Name])
	assert.Equal(t, "someUser", props[property_util.USER.Name])
	assert.Equal(t, "somePassword", props[property_util.PASSWORD.Name])
	assert.Equal(t, "localhost", props[property_util.HOST.Name])
	assert.Equal(t, "pgx_test", props[property_util.DATABASE.Name])
}

func TestParseDsnPgxUrlNoDb(t *testing.T) {
	dsn := "postgres://someUser:somePassword@localhost:5432"
	props, err := utils.ParseDsn(dsn)

	if err != nil {
		t.Errorf(`Unexpected error when calling ParseDsn: %s, Error: %q`, dsn, err)
	}

	assert.Equal(t, "postgresql", props[property_util.DRIVER_PROTOCOL.Name])
	assert.Equal(t, "someUser", props[property_util.USER.Name])
	assert.Equal(t, "somePassword", props[property_util.PASSWORD.Name])
	assert.Equal(t, "5432", props[property_util.PORT.Name])
	assert.Equal(t, "localhost", props[property_util.HOST.Name])

	dsnTrailingSlash := "postgres://someUser:somePassword@localhost:5432/"
	props, err = utils.ParseDsn(dsnTrailingSlash)

	if err != nil {
		t.Errorf(`Unexpected error when calling ParseDsn: %s, Error: %q`, dsn, err)
	}

	assert.Equal(t, "postgresql", props[property_util.DRIVER_PROTOCOL.Name])
	assert.Equal(t, "someUser", props[property_util.USER.Name])
	assert.Equal(t, "somePassword", props[property_util.PASSWORD.Name])
	assert.Equal(t, "5432", props[property_util.PORT.Name])
	assert.Equal(t, "localhost", props[property_util.HOST.Name])
}

func TestParseDsnPgxUrlNoPortNoDb(t *testing.T) {
	dsn := "postgres://someUser:somePassword@localhost"
	props, err := utils.ParseDsn(dsn)

	if err != nil {
		t.Errorf(`Unexpected error when calling ParseDsn: %s, Error: %q`, dsn, err)
	}

	assert.Equal(t, "postgresql", props[property_util.DRIVER_PROTOCOL.Name])
	assert.Equal(t, "someUser", props[property_util.USER.Name])
	assert.Equal(t, "somePassword", props[property_util.PASSWORD.Name])
	assert.Equal(t, "localhost", props[property_util.HOST.Name])

	dsnWithTrailingSlash := "postgres://someUser:somePassword@localhost/"
	props, err = utils.ParseDsn(dsnWithTrailingSlash)

	if err != nil {
		t.Errorf(`Unexpected error when calling ParseDsn: %s, Error: %q`, dsn, err)
	}

	assert.Equal(t, "postgresql", props[property_util.DRIVER_PROTOCOL.Name])
	assert.Equal(t, "someUser", props[property_util.USER.Name])
	assert.Equal(t, "somePassword", props[property_util.PASSWORD.Name])
	assert.Equal(t, "localhost", props[property_util.HOST.Name])
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

func TestParseDsnPgxUrlWithTrailingSpace(t *testing.T) {
	dsn := "postgres://someUser:somePassword@localhost:5432/pgx_test?sslmode=disable&foo=bar&customEndpoint=https://someendpoint.com:3456&randomNum=4    "
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
	assert.Equal(t, "https://someendpoint.com:3456", props["customEndpoint"])
	assert.Equal(t, "4", props["randomNum"])
}

func TestParsePgxUrlEndpointWithTrailingDot(t *testing.T) {
	dsnWithTrailingDot := "postgres://someUser:somePassword@mydatabase.com.:5432/pgx_test?foo=bar&pop=snap&customEndpoint=https://someendpoint.com:3456"
	props, err := utils.ParseDsn(dsnWithTrailingDot)

	if err != nil {
		t.Errorf(`Unexpected error when calling ParseDsn: %s, Error: %q`, dsnWithTrailingDot, err)
	}

	assert.Equal(t, "postgresql", props[property_util.DRIVER_PROTOCOL.Name])
	assert.Equal(t, "someUser", props[property_util.USER.Name])
	assert.Equal(t, "somePassword", props[property_util.PASSWORD.Name])
	assert.Equal(t, "mydatabase.com.", props[property_util.HOST.Name])
	assert.Equal(t, "5432", props[property_util.PORT.Name])
	assert.Equal(t, "pgx_test", props[property_util.DATABASE.Name])
	assert.Equal(t, "bar", props["foo"])
	assert.Equal(t, "snap", props["pop"])
	assert.Equal(t, "https://someendpoint.com:3456", props["customEndpoint"])
}

func TestParseDsnPgxKeyValue(t *testing.T) {
	dsn := "user=someUser password=somePassword host=localhost port=5432 database=pgx_test sslmode=disable foo=bar customEndpoint=https://someendpoint.com:3456"
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
	assert.Equal(t, "https://someendpoint.com:3456", props["customEndpoint"])
}

func TestParseDsnPgxKeyValueWithTrailingSpace(t *testing.T) {
	dsn := "user=someUser password=somePassword host=localhost port=5432 database=pgx_test sslmode=disable foo=bar customEndpoint=https://someendpoint.com:3456    "
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
	assert.Equal(t, "https://someendpoint.com:3456", props["customEndpoint"])
}

func TestParseDsnPgxKeyValueWithPathInParams(t *testing.T) {
	dsn := "user=someUser password=somePassword host=localhost port=5432 database=pgx_test sslmode=verify-full sslrootcert=/Users/myuser/mywork/root.pem"
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
	assert.Equal(t, "verify-full", props["sslmode"])
	assert.Equal(t, "/Users/myuser/mywork/root.pem", props["sslrootcert"])
}

func TestParsePgxKeyValueEndpointWithTrailingDot(t *testing.T) {
	dsnWithTrailingDot := "user=someUser password=somePassword host=mydatabase.com. port=5432 database=pgx_test foo=bar pop=snap customEndpoint=https://someendpoint.com:3456"
	props, err := utils.ParseDsn(dsnWithTrailingDot)

	if err != nil {
		t.Errorf(`Unexpected error when calling ParseDsn: %s, Error: %q`, dsnWithTrailingDot, err)
	}

	assert.Equal(t, "postgresql", props[property_util.DRIVER_PROTOCOL.Name])
	assert.Equal(t, "someUser", props[property_util.USER.Name])
	assert.Equal(t, "somePassword", props[property_util.PASSWORD.Name])
	assert.Equal(t, "mydatabase.com.", props[property_util.HOST.Name])
	assert.Equal(t, "5432", props[property_util.PORT.Name])
	assert.Equal(t, "pgx_test", props[property_util.DATABASE.Name])
	assert.Equal(t, "bar", props["foo"])
	assert.Equal(t, "snap", props["pop"])
	assert.Equal(t, "https://someendpoint.com:3456", props["customEndpoint"])
}

func TestParsePgxKeyValueEndpointWithTrailingSlash(t *testing.T) {
	dsn := "user=someUser password=somePassword host=mydatabase.com/ port=5432 database=pgx_test foo=bar pop=snap customEndpoint=https://someendpoint.com:3456"
	props, err := utils.ParseDsn(dsn)

	if err != nil {
		t.Errorf(`Unexpected error when calling ParseDsn: %s, Error: %q`, dsn, err)
	}

	assert.Equal(t, "postgresql", props[property_util.DRIVER_PROTOCOL.Name])
	assert.Equal(t, "someUser", props[property_util.USER.Name])
	assert.Equal(t, "somePassword", props[property_util.PASSWORD.Name])
	assert.Equal(t, "mydatabase.com/", props[property_util.HOST.Name])
	assert.Equal(t, "5432", props[property_util.PORT.Name])
	assert.Equal(t, "pgx_test", props[property_util.DATABASE.Name])
	assert.Equal(t, "bar", props["foo"])
	assert.Equal(t, "snap", props["pop"])
	assert.Equal(t, "https://someendpoint.com:3456", props["customEndpoint"])
}

func TestParsePgxKeyValueEndpointWithTrailingSlashDot(t *testing.T) {
	dsn := "user=someUser password=somePassword host=mydatabase.com/. port=5432 database=pgx_test foo=bar pop=snap customEndpoint=https://someendpoint.com:3456"
	props, err := utils.ParseDsn(dsn)

	if err != nil {
		t.Errorf(`Unexpected error when calling ParseDsn: %s, Error: %q`, dsn, err)
	}

	assert.Equal(t, "postgresql", props[property_util.DRIVER_PROTOCOL.Name])
	assert.Equal(t, "someUser", props[property_util.USER.Name])
	assert.Equal(t, "somePassword", props[property_util.PASSWORD.Name])
	assert.Equal(t, "mydatabase.com/.", props[property_util.HOST.Name])
	assert.Equal(t, "5432", props[property_util.PORT.Name])
	assert.Equal(t, "pgx_test", props[property_util.DATABASE.Name])
	assert.Equal(t, "bar", props["foo"])
	assert.Equal(t, "snap", props["pop"])
	assert.Equal(t, "https://someendpoint.com:3456", props["customEndpoint"])
}

func TestParseDsnMySql(t *testing.T) {
	dsn := "someUser:somePassword@tcp(mydatabase.com:3306)/myDatabase?foo=bar&pop=snap&customEndpoint=https://someendpoint.com:3456"
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
	assert.Equal(t, "https://someendpoint.com:3456", props["customEndpoint"])
}

func TestParseDsnMySqlWithoutParams(t *testing.T) {
	dsn := "someUser:somePassword@tcp(mydatabase.com:3306)/myDatabase"
	props, err := utils.ParseDsn(dsn)

	if err != nil {
		t.Errorf(`Unexpected error when calling ParseDsn: %s, Error: %q`, dsn, err)
	}

	assert.Equal(t, "tcp", props[property_util.NET.Name])
	assert.Equal(t, "mysql", props[property_util.DRIVER_PROTOCOL.Name])
	assert.Equal(t, "someUser", props[property_util.USER.Name])
	assert.Equal(t, "somePassword", props[property_util.PASSWORD.Name])
	assert.Equal(t, "mydatabase.com", props[property_util.HOST.Name])
	assert.Equal(t, "3306", props[property_util.PORT.Name])
	assert.Equal(t, "myDatabase", props[property_util.DATABASE.Name])
}

func TestParseDsnMySqlNoUserNoPassword(t *testing.T) {
	dsn := "tcp(mydatabase.com:3306)/myDatabase"
	props, err := utils.ParseDsn(dsn)

	if err != nil {
		t.Errorf(`Unexpected error when calling ParseDsn: %s, Error: %q`, dsn, err)
	}

	assert.Equal(t, "tcp", props[property_util.NET.Name])
	assert.Equal(t, "mysql", props[property_util.DRIVER_PROTOCOL.Name])
	assert.Equal(t, "", props[property_util.USER.Name])
	assert.Equal(t, "", props[property_util.PASSWORD.Name])
	assert.Equal(t, "mydatabase.com", props[property_util.HOST.Name])
	assert.Equal(t, "3306", props[property_util.PORT.Name])
	assert.Equal(t, "myDatabase", props[property_util.DATABASE.Name])
}

func TestParseDsnMySqlWithNoPort(t *testing.T) {
	dsn := "someUser:somePassword@tcp(mydatabase.com)/myDatabase"
	props, err := utils.ParseDsn(dsn)

	if err != nil {
		t.Errorf(`Unexpected error when calling ParseDsn: %s, Error: %q`, dsn, err)
	}

	assert.Equal(t, "mysql", props[property_util.DRIVER_PROTOCOL.Name])
	assert.Equal(t, "someUser", props[property_util.USER.Name])
	assert.Equal(t, "somePassword", props[property_util.PASSWORD.Name])
	assert.Equal(t, "mydatabase.com", props[property_util.HOST.Name])
	assert.Equal(t, "myDatabase", props[property_util.DATABASE.Name])
}

func TestParseDsnMySqlWithNoDb(t *testing.T) {
	dsn := "someUser:somePassword@tcp(mydatabase.com:3306)/"
	props, err := utils.ParseDsn(dsn)

	if err != nil {
		t.Errorf(`Unexpected error when calling ParseDsn: %s, Error: %q`, dsn, err)
	}

	assert.Equal(t, "mysql", props[property_util.DRIVER_PROTOCOL.Name])
	assert.Equal(t, "someUser", props[property_util.USER.Name])
	assert.Equal(t, "somePassword", props[property_util.PASSWORD.Name])
	assert.Equal(t, "mydatabase.com", props[property_util.HOST.Name])
	assert.Equal(t, "3306", props[property_util.PORT.Name])
}

func TestParseMySqlWithTrailingSpace(t *testing.T) {
	dsn := "someUser:somePassword@tcp(mydatabase.com:3306)/myDatabase?foo=bar&pop=snap&numTest=4   "
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
	assert.Equal(t, "4", props["numTest"])
}

func TestParseMySqlEndpointWithTrailingDot(t *testing.T) {
	dsnWithTrailingDot := "someUser:somePassword@tcp(mydatabase.com.:3306)/myDatabase?foo=bar&pop=snap&customEndpoint=https://someendpoint.com:3456"
	props, err := utils.ParseDsn(dsnWithTrailingDot)

	if err != nil {
		t.Errorf(`Unexpected error when calling ParseDsn: %s, Error: %q`, dsnWithTrailingDot, err)
	}

	assert.Equal(t, "mysql", props[property_util.DRIVER_PROTOCOL.Name])
	assert.Equal(t, "someUser", props[property_util.USER.Name])
	assert.Equal(t, "somePassword", props[property_util.PASSWORD.Name])
	assert.Equal(t, "mydatabase.com.", props[property_util.HOST.Name])
	assert.Equal(t, "3306", props[property_util.PORT.Name])
	assert.Equal(t, "myDatabase", props[property_util.DATABASE.Name])
	assert.Equal(t, "bar", props["foo"])
	assert.Equal(t, "snap", props["pop"])
	assert.Equal(t, "https://someendpoint.com:3456", props["customEndpoint"])
}

func TestParseMySqlEndpointWithTrailingSlash(t *testing.T) {
	dsnWithTrailingSlash := "someUser:somePassword@tcp(mydatabase.com/:3306)/myDatabase?foo=bar&pop=snap&customEndpoint=https://someendpoint.com:3456"
	props, err := utils.ParseDsn(dsnWithTrailingSlash)

	if err != nil {
		t.Errorf(`Unexpected error when calling ParseDsn: %s, Error: %q`, dsnWithTrailingSlash, err)
	}

	assert.Equal(t, "mysql", props[property_util.DRIVER_PROTOCOL.Name])
	assert.Equal(t, "someUser", props[property_util.USER.Name])
	assert.Equal(t, "somePassword", props[property_util.PASSWORD.Name])
	assert.Equal(t, "mydatabase.com/", props[property_util.HOST.Name])
	assert.Equal(t, "3306", props[property_util.PORT.Name])
	assert.Equal(t, "myDatabase", props[property_util.DATABASE.Name])
	assert.Equal(t, "bar", props["foo"])
	assert.Equal(t, "snap", props["pop"])
	assert.Equal(t, "https://someendpoint.com:3456", props["customEndpoint"])
}

func TestParseMySqlEndpointWithTrailingSlashDot(t *testing.T) {
	dsn := "someUser:somePassword@tcp(mydatabase.com/.:3306)/myDatabase?foo=bar&pop=snap&customEndpoint=https://someendpoint.com:3456"
	props, err := utils.ParseDsn(dsn)

	if err != nil {
		t.Errorf(`Unexpected error when calling ParseDsn: %s, Error: %q`, dsn, err)
	}

	assert.Equal(t, "mysql", props[property_util.DRIVER_PROTOCOL.Name])
	assert.Equal(t, "someUser", props[property_util.USER.Name])
	assert.Equal(t, "somePassword", props[property_util.PASSWORD.Name])
	assert.Equal(t, "mydatabase.com/.", props[property_util.HOST.Name])
	assert.Equal(t, "3306", props[property_util.PORT.Name])
	assert.Equal(t, "myDatabase", props[property_util.DATABASE.Name])
	assert.Equal(t, "bar", props["foo"])
	assert.Equal(t, "snap", props["pop"])
	assert.Equal(t, "https://someendpoint.com:3456", props["customEndpoint"])
}

func TestParseMySqlDsnWithIamToken(t *testing.T) {
	iamToken := "mydatabase.com:3306/?Action=connect&DBUser=someUser%"
	dsn := fmt.Sprintf("someUser:%s@tcp(mydatabase.com:3306)/myDatabase?foo=bar&pop=snap&customEndpoint=https://someendpoint.com:3456",
		iamToken)
	props, err := utils.ParseDsn(dsn)

	if err != nil {
		t.Errorf(`Unexpected error when calling ParseDsn: %s, Error: %q`, dsn, err)
	}

	assert.Equal(t, "mysql", props[property_util.DRIVER_PROTOCOL.Name])
	assert.Equal(t, "someUser", props[property_util.USER.Name])
	assert.Equal(t, iamToken, props[property_util.PASSWORD.Name])
	assert.Equal(t, "mydatabase.com", props[property_util.HOST.Name])
	assert.Equal(t, "3306", props[property_util.PORT.Name])
	assert.Equal(t, "myDatabase", props[property_util.DATABASE.Name])
	assert.Equal(t, "bar", props["foo"])
	assert.Equal(t, "snap", props["pop"])
	assert.Equal(t, "https://someendpoint.com:3456", props["customEndpoint"])
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
	hosts, err := utils.GetHostsFromDsn(dsn, true)
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
	hosts, err := utils.GetHostsFromDsn(dsn, true)
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
	_, err := utils.GetHostsFromDsn(dsn, true)
	if err == nil {
		t.Errorf("GetHostsFromDsn should throw an error with an invalid value for the port parameter")
	} else {
		assert.True(t, strings.Contains(err.Error(), "port"))
	}
}

func TestParseHostPortPair_ValidWriterWithPort(t *testing.T) {
	hostInfo, err := utils.ParseHostPortPair("test.cluster-abc.us-west-2.rds.amazonaws.com:5432", 3306)
	assert.NoError(t, err)
	assert.Equal(t, "test.cluster-abc.us-west-2.rds.amazonaws.com", hostInfo.Host)
	assert.Equal(t, 5432, hostInfo.Port)
	assert.Equal(t, host_info_util.WRITER, hostInfo.Role)
}

func TestParseHostPortPair_ValidReaderWithPort(t *testing.T) {
	hostInfo, err := utils.ParseHostPortPair("test.cluster-ro-abc.us-west-2.rds.amazonaws.com:5433", 3306)
	assert.NoError(t, err)
	assert.Equal(t, "test.cluster-ro-abc.us-west-2.rds.amazonaws.com", hostInfo.Host)
	assert.Equal(t, 5433, hostInfo.Port)
	assert.Equal(t, host_info_util.READER, hostInfo.Role)
}

func TestParseHostPortPair_NoPortProvided(t *testing.T) {
	hostInfo, err := utils.ParseHostPortPair("test.cluster-ro-abc.us-west-2.rds.amazonaws.com", 3306)
	assert.NoError(t, err)
	assert.Equal(t, "test.cluster-ro-abc.us-west-2.rds.amazonaws.com", hostInfo.Host)
	assert.Equal(t, 3306, hostInfo.Port)
	assert.Equal(t, host_info_util.READER, hostInfo.Role)
}

func TestParseHostPortPair_InvalidPort(t *testing.T) {
	hostInfo, err := utils.ParseHostPortPair("invalid-host:abc", 3306)
	assert.Nil(t, hostInfo)
	assert.Error(t, err)
}

func TestParseDatabaseFromDsn_PgxUrl(t *testing.T) {
	dsn := "postgres://user:pass@localhost:5432/mydb"
	db, err := utils.ParseDatabaseFromDsn(dsn)
	assert.NoError(t, err)
	assert.Equal(t, "mydb", db)
}

func TestParseUserFromDsn_PgxUrl(t *testing.T) {
	dsn := "postgres://myuser:mypassword@localhost:5432/mydb"
	user, err := utils.ParseUserFromDsn(dsn)
	assert.NoError(t, err)
	assert.Equal(t, "myuser", user)
}

func TestParsePasswordFromDsn_PgxUrl(t *testing.T) {
	dsn := "postgres://myuser:mypassword@localhost:5432/mydb"
	pass, err := utils.ParsePasswordFromDsn(dsn)
	assert.NoError(t, err)
	assert.Equal(t, "mypassword", pass)
}

func TestParseDatabaseFromDsn_MySQL(t *testing.T) {
	dsn := "myuser:mypassword@tcp(localhost:3306)/mydb"
	db, err := utils.ParseDatabaseFromDsn(dsn)
	assert.NoError(t, err)
	assert.Equal(t, "mydb", db)
}

func TestParseUserFromDsn_MySQL(t *testing.T) {
	dsn := "myuser:mypassword@tcp(localhost:3306)/mydb"
	user, err := utils.ParseUserFromDsn(dsn)
	assert.NoError(t, err)
	assert.Equal(t, "myuser", user)
}

func TestParsePasswordFromDsn_MySQL(t *testing.T) {
	dsn := "myuser:mypassword@tcp(localhost:3306)/mydb"
	pass, err := utils.ParsePasswordFromDsn(dsn)
	assert.NoError(t, err)
	assert.Equal(t, "mypassword", pass)
}

func TestGetProtocol_PgxUrl(t *testing.T) {
	dsn := "postgres://user:pass@localhost:5432/db"
	protocol, err := utils.GetProtocol(dsn)
	assert.NoError(t, err)
	assert.Equal(t, utils.PGX_DRIVER_PROTOCOL, protocol)
}

func TestGetProtocol_PgxKeyValue(t *testing.T) {
	dsn := "user=postgres password=secret host=localhost dbname=mydb"
	protocol, err := utils.GetProtocol(dsn)
	assert.NoError(t, err)
	assert.Equal(t, utils.PGX_DRIVER_PROTOCOL, protocol)
}

func TestGetProtocol_MySQL(t *testing.T) {
	dsn := "myuser:mypassword@tcp(localhost:3306)/mydb"
	protocol, err := utils.GetProtocol(dsn)
	assert.NoError(t, err)
	assert.Equal(t, utils.MYSQL_DRIVER_PROTOCOL, protocol)
}

func TestGetProtocol_Invalid(t *testing.T) {
	dsn := "user=postgres password=secret host=localhost dbname=mydb"
	protocol, err := utils.GetProtocol(dsn)
	assert.NoError(t, err)
	assert.Equal(t, utils.PGX_DRIVER_PROTOCOL, protocol)

	dsn = "myuser:mypassword@tcp(localhost:3306)/mydb"
	protocol, err = utils.GetProtocol(dsn)
	assert.NoError(t, err)
	assert.Equal(t, utils.MYSQL_DRIVER_PROTOCOL, protocol)

	dsn = "postgres://user:pass@localhost:5432/db"
	protocol, err = utils.GetProtocol(dsn)
	assert.NoError(t, err)
	assert.Equal(t, utils.PGX_DRIVER_PROTOCOL, protocol)
}
