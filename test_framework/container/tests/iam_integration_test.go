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
	_ "awssql/driver"
	"awssql/property_util"
	"awssql/test_framework/container/test_utils"
	"context"
	"database/sql"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// Attempt to connect using the wrong database username.
func TestIamWrongDatabaseUsername(t *testing.T) {
	testEnvironment, err := test_utils.GetCurrentTestEnvironment()

	assert.NoError(t, err)
	props := initIamProps(
		"WRONG_Username"+testEnvironment.User(),
		testEnvironment.Password(),
		testEnvironment,
	)

	dsn := test_utils.GetDsn(testEnvironment, props)

	db, err := sql.Open("awssql", dsn)

	assert.NoError(t, err)
	assert.NotNil(t, db)
	defer db.Close()

	pingErr := db.Ping()
	assert.Error(t, pingErr)
}

// Attempt to connect without specifying a database username.
func TestIamNoDatabaseUsername(t *testing.T) {
	testEnvironment, err := test_utils.GetCurrentTestEnvironment()
	assert.NoError(t, err)
	props := initIamProps(
		"",
		testEnvironment.Password(),
		testEnvironment,
	)

	dsn := test_utils.GetDsn(testEnvironment, props)

	db, err := sql.Open("awssql", dsn)

	assert.NoError(t, err)
	assert.NotNil(t, db)
	defer db.Close()

	pingErr := db.Ping()
	assert.Error(t, pingErr)
}

// Attempt to connect using IP address instead of a hostname.
func TestIamUsingIpAddress(t *testing.T) {
	testEnvironment, err := test_utils.GetCurrentTestEnvironment()
	assert.NoError(t, err)
	props := initIamProps(
		testEnvironment.IamUsername(),
		"Anypassword",
		testEnvironment,
	)

	hostIp, err := hostToIp(testEnvironment.ClusterEndpoint())
	assert.NoError(t, err)

	property_util.IAM_HOST.Set(props, testEnvironment.ClusterEndpoint())
	property_util.IAM_DEFAULT_PORT.Set(props, strconv.Itoa(testEnvironment.InstanceEndpointPort()))
	property_util.HOST.Set(props, hostIp)

	dsn := test_utils.GetDsn(testEnvironment, props)
	db, err := sql.Open("awssql", dsn)

	assert.NoError(t, err)
	assert.NotNil(t, db)
	defer db.Close()

	pingErr := db.Ping()
	assert.NoError(t, pingErr)
}

// Attempt to connect using valid database username/password & valid Amazon RDS hostname.
func TestIamValidConnectionProperties(t *testing.T) {
	testEnvironment, err := test_utils.GetCurrentTestEnvironment()
	assert.NoError(t, err)
	props := initIamProps(
		testEnvironment.IamUsername(),
		"anypassword",
		testEnvironment,
	)
	dsn := test_utils.GetDsn(testEnvironment, props)
	db, err := sql.Open("awssql", dsn)

	assert.NoError(t, err)
	assert.NotNil(t, db)
	defer db.Close()
	pingErr := db.Ping()
	assert.NoError(t, pingErr)
}

// Attempt to connect using valid database username, valid Amazon RDS hostname, but no password.
func TestIamValidConnectionPropertiesNoPassword(t *testing.T) {
	testEnvironment, err := test_utils.GetCurrentTestEnvironment()
	assert.NoError(t, err)
	props := initIamProps(
		testEnvironment.IamUsername(),
		"",
		testEnvironment,
	)
	dsn := test_utils.GetDsn(testEnvironment, props)
	db, err := sql.Open("awssql", dsn)

	assert.NoError(t, err)
	assert.NotNil(t, db)
	defer db.Close()
	pingErr := db.Ping()
	assert.NoError(t, pingErr)
}

// Attempts a valid connection followed by invalid connection without the AWS protocol in Connection URL.
func TestIamNoAwsProtocolConnection(t *testing.T) {
	// Successful connection
	testEnvironment, err := test_utils.GetCurrentTestEnvironment()
	assert.NoError(t, err)
	props := initIamProps(
		testEnvironment.IamUsername(),
		"anypassword",
		testEnvironment,
	)
	dsn := test_utils.GetDsn(testEnvironment, props)
	db, err := sql.Open("awssql", dsn)

	assert.NoError(t, err)
	assert.NotNil(t, db)
	defer db.Close()
	pingErr := db.Ping()
	assert.NoError(t, pingErr)

	// Invalid connection
	invalidProps := initIamProps(
		"WRONG_USER"+testEnvironment.IamUsername(),
		"anypassword",
		testEnvironment,
	)
	invalidDsn := test_utils.GetDsn(testEnvironment, invalidProps)
	invalidDb, err := sql.Open("awssql", invalidDsn)

	assert.NoError(t, err)
	assert.NotNil(t, invalidDb)
	defer invalidDb.Close()
	pingErr = invalidDb.Ping()
	assert.Error(t, pingErr)
}

// Attempts a valid connection with a datasource and makes sure the user and password properties persist.
func TestIamUserAndPasswordPropertiesArePreserved(t *testing.T) {
	// Successful connection
	testEnvironment, err := test_utils.GetCurrentTestEnvironment()
	assert.NoError(t, err)
	props := initIamProps(
		testEnvironment.IamUsername(),
		"anypassword",
		testEnvironment,
	)
	dsn := test_utils.GetDsn(testEnvironment, props)
	db, err := sql.Open("awssql", dsn)

	assert.NoError(t, err)
	assert.NotNil(t, db)
	defer db.Close()

	// Ping for 10 seconds
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	pingErr := db.PingContext(ctx)
	assert.NoError(t, pingErr)

	// Ping again for another 5 seconds
	pingErr = db.PingContext(ctx)
	assert.NoError(t, pingErr)
}

func initIamProps(user string, password string, testEnvironment *test_utils.TestEnvironment) map[string]string {
	props := map[string]string{
		property_util.PLUGINS.Name:    "iam",
		property_util.USER.Name:       user,
		property_util.PASSWORD.Name:   password,
		property_util.IAM_REGION.Name: testEnvironment.Region(),
	}

	// Needed for MYSQL
	if testEnvironment.Engine() == test_utils.MYSQL {
		props["tls"] = "skip-verify"
		props["allowCleartextPasswords"] = "true"
	}
	return props
}

func hostToIp(hostName string) (string, error) {
	ips, err := net.LookupIP(hostName)

	if len(ips) < 1 {
		return "", err
	}

	// return the first IP we get.
	return ips[0].String(), err
}
