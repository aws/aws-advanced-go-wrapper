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
	awsDriver "awssql/driver"
	"awssql/plugins/iam"
	"awssql/property_util"
	"awssql/test_framework/container/test_utils"
	"context"
	"database/sql"
	"database/sql/driver"
	"net"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

// Attempt to connect using the wrong database username.
func TestIamWrongDatabaseUsername(t *testing.T) {
	iam.ClearCaches()
	testEnvironment, err := test_utils.GetCurrentTestEnvironment()

	assert.NoError(t, err)
	props := initIamProps(
		"WRONG_Username"+testEnvironment.Info().DatabaseInfo.Username,
		testEnvironment.Info().DatabaseInfo.Password,
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
	iam.ClearCaches()
	testEnvironment, err := test_utils.GetCurrentTestEnvironment()
	assert.NoError(t, err)
	props := initIamProps(
		"",
		testEnvironment.Info().DatabaseInfo.Password,
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
	iam.ClearCaches()
	testEnvironment, err := test_utils.GetCurrentTestEnvironment()
	assert.NoError(t, err)
	props := initIamProps(
		testEnvironment.Info().IamUsername,
		"Anypassword",
		testEnvironment,
	)

	clusterEndpoint := testEnvironment.Info().DatabaseInfo.ClusterEndpoint
	port := testEnvironment.Info().DatabaseInfo.InstanceEndpointPort

	hostIp, err := hostToIp(clusterEndpoint)
	assert.NoError(t, err)

	property_util.IAM_HOST.Set(props, clusterEndpoint)
	property_util.IAM_DEFAULT_PORT.Set(props, strconv.Itoa(port))
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
	iam.ClearCaches()
	testEnvironment, err := test_utils.GetCurrentTestEnvironment()
	assert.NoError(t, err)
	props := initIamProps(
		testEnvironment.Info().IamUsername,
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
	iam.ClearCaches()
	testEnvironment, err := test_utils.GetCurrentTestEnvironment()
	assert.NoError(t, err)
	props := initIamProps(
		testEnvironment.Info().IamUsername,
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

// Attempt to connect with a conn object.
func TestIamValidConnectionConObject(t *testing.T) {
	iam.ClearCaches()
	testEnvironment, err := test_utils.GetCurrentTestEnvironment()
	assert.NoError(t, err)
	props := initIamProps(
		testEnvironment.Info().IamUsername,
		"anypassword",
		testEnvironment,
	)
	dsn := test_utils.GetDsn(testEnvironment, props)

	wrapperDriver := &awsDriver.AwsWrapperDriver{}

	conn, err := wrapperDriver.Open(dsn)
	assert.NoError(t, err)
	defer conn.Close()

	queryer, ok := conn.(driver.QueryerContext)
	assert.True(t, ok)

	// Execute the query
	rows, err := queryer.QueryContext(context.Background(), "SELECT 1", nil)
	assert.NoError(t, err)
	defer rows.Close()
}

func initIamProps(user string, password string, testEnvironment *test_utils.TestEnvironment) map[string]string {
	props := map[string]string{
		property_util.PLUGINS.Name:    "iam",
		property_util.USER.Name:       user,
		property_util.PASSWORD.Name:   password,
		property_util.IAM_REGION.Name: testEnvironment.Info().Region,
	}

	// Needed for MYSQL
	if testEnvironment.Info().Request.Engine == test_utils.MYSQL {
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
