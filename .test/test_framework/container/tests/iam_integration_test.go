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
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-advanced-go-wrapper/.test/test_framework/container/test_utils"
	awsDriver "github.com/aws/aws-advanced-go-wrapper/awssql/driver"
	"github.com/aws/aws-advanced-go-wrapper/awssql/error_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/property_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils"
	_ "github.com/aws/aws-advanced-go-wrapper/iam"
	_ "github.com/aws/aws-advanced-go-wrapper/otlp"
	_ "github.com/aws/aws-advanced-go-wrapper/xray"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Attempt to connect using the wrong database username.
func TestIamWrongDatabaseUsername(t *testing.T) {
	awsDriver.ClearCaches()
	testEnvironment, err := test_utils.GetCurrentTestEnvironment()

	assert.NoError(t, err)
	props := initIamProps(
		"WRONG_Username"+testEnvironment.Info().DatabaseInfo.Username,
		testEnvironment.Info().DatabaseInfo.Password,
		testEnvironment,
	)

	dsn := test_utils.GetDsn(testEnvironment, props)

	db, err := test_utils.OpenDb(testEnvironment.Info().Request.Engine, dsn)

	assert.NoError(t, err)
	assert.NotNil(t, db)
	defer db.Close()

	pingErr := db.Ping()
	assert.Error(t, pingErr)
}

// Attempt to connect without specifying a database username.
func TestIamNoDatabaseUsername(t *testing.T) {
	awsDriver.ClearCaches()
	testEnvironment, err := test_utils.GetCurrentTestEnvironment()
	assert.NoError(t, err)
	props := initIamProps(
		"",
		testEnvironment.Info().DatabaseInfo.Password,
		testEnvironment,
	)

	dsn := test_utils.GetDsn(testEnvironment, props)

	db, err := test_utils.OpenDb(testEnvironment.Info().Request.Engine, dsn)

	assert.NoError(t, err)
	assert.NotNil(t, db)
	defer db.Close()

	pingErr := db.Ping()
	assert.Error(t, pingErr)
}

// Attempt to connect using IP address instead of a hostname.
func TestIamUsingIpAddress(t *testing.T) {
	awsDriver.ClearCaches()
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

	props[property_util.IAM_HOST.Name] = clusterEndpoint
	props[property_util.IAM_DEFAULT_PORT.Name] = strconv.Itoa(port)
	props[property_util.HOST.Name] = hostIp

	dsn := test_utils.GetDsn(testEnvironment, props)
	db, err := test_utils.OpenDb(testEnvironment.Info().Request.Engine, dsn)

	assert.NoError(t, err)
	assert.NotNil(t, db)
	defer db.Close()

	pingErr := db.Ping()
	assert.NoError(t, pingErr)
}

// Attempt to connect using valid database username/password & valid Amazon RDS hostname.
func TestIamValidConnectionProperties(t *testing.T) {
	awsDriver.ClearCaches()
	testEnvironment, err := test_utils.GetCurrentTestEnvironment()
	assert.NoError(t, err)
	props := initIamProps(
		testEnvironment.Info().IamUsername,
		"anypassword",
		testEnvironment,
	)
	dsn := test_utils.GetDsn(testEnvironment, props)
	db, err := test_utils.OpenDb(testEnvironment.Info().Request.Engine, dsn)

	assert.NoError(t, err)
	assert.NotNil(t, db)
	defer db.Close()
	pingErr := db.Ping()
	assert.NoError(t, pingErr)
}

// Attempt to connect using valid database username, valid Amazon RDS hostname, but no password.
func TestIamValidConnectionPropertiesNoPassword(t *testing.T) {
	awsDriver.ClearCaches()
	testEnvironment, err := test_utils.GetCurrentTestEnvironment()
	assert.NoError(t, err)
	props := initIamProps(
		testEnvironment.Info().IamUsername,
		"",
		testEnvironment,
	)
	dsn := test_utils.GetDsn(testEnvironment, props)
	db, err := test_utils.OpenDb(testEnvironment.Info().Request.Engine, dsn)

	assert.NoError(t, err)
	assert.NotNil(t, db)
	defer db.Close()
	pingErr := db.Ping()
	assert.NoError(t, pingErr)
}

// Attempt to connect with a conn object.
func TestIamValidConnectionConnObject(t *testing.T) {
	awsDriver.ClearCaches()
	testEnvironment, err := test_utils.GetCurrentTestEnvironment()
	assert.NoError(t, err)
	props := initIamProps(
		testEnvironment.Info().IamUsername,
		"anypassword",
		testEnvironment,
	)
	dsn := test_utils.GetDsn(testEnvironment, props)

	wrapperDriver := test_utils.NewWrapperDriver(testEnvironment.Info().Request.Engine)

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

func TestIamValidConnectionConnObjectWithTelemetryOtel(t *testing.T) {
	awsDriver.ClearCaches()
	testEnvironment, err := test_utils.GetCurrentTestEnvironment()
	assert.NoError(t, err)
	bsp, err := test_utils.SetupTelemetry(testEnvironment)
	assert.NoError(t, err)
	assert.NotNil(t, bsp)
	defer func() { _ = bsp.Shutdown(context.TODO()) }()

	props := initIamProps(
		testEnvironment.Info().IamUsername,
		"anypassword",
		testEnvironment,
	)
	props["enableTelemetry"] = "true"
	props["telemetryTracesBackend"] = "OTLP"
	props["telemetryMetricsBackend"] = "OTLP"
	dsn := test_utils.GetDsn(testEnvironment, props)

	wrapperDriver := test_utils.NewWrapperDriver(testEnvironment.Info().Request.Engine)

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

func TestIamValidConnectionConnObjectWithTelemetryXray(t *testing.T) {
	awsDriver.ClearCaches()
	testEnvironment, err := test_utils.GetCurrentTestEnvironment()
	assert.NoError(t, err)
	bsp, err := test_utils.SetupTelemetry(testEnvironment)
	assert.NoError(t, err)
	assert.NotNil(t, bsp)
	defer func() { _ = bsp.Shutdown(context.TODO()) }()
	props := initIamProps(
		testEnvironment.Info().IamUsername,
		"anypassword",
		testEnvironment,
	)
	props["enableTelemetry"] = "true"
	props["telemetryTracesBackend"] = "XRAY"
	props["telemetryMetricsBackend"] = "OTLP"
	dsn := test_utils.GetDsn(testEnvironment, props)

	wrapperDriver := test_utils.NewWrapperDriver(testEnvironment.Info().Request.Engine)

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

func TestIamWithFailover(t *testing.T) {
	awsDriver.ClearCaches()
	auroraTestUtility, environment, err := failoverSetup(t)
	defer test_utils.BasicCleanup(t.Name())
	assert.Nil(t, err)
	test_utils.SkipForMultiAzMySql(t, environment.Info().Request.Deployment, environment.Info().Request.Engine)

	props := initIamProps(
		environment.Info().IamUsername,
		"anypassword",
		environment,
	)
	props[property_util.PLUGINS.Name] = "failover,iam"
	props[property_util.IAM_HOST.Name] = environment.Info().DatabaseInfo.ClusterEndpoint
	props[property_util.IAM_DEFAULT_PORT.Name] = strconv.Itoa(environment.Info().DatabaseInfo.InstanceEndpointPort)
	dsn := test_utils.GetDsnForTestsWithProxy(environment, props)
	wrapperDriver := test_utils.NewWrapperDriver(environment.Info().Request.Engine)

	conn, err := wrapperDriver.Open(dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	// Check that we are connected to the writer.
	instanceId, err := test_utils.ExecuteInstanceQuery(environment.Info().Request.Engine, environment.Info().Request.Deployment, conn)
	assert.Nil(t, err)
	assert.True(t, auroraTestUtility.IsDbInstanceWriter(instanceId, ""))

	// Failover and check that it has failed over.
	triggerFailoverError := auroraTestUtility.TriggerFailover(instanceId, "", "")
	assert.Nil(t, triggerFailoverError)
	_, queryError := test_utils.ExecuteInstanceQuery(environment.Info().Request.Engine, environment.Info().Request.Deployment, conn)
	require.Error(t, queryError, "Failover plugin did not complete failover successfully.")
	assert.Equal(t, error_util.GetMessage("Failover.connectionChangedError"), queryError.Error())

	// Assert that we are connected to the new writer after failover.
	newInstanceId, err := test_utils.ExecuteInstanceQuery(environment.Info().Request.Engine, environment.Info().Request.Deployment, conn)
	assert.Nil(t, err)
	currWriterId, err := auroraTestUtility.GetClusterWriterInstanceId("")
	assert.Nil(t, err)
	assert.Equal(t, currWriterId, newInstanceId)

	// Skip for multi-AZ b/c it simulates failover which reconnects to the original instance.
	if environment.Info().Request.Deployment == test_utils.RDS_MULTI_AZ_CLUSTER {
		assert.Equal(t, instanceId, newInstanceId)
	} else {
		assert.NotEqual(t, instanceId, newInstanceId)
	}
}

func TestIamWithEfm(t *testing.T) {
	awsDriver.ClearCaches()
	_, environment, err := failoverSetup(t)
	defer test_utils.BasicCleanup(t.Name())
	assert.Nil(t, err)

	proxyTestProps := test_utils.GetPropsForProxy(environment, environment.Info().ProxyDatabaseInfo.ClusterEndpoint, "efm,iam", TEST_FAILURE_DETECTION_INTERVAL_SECONDS)
	iamProps := initIamProps(
		environment.Info().IamUsername,
		"anypassword",
		environment,
	)
	props := utils.CombineMaps(iamProps, proxyTestProps)
	props[property_util.IAM_HOST.Name] = environment.Info().DatabaseInfo.ClusterEndpoint
	props[property_util.IAM_DEFAULT_PORT.Name] = strconv.Itoa(environment.Info().DatabaseInfo.InstanceEndpointPort)

	dsn := test_utils.GetDsn(environment, props)
	db, err := test_utils.OpenDb(environment.Info().Request.Engine, dsn)
	assert.Nil(t, err)
	assert.NotNil(t, db)
	defer db.Close()

	// Verify connection works.
	err = db.Ping()
	require.NoError(t, err, "Failed to open database connection.")

	// Start a long-running query in a goroutine
	queryChan := make(chan error)
	go func() {
		// Execute a sleep query that will run for 10 seconds
		sleepQuery := test_utils.GetSleepSql(environment.Info().Request.Engine, TEST_SLEEP_QUERY_SECONDS)

		_, err := test_utils.ExecuteQueryDB(environment.Info().Request.Engine, db, sleepQuery, TEST_SLEEP_QUERY_TIMEOUT_SECONDS)
		queryChan <- err
	}()

	// Wait a bit to ensure the query has started
	time.Sleep(5 * time.Second)

	// Disable connectivity while the sleep query is running
	slog.Debug("Disabling all connectivity.")
	test_utils.DisableAllConnectivity()

	// Wait for the query to complete and check the error
	queryErr := <-queryChan
	close(queryChan)
	require.NotNil(t, queryErr)
	slog.Debug(fmt.Sprintf("Sleep query fails with error: %s.", queryErr.Error()))
	assert.False(t, errors.Is(queryErr, context.DeadlineExceeded), "Sleep query should have failed due to connectivity loss")

	// Re-enable connectivity
	slog.Debug("Re-enabling all connectivity.")
	test_utils.EnableAllConnectivity(true)

	newInstanceId, err := test_utils.ExecuteInstanceQueryDB(environment.Info().Request.Engine, environment.Info().Request.Deployment, db)
	assert.Nil(t, err, "After connectivity is re-enabled new connections should not throw errors.")
	assert.NotZero(t, newInstanceId)
}

func TestIamWithFailoverEfm(t *testing.T) {
	awsDriver.ClearCaches()
	auroraTestUtility, environment, err := failoverSetup(t)
	defer test_utils.BasicCleanup(t.Name())
	assert.Nil(t, err)
	test_utils.SkipForMultiAzMySql(t, environment.Info().Request.Deployment, environment.Info().Request.Engine)

	proxyTestProps := test_utils.GetPropsForProxy(environment, environment.Info().ProxyDatabaseInfo.ClusterEndpoint, "failover,efm,iam", TEST_FAILURE_DETECTION_INTERVAL_SECONDS)
	iamProps := initIamProps(
		environment.Info().IamUsername,
		"anypassword",
		environment,
	)
	props := utils.CombineMaps(iamProps, proxyTestProps)
	props[property_util.IAM_HOST.Name] = environment.Info().DatabaseInfo.ClusterEndpoint
	props[property_util.IAM_DEFAULT_PORT.Name] = strconv.Itoa(environment.Info().DatabaseInfo.InstanceEndpointPort)
	props[property_util.FAILOVER_TIMEOUT_MS.Name] = strconv.Itoa(4 * TEST_MONITORING_TIMEOUT_SECONDS * 1000)

	dsn := test_utils.GetDsn(environment, props)
	db, err := test_utils.OpenDb(environment.Info().Request.Engine, dsn)
	assert.Nil(t, err)
	assert.NotNil(t, db)
	defer db.Close()

	// Verify connection works.
	err = db.Ping()
	require.NoError(t, err, "Failed to open database connection.")

	// Start a long-running query in a goroutine
	queryChan := make(chan error)
	go func() {
		// Execute a sleep query that will run for 10 seconds
		sleepQuery := test_utils.GetSleepSql(environment.Info().Request.Engine, TEST_SLEEP_QUERY_SECONDS)

		_, err := test_utils.ExecuteQueryDB(environment.Info().Request.Engine, db, sleepQuery, TEST_SLEEP_QUERY_TIMEOUT_SECONDS)
		queryChan <- err
	}()

	// Wait a bit to ensure the query has started
	time.Sleep(5 * time.Second)

	// Disable connectivity while the sleep query is running
	slog.Debug("Disabling all connectivity.")
	test_utils.DisableAllConnectivity()

	// Wait a bit to ensure failover has started
	time.Sleep(2 * TEST_MONITORING_TIMEOUT_SECONDS * time.Second)

	// Re-enable connectivity
	slog.Debug("Re-enabling all connectivity.")
	test_utils.EnableAllConnectivity(true)

	// Wait for the query to complete and check the error
	queryErr := <-queryChan
	close(queryChan)
	require.NotNil(t, queryErr)
	assert.Equal(t, error_util.GetMessage("Failover.connectionChangedError"), queryErr.Error())

	newInstanceId, err := test_utils.ExecuteInstanceQueryDB(environment.Info().Request.Engine, environment.Info().Request.Deployment, db)
	require.True(t, auroraTestUtility.IsDbInstanceWriter(newInstanceId, ""))
	assert.Nil(t, err, "After connectivity is re-enabled new connections should not throw errors.")
	assert.NotZero(t, newInstanceId)
}

func initIamProps(user string, password string, testEnvironment *test_utils.TestEnvironment) map[string]string {
	props := map[string]string{
		property_util.PLUGINS.Name:    "iam",
		property_util.USER.Name:       user,
		property_util.PASSWORD.Name:   password,
		property_util.IAM_REGION.Name: testEnvironment.Info().Region,
	}

	return addMySQLIamHandlingIfNecessary(testEnvironment, props)
}

func addMySQLIamHandlingIfNecessary(testEnvironment *test_utils.TestEnvironment, props map[string]string) map[string]string {
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
