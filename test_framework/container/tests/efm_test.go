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
	"awssql/property_util"
	"awssql/test_framework/container/test_utils"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const TEST_FAILURE_DETECTION_INTERVAL_SECONDS = 10
const TEST_FAILURE_DETECTION_COUNT = 3
const TEST_SLEEP_QUERY_SECONDS = (TEST_FAILURE_DETECTION_COUNT + 1) * TEST_FAILURE_DETECTION_INTERVAL_SECONDS
const TEST_SLEEP_QUERY_TIMEOUT_SECONDS = 2 * TEST_SLEEP_QUERY_SECONDS

func TestEfmDisableInstance(t *testing.T) {
	err := test_utils.BasicSetup(t.Name())
	defer test_utils.BasicCleanup(t.Name())
	assert.Nil(t, err)

	environment, err := test_utils.GetCurrentTestEnvironment()
	assert.Nil(t, err)
	dsn := getDsnForEfmIntegrationTest(environment, environment.Info().ProxyDatabaseInfo.WriterInstanceEndpoint())

	wrapperDriver := &awsDriver.AwsWrapperDriver{}
	conn, err := wrapperDriver.Open(dsn)
	require.Nil(t, err)
	defer conn.Close()

	// Get initial instance ID
	instanceId, err := test_utils.ExecuteInstanceQuery(environment.Info().Request.Engine, environment.Info().Request.Deployment, conn)
	assert.Nil(t, err)
	assert.NotZero(t, instanceId)

	// Start a long-running query in a goroutine
	queryChan := make(chan error)
	go func() {
		// Execute a sleep query that will run for 10 seconds
		sleepQuery := test_utils.GetSleepSql(environment.Info().Request.Engine, TEST_SLEEP_QUERY_SECONDS)
		_, err := test_utils.ExecuteQuery(environment.Info().Request.Engine, conn, sleepQuery, TEST_SLEEP_QUERY_TIMEOUT_SECONDS)
		queryChan <- err
	}()

	// Wait a bit to ensure the query has started
	time.Sleep(5 * time.Second)

	// Disable connectivity while the sleep query is running
	proxyInfo := environment.GetProxy(instanceId)
	slog.Debug(fmt.Sprintf("Disabling connectivity of instance %s.", instanceId))
	test_utils.DisableProxyConnectivity(proxyInfo)

	// Wait for the query to complete and check the error
	queryErr := <-queryChan
	close(queryChan)
	require.NotNil(t, queryErr)
	slog.Debug(fmt.Sprintf("Sleep query fails with error: %s.", queryErr.Error()))
	assert.False(t, errors.Is(queryErr, context.DeadlineExceeded), "Sleep query should have failed due to connectivity loss")

	// Re-enable connectivity
	slog.Debug(fmt.Sprintf("Re-enabling connectivity of instance %s.", instanceId))
	test_utils.EnableProxyConnectivity(proxyInfo, true)

	_, err = test_utils.ExecuteInstanceQuery(environment.Info().Request.Engine, environment.Info().Request.Deployment, conn)
	assert.NotNil(t, err, "Query should fail as instance is marked as unavailable.")

	conn, err = wrapperDriver.Open(dsn)
	require.Nil(t, err)

	newInstanceId, err := test_utils.ExecuteInstanceQuery(environment.Info().Request.Engine, environment.Info().Request.Deployment, conn)
	assert.Nil(t, err, "After connectivity is re-enabled new connections should not throw errors.")
	assert.Equal(t, instanceId, newInstanceId)
}

func TestEfmDisableAllInstances(t *testing.T) {
	err := test_utils.BasicSetup(t.Name())
	defer test_utils.BasicCleanup(t.Name())
	assert.Nil(t, err)

	environment, err := test_utils.GetCurrentTestEnvironment()
	assert.Nil(t, err)

	dsn := getDsnForEfmIntegrationTest(environment, environment.Info().ProxyDatabaseInfo.ClusterEndpoint)

	wrapperDriver := &awsDriver.AwsWrapperDriver{}
	conn, err := wrapperDriver.Open(dsn)
	require.Nil(t, err)
	defer conn.Close()

	// Get initial instance ID
	instanceId, err := test_utils.ExecuteInstanceQuery(environment.Info().Request.Engine, environment.Info().Request.Deployment, conn)
	assert.Nil(t, err)
	assert.NotZero(t, instanceId)

	// Start a long-running query in a goroutine
	queryChan := make(chan error)
	go func() {
		// Execute a sleep query that will run for 10 seconds
		sleepQuery := test_utils.GetSleepSql(environment.Info().Request.Engine, TEST_SLEEP_QUERY_SECONDS)

		_, err := test_utils.ExecuteQuery(environment.Info().Request.Engine, conn, sleepQuery, TEST_SLEEP_QUERY_TIMEOUT_SECONDS)
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

	_, err = test_utils.ExecuteInstanceQuery(environment.Info().Request.Engine, environment.Info().Request.Deployment, conn)
	assert.NotNil(t, err, "Query should fail as instance is marked as unavailable.")

	conn, err = wrapperDriver.Open(dsn)
	require.Nil(t, err)

	newInstanceId, err := test_utils.ExecuteInstanceQuery(environment.Info().Request.Engine, environment.Info().Request.Deployment, conn)
	assert.Nil(t, err, "After connectivity is re-enabled new connections should not throw errors.")
	assert.Equal(t, instanceId, newInstanceId)
}

func TestEfmDisableAllInstancesDB(t *testing.T) {
	err := test_utils.BasicSetup(t.Name())
	defer test_utils.BasicCleanup(t.Name())
	assert.Nil(t, err)

	environment, err := test_utils.GetCurrentTestEnvironment()
	assert.Nil(t, err)

	dsn := getDsnForEfmIntegrationTest(environment, environment.Info().ProxyDatabaseInfo.ClusterEndpoint)

	db, err := sql.Open("awssql", dsn)
	require.Nil(t, err)
	defer db.Close()

	// Get initial instance ID
	instanceId, err := test_utils.ExecuteInstanceQueryDB(environment.Info().Request.Engine, environment.Info().Request.Deployment, db)
	assert.Nil(t, err)
	assert.NotZero(t, instanceId)

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

	// Re-enable connectivity
	slog.Debug("Re-enabling all connectivity.")
	test_utils.EnableAllConnectivity(true)

	newInstanceId, err := test_utils.ExecuteInstanceQueryDB(environment.Info().Request.Engine, environment.Info().Request.Deployment, db)
	assert.Nil(t, err, "After connectivity is re-enabled new connections should not throw errors.")
	assert.NotZero(t, newInstanceId)
}

func getDsnForEfmIntegrationTest(environment *test_utils.TestEnvironment, host string) string {
	return getDsnForTestsWithProxy(environment, host, "efm")
}

func getDsnForTestsWithProxy(environment *test_utils.TestEnvironment, host string, plugins string) string {
	monitoringConnectTimeoutSeconds := strconv.Itoa(TEST_FAILURE_DETECTION_INTERVAL_SECONDS - 1)
	monitoringConnectTimeoutParameterName := property_util.MONITORING_PROPERTY_PREFIX
	switch environment.Info().Request.Engine {
	case test_utils.PG:
		monitoringConnectTimeoutParameterName = monitoringConnectTimeoutParameterName + "connect_timeout"
	case test_utils.MYSQL:
		monitoringConnectTimeoutParameterName = monitoringConnectTimeoutParameterName + "readTimeout"
		monitoringConnectTimeoutSeconds = monitoringConnectTimeoutSeconds + "s"
	}
	return test_utils.GetDsn(environment, map[string]string{
		"host":                       host,
		"port":                       strconv.Itoa(environment.Info().ProxyDatabaseInfo.InstanceEndpointPort),
		"clusterInstanceHostPattern": "?." + environment.Info().ProxyDatabaseInfo.InstanceEndpointSuffix,
		"plugins":                    plugins,
		"failureDetectionEnabled":    "true",
		"failureDetectionIntervalMs": strconv.Itoa(TEST_FAILURE_DETECTION_INTERVAL_SECONDS * 1000), // interval between probes to host
		"failureDetectionCount":      strconv.Itoa(TEST_FAILURE_DETECTION_COUNT),                   // 3 consecutive failures before marks host as dead
		"failureDetectionTimeMs":     "1000",                                                       // 1 second time before polling starts
		"failoverTimeoutMs":          strconv.Itoa(TEST_FAILURE_DETECTION_COUNT * TEST_FAILURE_DETECTION_INTERVAL_SECONDS * 1000),
		// each monitoring connection has monitoringConnectTimeoutSeconds seconds to connect
		monitoringConnectTimeoutParameterName: monitoringConnectTimeoutSeconds,
	})
}
