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
	"log/slog"

	"github.com/aws/aws-advanced-go-wrapper/.test/test_framework/container/test_utils"

	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-advanced-go-wrapper/awssql/error_util"
	_ "github.com/aws/aws-advanced-go-wrapper/otlp"
	_ "github.com/aws/aws-advanced-go-wrapper/xray"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFailoverWriterDB(t *testing.T) {
	auroraTestUtility, environment, err := failoverSetup(t)
	defer test_utils.BasicCleanup(t.Name())
	assert.Nil(t, err)
	dsn := test_utils.GetDsnForTestsWithProxy(environment, map[string]string{
		"host":    environment.Info().ProxyDatabaseInfo.ClusterEndpoint,
		"plugins": "failover",
	})
	db, err := test_utils.OpenDb(environment.Info().Request.Engine, dsn)
	assert.Nil(t, err)
	assert.NotNil(t, db)
	defer db.Close()

	// Verify connection works.
	err = db.Ping()
	require.NoError(t, err, "Failed to open database connection.")

	// Failover and check that it has failed over.
	failoverComplete := make(chan struct{})
	var triggerFailoverError error
	go func() {
		time.Sleep(5 * time.Second) // Give the query time to reach the database.
		triggerFailoverError = auroraTestUtility.CrashInstance("", "", "")
		close(failoverComplete)
	}()
	_, queryError := test_utils.ExecuteQueryDB(environment.Info().Request.Engine, db, test_utils.GetSleepSql(environment.Info().Request.Engine, 60), 61)
	<-failoverComplete
	require.NoError(t, triggerFailoverError, "Request to DB to failover did not succeed.")
	require.Error(t, queryError, "Failover plugin did not complete failover successfully.")
	assert.Equal(t, error_util.GetMessage("Failover.connectionChangedError"), queryError.Error())

	// Assert that we are connected to the new writer after failover.
	instanceId, err := test_utils.ExecuteInstanceQueryDB(environment.Info().Request.Engine, environment.Info().Request.Deployment, db)
	assert.Nil(t, err)
	currWriterId, err := auroraTestUtility.GetClusterWriterInstanceId("")
	assert.Nil(t, err)
	assert.Equal(t, currWriterId, instanceId)
}

func TestFailoverWriterDBWithTelemetryOtel(t *testing.T) {
	auroraTestUtility, environment, err := failoverSetup(t)
	defer test_utils.BasicCleanup(t.Name())
	assert.Nil(t, err)
	bsp, err := test_utils.SetupTelemetry(environment)
	assert.Nil(t, err)
	assert.NotNil(t, bsp)
	defer func() { _ = bsp.Shutdown(context.TODO()) }()
	dsn := test_utils.GetDsnForTestsWithProxy(environment, map[string]string{
		"host":                    environment.Info().ProxyDatabaseInfo.ClusterEndpoint,
		"plugins":                 "failover",
		"enableTelemetry":         "true",
		"telemetryTracesBackend":  "OTLP",
		"telemetryMetricsBackend": "OTLP",
	})
	db, err := test_utils.OpenDb(environment.Info().Request.Engine, dsn)
	assert.Nil(t, err)
	assert.NotNil(t, db)
	defer db.Close()

	// Verify connection works.
	err = db.Ping()
	require.NoError(t, err, "Failed to open database connection.")

	// Failover and check that it has failed over.
	failoverComplete := make(chan struct{})
	var triggerFailoverError error
	go func() {
		time.Sleep(5 * time.Second) // Give the query time to reach the database.
		triggerFailoverError = auroraTestUtility.CrashInstance("", "", "")
		close(failoverComplete)
	}()
	_, queryError := test_utils.ExecuteQueryDB(environment.Info().Request.Engine, db, test_utils.GetSleepSql(environment.Info().Request.Engine, 60), 61)
	<-failoverComplete
	require.NoError(t, triggerFailoverError, "Request to DB to failover did not succeed.")
	require.Error(t, queryError, "Failover plugin did not complete failover successfully.")
	assert.Equal(t, error_util.GetMessage("Failover.connectionChangedError"), queryError.Error())

	// Assert that we are connected to the new writer after failover.
	instanceId, err := test_utils.ExecuteInstanceQueryDB(environment.Info().Request.Engine, environment.Info().Request.Deployment, db)
	assert.Nil(t, err)
	currWriterId, err := auroraTestUtility.GetClusterWriterInstanceId("")
	assert.Nil(t, err)
	assert.Equal(t, currWriterId, instanceId)
}

func TestFailoverWriterDBWithTelemetryXray(t *testing.T) {
	auroraTestUtility, environment, err := failoverSetup(t)
	defer test_utils.BasicCleanup(t.Name())
	assert.Nil(t, err)
	bsp, err := test_utils.SetupTelemetry(environment)
	assert.Nil(t, err)
	assert.NotNil(t, bsp)
	defer func() { _ = bsp.Shutdown(context.TODO()) }()
	dsn := test_utils.GetDsnForTestsWithProxy(environment, map[string]string{
		"host":                    environment.Info().ProxyDatabaseInfo.ClusterEndpoint,
		"plugins":                 "failover",
		"enableTelemetry":         "true",
		"telemetryTracesBackend":  "XRAY",
		"telemetryMetricsBackend": "OTLP",
	})
	db, err := test_utils.OpenDb(environment.Info().Request.Engine, dsn)
	assert.Nil(t, err)
	assert.NotNil(t, db)
	defer db.Close()

	// Verify connection works.
	err = db.Ping()
	require.NoError(t, err, "Failed to open database connection.")

	// Failover and check that it has failed over.
	failoverComplete := make(chan struct{})
	var triggerFailoverError error
	go func() {
		time.Sleep(5 * time.Second) // Give the query time to reach the database.
		triggerFailoverError = auroraTestUtility.CrashInstance("", "", "")
		close(failoverComplete)
	}()
	_, queryError := test_utils.ExecuteQueryDB(environment.Info().Request.Engine, db, test_utils.GetSleepSql(environment.Info().Request.Engine, 60), 61)
	<-failoverComplete
	require.NoError(t, triggerFailoverError, "Request to DB to failover did not succeed.")
	require.Error(t, queryError, "Failover plugin did not complete failover successfully.")
	assert.Equal(t, error_util.GetMessage("Failover.connectionChangedError"), queryError.Error())

	// Assert that we are connected to the new writer after failover.
	instanceId, err := test_utils.ExecuteInstanceQueryDB(environment.Info().Request.Engine, environment.Info().Request.Deployment, db)
	assert.Nil(t, err)
	currWriterId, err := auroraTestUtility.GetClusterWriterInstanceId("")
	assert.Nil(t, err)
	assert.Equal(t, currWriterId, instanceId)
}

func TestFailoverWriterInTransactionWithBegin(t *testing.T) {
	auroraTestUtility, environment, err := failoverSetup(t)
	defer test_utils.BasicCleanup(t.Name())
	assert.Nil(t, err)
	dsn := test_utils.GetDsnForTestsWithProxy(environment, map[string]string{
		"host":    environment.Info().ProxyDatabaseInfo.WriterInstanceEndpoint(),
		"plugins": "failover",
	})
	db, err := test_utils.OpenDb(environment.Info().Request.Engine, dsn)
	assert.Nil(t, err)
	assert.NotNil(t, db)
	defer db.Close()

	// Check that we are connected to the writer.
	instanceId, err := test_utils.ExecuteInstanceQueryDB(environment.Info().Request.Engine, environment.Info().Request.Deployment, db)
	assert.Nil(t, err)
	require.True(t, auroraTestUtility.IsDbInstanceWriter(instanceId, ""))

	// Create a test table to verify transaction rollback
	_, err = db.Exec("DROP TABLE IF EXISTS test_failover_tx_rollback")
	assert.Nil(t, err)
	_, err = db.Exec("CREATE TABLE test_failover_tx_rollback (id INT, value VARCHAR(50))")
	assert.Nil(t, err)

	// Use Begin to start a transaction.
	tx, err := db.Begin()
	assert.Nil(t, err)

	// Make changes in the transaction that should be rolled back after failover
	_, err = tx.Exec("INSERT INTO test_failover_tx_rollback VALUES (1, 'should be rolled back')")
	assert.Nil(t, err)

	// Failover and check that it has failed over.
	triggerFailoverError := auroraTestUtility.CrashInstance("", "", "")
	assert.Nil(t, triggerFailoverError)

	_, txErr := tx.Exec("INSERT INTO test_failover_tx_rollback VALUES (2, 'should fail to execute')")
	require.Error(t, txErr)
	assert.Equal(t, error_util.GetMessage("Failover.transactionResolutionUnknownError"), txErr.Error())

	// Verify that the transaction was rolled back by checking the table contents.
	var count int = -1
	err = db.QueryRow("SELECT COUNT(*) FROM test_failover_tx_rollback").Scan(&count)
	assert.Nil(t, err)
	assert.Equal(t, 0, count, "Transaction should have been rolled back, table should be empty.")

	assert.Nil(t, test_utils.VerifyClusterStatus())
	environment, err = test_utils.GetCurrentTestEnvironment()
	assert.Nil(t, err)
	cleanup_db, err := test_utils.OpenDb(environment.Info().Request.Engine, test_utils.GetDsn(environment, map[string]string{
		"host": environment.Info().DatabaseInfo.WriterInstanceEndpoint(),
		"port": strconv.Itoa(environment.Info().DatabaseInfo.InstanceEndpointPort),
	}))
	assert.Nil(t, err)
	assert.NotNil(t, cleanup_db)
	defer cleanup_db.Close()

	// Check that we are connected to the writer.
	instanceId, err = test_utils.ExecuteInstanceQueryDB(environment.Info().Request.Engine, environment.Info().Request.Deployment, cleanup_db)
	assert.Nil(t, err)
	require.True(t, auroraTestUtility.IsDbInstanceWriter(instanceId, ""))

	_, err = cleanup_db.Exec("DROP TABLE IF EXISTS test_failover_tx_rollback")
	assert.Nil(t, err)
}

func TestFailoverDisableProxies(t *testing.T) {
	_, environment, err := failoverSetup(t)
	defer test_utils.BasicCleanup(t.Name())
	assert.Nil(t, err)
	dsn := getDsnForTestsWithProxy(environment, environment.Info().ProxyDatabaseInfo.ClusterEndpoint, "failover")
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
	slog.Debug("Disabling all proxies.")
	test_utils.DisableAllProxies()

	// Wait for the query to complete and check the error
	queryErr := <-queryChan
	close(queryChan)
	require.NotNil(t, queryErr)
	assert.Equal(t, error_util.GetMessage("Failover.unableToRefreshHostList"), queryErr.Error())

	// Re-enable connectivity
	slog.Debug("Re-enabling all proxies.")
	test_utils.EnableAllProxies()

	newInstanceId, err := test_utils.ExecuteInstanceQueryDB(environment.Info().Request.Engine, environment.Info().Request.Deployment, db)
	assert.Nil(t, err, "After connectivity is re-enabled new connections should not throw errors.")
	assert.NotZero(t, newInstanceId)
}

func TestFailoverEfmDisableAllInstances(t *testing.T) {
	_, environment, err := failoverSetup(t)
	defer test_utils.BasicCleanup(t.Name())
	assert.Nil(t, err)
	dsn := getDsnForTestsWithProxy(environment, environment.Info().ProxyDatabaseInfo.ClusterEndpoint, "failover,efm")
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
	assert.Equal(t, error_util.GetMessage("Failover.unableToRefreshHostList"), queryErr.Error())

	// Re-enable connectivity
	slog.Debug("Re-enabling all connectivity.")
	test_utils.EnableAllConnectivity(true)

	newInstanceId, err := test_utils.ExecuteInstanceQueryDB(environment.Info().Request.Engine, environment.Info().Request.Deployment, db)
	assert.Nil(t, err, "After connectivity is re-enabled new connections should not throw errors.")
	assert.NotZero(t, newInstanceId)
}
