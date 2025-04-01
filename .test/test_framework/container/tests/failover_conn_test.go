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
	"time"

	"github.com/aws/aws-advanced-go-wrapper/.test/test_framework/container/test_utils"

	"database/sql/driver"
	"log"
	"strconv"
	"testing"

	"github.com/aws/aws-advanced-go-wrapper/awssql/error_util"
	_ "github.com/aws/aws-advanced-go-wrapper/otlp"
	_ "github.com/aws/aws-advanced-go-wrapper/xray"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func failoverSetup(t *testing.T) (*test_utils.AuroraTestUtility, *test_utils.TestEnvironment, error) {
	environment, err := test_utils.GetCurrentTestEnvironment()
	assert.Nil(t, err)
	test_utils.SkipForTestEnvironmentFeatures(t, environment.Info().Request.Features, test_utils.LIMITLESS_DEPLOYMENT)

	if environment.Info().Request.InstanceCount < 2 {
		t.Skipf("Skipping integration test %s, instanceCount = %v.", t.Name(), environment.Info().Request.InstanceCount)
	}
	auroraTestUtility := test_utils.NewAuroraTestUtility(environment.Info().Region)
	return auroraTestUtility, environment, test_utils.BasicSetup(t.Name())
}

func TestFailoverWriter(t *testing.T) {
	auroraTestUtility, environment, err := failoverSetup(t)
	defer test_utils.BasicCleanup(t.Name())
	assert.Nil(t, err)
	dsn := test_utils.GetDsn(environment, map[string]string{
		"host":    environment.Info().DatabaseInfo.ClusterEndpoint,
		"port":    strconv.Itoa(environment.Info().DatabaseInfo.InstanceEndpointPort),
		"plugins": "failover",
	})
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
	triggerFailoverError := auroraTestUtility.FailoverClusterAndWaitTillWriterChanged(instanceId, "", "")
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
	assert.NotEqual(t, instanceId, newInstanceId)

	test_utils.BasicCleanup(t.Name())
}

func TestFailoverWriterWithTelemetryOtel(t *testing.T) {
	auroraTestUtility, environment, err := failoverSetup(t)
	assert.Nil(t, err)
	bsp, err := test_utils.SetupTelemetry(environment)
	assert.Nil(t, err)
	assert.NotNil(t, bsp)
	defer func() { _ = bsp.Shutdown(context.TODO()) }()

	dsn := test_utils.GetDsn(environment, map[string]string{
		"host":                    environment.Info().DatabaseInfo.ClusterEndpoint,
		"port":                    strconv.Itoa(environment.Info().DatabaseInfo.InstanceEndpointPort),
		"plugins":                 "failover",
		"enableTelemetry":         "true",
		"telemetryTracesBackend":  "OTLP",
		"telemetryMetricsBackend": "OTLP",
	})
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
	triggerFailoverError := auroraTestUtility.FailoverClusterAndWaitTillWriterChanged(instanceId, "", "")
	assert.Nil(t, triggerFailoverError)
	_, queryError := test_utils.ExecuteInstanceQuery(environment.Info().Request.Engine, environment.Info().Request.Deployment, conn)
	require.Error(t, queryError, "Failover plugin did not complete failover successfully.")
	assert.Equal(t, error_util.GetMessage("Failover.connectionChangedError"), queryError.Error())

	// Assert that we are connected to the new writer after failover.
	newInstanceId, err := test_utils.ExecuteInstanceQuery(environment.Info().Request.Engine, environment.Info().Request.Deployment, conn)
	require.True(t, auroraTestUtility.IsDbInstanceWriter(newInstanceId, ""))
	assert.Nil(t, err, "After failover new connections should not throw errors.")
	assert.NotZero(t, newInstanceId)

	test_utils.BasicCleanup(t.Name())
}

func TestFailoverWriterWithTelemetryXray(t *testing.T) {
	auroraTestUtility, environment, err := failoverSetup(t)
	assert.Nil(t, err)
	bsp, err := test_utils.SetupTelemetry(environment)
	assert.Nil(t, err)
	assert.NotNil(t, bsp)
	defer func() { _ = bsp.Shutdown(context.TODO()) }()

	dsn := test_utils.GetDsn(environment, map[string]string{
		"host":                    environment.Info().DatabaseInfo.ClusterEndpoint,
		"port":                    strconv.Itoa(environment.Info().DatabaseInfo.InstanceEndpointPort),
		"plugins":                 "failover",
		"enableTelemetry":         "true",
		"telemetryTracesBackend":  "XRAY",
		"telemetryMetricsBackend": "OTLP",
	})
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
	triggerFailoverError := auroraTestUtility.FailoverClusterAndWaitTillWriterChanged(instanceId, "", "")
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
	assert.NotEqual(t, instanceId, newInstanceId)

	test_utils.BasicCleanup(t.Name())
}

func TestFailoverWriterEndpoint(t *testing.T) {
	auroraTestUtility, environment, err := failoverSetup(t)
	defer test_utils.BasicCleanup(t.Name())
	assert.Nil(t, err)
	dsn := test_utils.GetDsn(environment, map[string]string{
		"host":    environment.Info().DatabaseInfo.WriterInstanceEndpoint(),
		"port":    strconv.Itoa(environment.Info().DatabaseInfo.InstanceEndpointPort),
		"plugins": "failover",
	})
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
	triggerFailoverError := auroraTestUtility.FailoverClusterAndWaitTillWriterChanged(instanceId, "", "")
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
	assert.NotEqual(t, instanceId, newInstanceId)
}

func TestFailoverReaderOrWriter(t *testing.T) {
	auroraTestUtility, environment, err := failoverSetup(t)
	defer test_utils.BasicCleanup(t.Name())
	assert.Nil(t, err)
	dsn := test_utils.GetDsn(environment, map[string]string{
		"host":         environment.Info().DatabaseInfo.WriterInstanceEndpoint(),
		"port":         strconv.Itoa(environment.Info().DatabaseInfo.InstanceEndpointPort),
		"plugins":      "failover",
		"failoverMode": "reader-or-writer",
	})
	wrapperDriver := test_utils.NewWrapperDriver(environment.Info().Request.Engine)

	conn, err := wrapperDriver.Open(dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	// Check that we are connected.
	instanceId, err := test_utils.ExecuteInstanceQuery(environment.Info().Request.Engine, environment.Info().Request.Deployment, conn)
	assert.Nil(t, err)
	assert.NotZero(t, instanceId)

	// Failover and check that it has failed over.
	triggerFailoverError := auroraTestUtility.FailoverClusterAndWaitTillWriterChanged("", "", "")
	assert.Nil(t, triggerFailoverError)
	_, queryError := test_utils.ExecuteInstanceQuery(environment.Info().Request.Engine, environment.Info().Request.Deployment, conn)
	require.Error(t, queryError, "Failover plugin did not complete failover successfully.")
	assert.Equal(t, error_util.GetMessage("Failover.connectionChangedError"), queryError.Error())

	// Assert that we are able to connect after failover.
	newInstanceId, err := test_utils.ExecuteInstanceQuery(environment.Info().Request.Engine, environment.Info().Request.Deployment, conn)
	assert.Nil(t, err)
	assert.NotZero(t, newInstanceId)
}

func TestFailoverStrictReader(t *testing.T) {
	auroraTestUtility, environment, err := failoverSetup(t)
	defer test_utils.BasicCleanup(t.Name())
	assert.Nil(t, err)
	dsn := test_utils.GetDsn(environment, map[string]string{
		"host":              environment.Info().DatabaseInfo.ReaderInstance().Host(),
		"port":              strconv.Itoa(environment.Info().DatabaseInfo.InstanceEndpointPort),
		"plugins":           "failover",
		"failoverMode":      "strict-reader",
		"failoverTimeoutMs": "30000",
	})
	wrapperDriver := test_utils.NewWrapperDriver(environment.Info().Request.Engine)

	conn, err := wrapperDriver.Open(dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	// Check that we are not connected to the writer.
	instanceId, err := test_utils.ExecuteInstanceQuery(environment.Info().Request.Engine, environment.Info().Request.Deployment, conn)
	assert.Nil(t, err)
	assert.False(t, auroraTestUtility.IsDbInstanceWriter(instanceId, ""))

	// Failover and check that it has failed over.
	triggerFailoverError := auroraTestUtility.FailoverClusterAndWaitTillWriterChanged("", "", "")
	assert.Nil(t, triggerFailoverError)
	_, queryError := test_utils.ExecuteInstanceQuery(environment.Info().Request.Engine, environment.Info().Request.Deployment, conn)
	require.Error(t, queryError, "Failover plugin did not complete failover successfully.")
	assert.Equal(t, error_util.GetMessage("Failover.connectionChangedError"), queryError.Error())

	// Assert that we are connected to a reader instance after failover.
	newInstanceId, err := test_utils.ExecuteInstanceQuery(environment.Info().Request.Engine, environment.Info().Request.Deployment, conn)
	assert.Nil(t, err)
	assert.False(t, auroraTestUtility.IsDbInstanceWriter(newInstanceId, ""))
}

func TestFailoverWriterInTransactionWithSQL(t *testing.T) {
	auroraTestUtility, environment, err := failoverSetup(t)
	defer test_utils.BasicCleanup(t.Name())
	assert.Nil(t, err)
	dsn := test_utils.GetDsn(environment, map[string]string{
		"host":    environment.Info().DatabaseInfo.WriterInstanceEndpoint(),
		"port":    strconv.Itoa(environment.Info().DatabaseInfo.InstanceEndpointPort),
		"plugins": "failover",
	})
	wrapperDriver := test_utils.NewWrapperDriver(environment.Info().Request.Engine)

	conn, err := wrapperDriver.Open(dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	// Check that we are connected to the writer.
	instanceId, err := test_utils.ExecuteInstanceQuery(environment.Info().Request.Engine, environment.Info().Request.Deployment, conn)
	assert.Nil(t, err)
	require.True(t, auroraTestUtility.IsDbInstanceWriter(instanceId, ""))

	// Create a test table to verify transaction rollback
	ctx := context.TODO()
	execer, ok := conn.(driver.ExecerContext)
	assert.True(t, ok)
	_, err = execer.ExecContext(ctx, "DROP TABLE IF EXISTS test_failover_tx_rollback", []driver.NamedValue{})
	assert.Nil(t, err)
	_, err = execer.ExecContext(ctx, "CREATE TABLE test_failover_tx_rollback (id INT, value VARCHAR(50))", []driver.NamedValue{})
	assert.Nil(t, err)

	// Use SQL statement of BEGIN to start a transaction, make changes in the transaction that should be rolled back after failover.
	_, err = execer.ExecContext(ctx, "BEGIN", []driver.NamedValue{})
	assert.Nil(t, err)
	_, err = execer.ExecContext(ctx, "INSERT INTO test_failover_tx_rollback VALUES (1, 'should be rolled back')", []driver.NamedValue{})
	assert.Nil(t, err)

	// Failover and check that it has failed over.
	triggerFailoverError := auroraTestUtility.FailoverClusterAndWaitTillWriterChanged("", "", "")
	assert.Nil(t, triggerFailoverError)
	_, queryError := test_utils.ExecuteInstanceQuery(environment.Info().Request.Engine, environment.Info().Request.Deployment, conn)
	require.Error(t, queryError, "Failover plugin did not complete failover successfully.")
	assert.Equal(t, error_util.GetMessage("Failover.transactionResolutionUnknownError"), queryError.Error())

	// Verify that the transaction was rolled back by checking the table contents.
	count, err := test_utils.GetFirstItemFromQueryAsInt(conn, "SELECT COUNT(*) FROM test_failover_tx_rollback")
	assert.Nil(t, err)
	assert.Equal(t, 0, count, "Transaction should have been rolled back, table should be empty.")

	// Check that we are connected to the writer.
	instanceId, err = test_utils.ExecuteInstanceQuery(environment.Info().Request.Engine, environment.Info().Request.Deployment, conn)
	assert.Nil(t, err)
	require.True(t, auroraTestUtility.IsDbInstanceWriter(instanceId, ""))

	// Clean up the test table
	_, err = execer.ExecContext(ctx, "DROP TABLE IF EXISTS test_failover_tx_rollback", []driver.NamedValue{})
	assert.Nil(t, err)
}

func TestFailoverEfmDisableInstance(t *testing.T) {
	_, environment, err := failoverSetup(t)
	defer test_utils.BasicCleanup(t.Name())
	assert.Nil(t, err)
	dsn := getDsnForTestsWithProxy(environment, environment.Info().ProxyDatabaseInfo.WriterInstanceEndpoint(), "efm,failover")
	wrapperDriver := test_utils.NewWrapperDriver(environment.Info().Request.Engine)

	conn, err := wrapperDriver.Open(dsn)
	if err != nil {
		log.Fatal(err)
	}
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
	slog.Debug("Disabling proxy connectivity.")
	test_utils.DisableProxyConnectivity(proxyInfo)

	// Wait for the query to complete and check the error
	queryErr := <-queryChan
	close(queryChan)
	require.NotNil(t, queryErr)
	assert.Equal(t, error_util.GetMessage("Failover.unableToRefreshHostList"), queryErr.Error())

	// Re-enable connectivity
	slog.Debug("Re-enabling proxy connectivity.")
	test_utils.EnableProxyConnectivity(proxyInfo, true)
}

func TestFailoverWriterMaintainSessionState(t *testing.T) {
	auroraTestUtility, environment, err := failoverSetup(t)
	defer test_utils.BasicCleanup(t.Name())
	assert.Nil(t, err)
	dsn := test_utils.GetDsn(environment, map[string]string{
		"host":    environment.Info().DatabaseInfo.ClusterEndpoint,
		"port":    strconv.Itoa(environment.Info().DatabaseInfo.InstanceEndpointPort),
		"plugins": "failover",
	})
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

	SetupSessionState(t, environment.Info().Request.Engine, conn)

	// Failover and check that it has failed over.
	triggerFailoverError := auroraTestUtility.FailoverClusterAndWaitTillWriterChanged(instanceId, "", "")
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
	assert.NotEqual(t, instanceId, newInstanceId)

	VerifySessionStateMaintained(t, environment.Info().Request.Engine, conn)

	test_utils.BasicCleanup(t.Name())
}

func SetupSessionState(t *testing.T, engine test_utils.DatabaseEngine, conn driver.Conn) {
	var statements []string
	if engine == test_utils.MYSQL {
		statements = append(statements, "create database if not exists test_session_state ")
		statements = append(statements, "set session transaction read only")
		statements = append(statements, "set session transaction isolation level read committed")
		statements = append(statements, "set autocommit=0")
		statements = append(statements, "use test_session_state")
	} else {
		statements = append(statements, "set session characteristics as transaction read only")
		statements = append(statements, "set session characteristics as transaction isolation level read uncommitted")
		statements = append(statements, "set search_path to myschema,public")
	}

	for _, statement := range statements {
		_, err := test_utils.ExecuteQuery(engine, conn, statement, 15)
		assert.Nil(t, err)
	}
}

func VerifySessionStateMaintained(t *testing.T, engine test_utils.DatabaseEngine, conn driver.Conn) {
	var sql string
	if engine == test_utils.MYSQL {
		sql = "select @@session.transaction_read_only"
		result, err := test_utils.GetFirstItemFromQueryAsInt(conn, sql)
		assert.Nil(t, err)
		assert.Equal(t, 1, result)

		sql = "select @@session.transaction_isolation"
		level, err := test_utils.GetFirstItemFromQueryAsString(engine, conn, sql)
		assert.Nil(t, err)
		assert.Equal(t, "READ-COMMITTED", level)

		sql = "select @@autocommit"
		autoCommit, err := test_utils.GetFirstItemFromQueryAsInt(conn, sql)
		assert.Nil(t, err)
		assert.Equal(t, 0, autoCommit)

		sql = "select database()"
		catalog, err := test_utils.GetFirstItemFromQueryAsString(engine, conn, sql)
		assert.Nil(t, err)
		assert.Equal(t, "test_session_state", catalog)
	} else {
		sql = "show transaction_read_only"
		readOnly, err := test_utils.GetFirstItemFromQueryAsString(engine, conn, sql)
		assert.Nil(t, err)
		assert.Equal(t, "on", readOnly)

		sql = "show transaction isolation level"
		level, err := test_utils.GetFirstItemFromQueryAsString(engine, conn, sql)
		assert.Nil(t, err)
		assert.Equal(t, "read uncommitted", level)

		sql = "show search_path"
		path, err := test_utils.GetFirstItemFromQueryAsString(engine, conn, sql)
		assert.Nil(t, err)
		assert.Equal(t, "myschema, public", path)
	}
}
