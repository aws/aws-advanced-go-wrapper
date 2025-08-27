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
	"database/sql"
	"log/slog"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-advanced-go-wrapper/.test/test_framework/container/test_utils"
	"github.com/aws/aws-advanced-go-wrapper/awssql/awsctx"
	"github.com/aws/aws-advanced-go-wrapper/awssql/driver_infrastructure"
	"github.com/aws/aws-advanced-go-wrapper/awssql/error_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/internal_pool"
	"github.com/aws/aws-advanced-go-wrapper/awssql/plugin_helpers"
	"github.com/aws/aws-advanced-go-wrapper/awssql/property_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	readOnlyCtx = context.WithValue(context.Background(), awsctx.SetReadOnly, true)
	writeCtx    = context.WithValue(context.Background(), awsctx.SetReadOnly, false)
)

func setupReadWriteSplittingTest(t *testing.T) (*test_utils.TestEnvironment, *test_utils.AuroraTestUtility, *sql.DB) {
	env, err := test_utils.GetCurrentTestEnvironment()
	require.NoError(t, err)
	auroraTestUtility := test_utils.NewAuroraTestUtility(env.Info().Region)

	props := map[string]string{
		"plugins": "readWriteSplitting,efm,failover",
	}
	dsn := test_utils.GetDsn(env, props)

	db, err := test_utils.OpenDb(env.Info().Request.Engine, dsn)
	require.NoError(t, err)

	return env, auroraTestUtility, db
}

func executeInstanceQuery(env *test_utils.TestEnvironment, dbOrConn interface{}, ctx context.Context, timeout int) (string, error) {
	return test_utils.ExecuteInstanceQueryContextWithTimeout(
		env.Info().Request.Engine,
		env.Info().Request.Deployment,
		dbOrConn,
		timeout,
		ctx,
	)
}

func executeInstanceQueryReadOnly(env *test_utils.TestEnvironment, dbOrConn interface{}, timeout int) (string, error) {
	return executeInstanceQuery(env, dbOrConn, readOnlyCtx, timeout)
}

func executeInstanceQueryWrite(env *test_utils.TestEnvironment, dbOrConn interface{}, timeout int) (string, error) {
	return executeInstanceQuery(env, dbOrConn, writeCtx, timeout)
}

func skipIfInsufficientInstances(t *testing.T, env *test_utils.TestEnvironment, minInstances int) {
	if env.Info().Request.InstanceCount < minInstances {
		t.Skipf("Skipping integration test %s, instanceCount = %v.", t.Name(), env.Info().Request.InstanceCount)
	}
}

func rwsplit_getDsnForTestsWithProxy(environment *test_utils.TestEnvironment, host string, plugins string, timeout int) string {
	return test_utils.GetDsn(environment, rwsplit_getPropsForTestsWithProxy(environment, host, plugins, timeout))
}

func rwsplit_getPropsForTestsWithProxy(environment *test_utils.TestEnvironment, host string, plugins string, timeout int) map[string]string {
	monitoringConnectTimeoutSeconds := strconv.Itoa(timeout - 1)
	monitoringConnectTimeoutParameterName := property_util.MONITORING_PROPERTY_PREFIX
	var driverProtocol = ""
	var timeoutParamName = ""
	switch environment.Info().Request.Engine {
	case test_utils.PG:
		timeoutParamName = "connect_timeout"
		monitoringConnectTimeoutParameterName = monitoringConnectTimeoutParameterName + timeoutParamName
		driverProtocol = utils.PGX_DRIVER_PROTOCOL
	case test_utils.MYSQL:
		timeoutParamName = "readTimeout"
		monitoringConnectTimeoutParameterName = monitoringConnectTimeoutParameterName + "readTimeout"
		monitoringConnectTimeoutSeconds = monitoringConnectTimeoutSeconds + "s"
		driverProtocol = utils.MYSQL_DRIVER_PROTOCOL
	}
	return map[string]string{
		"host":                       host,
		"port":                       strconv.Itoa(environment.Info().ProxyDatabaseInfo.InstanceEndpointPort),
		"clusterInstanceHostPattern": "?." + environment.Info().ProxyDatabaseInfo.InstanceEndpointSuffix,
		"plugins":                    plugins,
		"failureDetectionIntervalMs": strconv.Itoa(5 * 1000), // interval between probes to host
		"failureDetectionCount":      strconv.Itoa(3),        // consecutive failures before marks host as dead
		"failureDetectionTimeMs":     strconv.Itoa(1 * 1000), // time before starting monitoring
		"failoverTimeoutMs":          strconv.Itoa(15 * 1000),
		// each monitoring connection has monitoringConnectTimeoutSeconds seconds to connect
		monitoringConnectTimeoutParameterName: monitoringConnectTimeoutSeconds,
		timeoutParamName:                      monitoringConnectTimeoutSeconds,
		property_util.DRIVER_PROTOCOL.Name:    driverProtocol,
	}
}

func TestReadWriteSplitting_SetReadOnlyCtxTrue(t *testing.T) {
	err := test_utils.BasicSetup(t.Name())
	require.NoError(t, err)
	defer test_utils.BasicCleanup(t.Name())
	env, err := test_utils.GetCurrentTestEnvironment()
	require.NoError(t, err)
	skipIfInsufficientInstances(t, env, 2)
	env, auroraTestUtility, db := setupReadWriteSplittingTest(t)
	defer db.Close()

	conn, err := db.Conn(context.TODO())
	require.NoError(t, err)

	instanceId, err := executeInstanceQueryReadOnly(env, conn, 5)
	assert.NoError(t, err)
	assert.False(t, auroraTestUtility.IsDbInstanceWriter(instanceId, ""))
}

func TestReadWriteSplitting_SetReadOnlyCtxFalse(t *testing.T) {
	err := test_utils.BasicSetup(t.Name())
	require.NoError(t, err)
	defer test_utils.BasicCleanup(t.Name())
	env, err := test_utils.GetCurrentTestEnvironment()
	require.NoError(t, err)
	skipIfInsufficientInstances(t, env, 2)
	env, auroraTestUtility, db := setupReadWriteSplittingTest(t)
	defer db.Close()

	instanceId, err := executeInstanceQueryWrite(env, db, 5)
	assert.NoError(t, err)
	assert.True(t, auroraTestUtility.IsDbInstanceWriter(instanceId, ""))
}

func TestReadWriteSplitting_SetReadOnlyCtxNoCtx(t *testing.T) {
	err := test_utils.BasicSetup(t.Name())
	require.NoError(t, err)
	defer test_utils.BasicCleanup(t.Name())
	env, err := test_utils.GetCurrentTestEnvironment()
	require.NoError(t, err)
	skipIfInsufficientInstances(t, env, 2)
	env, auroraTestUtility, db := setupReadWriteSplittingTest(t)
	defer db.Close()

	instanceId, err := executeInstanceQuery(env, db, context.TODO(), 5)
	assert.NoError(t, err)
	assert.True(t, auroraTestUtility.IsDbInstanceWriter(instanceId, ""))
}

func TestReadWriteSplitting_SetReadOnlyCtxSwitchFalseTrueNoCtx(t *testing.T) {
	err := test_utils.BasicSetup(t.Name())
	require.NoError(t, err)
	defer test_utils.BasicCleanup(t.Name())
	env, err := test_utils.GetCurrentTestEnvironment()
	require.NoError(t, err)
	skipIfInsufficientInstances(t, env, 2)
	env, auroraTestUtility, db := setupReadWriteSplittingTest(t)
	defer db.Close()

	// Test no context (should be writer)
	instanceId, err := executeInstanceQuery(env, db, context.TODO(), 5)
	assert.NoError(t, err)
	assert.True(t, auroraTestUtility.IsDbInstanceWriter(instanceId, ""))

	// Test switching to read-only mode
	instanceId, err = executeInstanceQueryReadOnly(env, db, 5)
	assert.NoError(t, err)
	assert.False(t, auroraTestUtility.IsDbInstanceWriter(instanceId, ""))

	// Test switching back to write mode
	instanceId, err = executeInstanceQueryWrite(env, db, 5)
	assert.NoError(t, err)
	assert.True(t, auroraTestUtility.IsDbInstanceWriter(instanceId, ""))
}

func TestReadWriteSplitting_SetReadOnlyFalseInReadOnlyTransaction(t *testing.T) {
	err := test_utils.BasicSetup(t.Name())
	require.NoError(t, err)
	defer test_utils.BasicCleanup(t.Name())
	env, err := test_utils.GetCurrentTestEnvironment()
	require.NoError(t, err)
	skipIfInsufficientInstances(t, env, 2)
	env, auroraTestUtility, db := setupReadWriteSplittingTest(t)
	defer db.Close()

	conn, err := db.Conn(context.TODO())
	require.NoError(t, err)

	// Start read-only transaction
	_, err = conn.ExecContext(context.TODO(), "START TRANSACTION READ ONLY")
	require.NoError(t, err)

	// Test query that we are still connected to writer
	instanceId, err := executeInstanceQueryWrite(env, conn, 5)
	assert.NoError(t, err)
	assert.True(t, auroraTestUtility.IsDbInstanceWriter(instanceId, ""))
}

func TestReadWriteSplitting_SetReadOnlyTrueInTransaction(t *testing.T) {
	err := test_utils.BasicSetup(t.Name())
	require.NoError(t, err)
	defer test_utils.BasicCleanup(t.Name())
	env, err := test_utils.GetCurrentTestEnvironment()
	require.NoError(t, err)
	skipIfInsufficientInstances(t, env, 2)
	env, _, db := setupReadWriteSplittingTest(t)
	defer db.Close()

	conn, err := db.Conn(context.TODO())
	require.NoError(t, err)

	// Start read-only transaction
	_, err = conn.ExecContext(readOnlyCtx, "START TRANSACTION READ ONLY")
	require.NoError(t, err)

	// Test that we cannot switch to read only true while in transaction
	_, err = executeInstanceQueryWrite(env, conn, 5)
	assert.Error(t, err)
	assert.Equal(t, error_util.GetMessage("ReadWriteSplittingPlugin.setReadOnlyFalseInTransaction"), err.Error())
}

func TestReadWriteSplitting_ExecuteWithCachedConnection(t *testing.T) {
	err := test_utils.BasicSetup(t.Name())
	require.NoError(t, err)
	defer test_utils.BasicCleanup(t.Name())
	env, err := test_utils.GetCurrentTestEnvironment()
	require.NoError(t, err)
	skipIfInsufficientInstances(t, env, 2)
	env, auroraTestUtility, db := setupReadWriteSplittingTest(t)
	defer db.Close()

	conn, err := db.Conn(context.TODO())
	require.NoError(t, err)

	// Execute query with cached connection
	firstReaderId, err := executeInstanceQueryReadOnly(env, conn, 5)
	assert.NoError(t, err)
	assert.False(t, auroraTestUtility.IsDbInstanceWriter(firstReaderId, ""))

	// Execute with setReadOnly false
	instanceId, err := executeInstanceQueryWrite(env, conn, 5)
	assert.NoError(t, err)
	assert.True(t, auroraTestUtility.IsDbInstanceWriter(instanceId, ""))
	assert.NotEqual(t, firstReaderId, instanceId)

	// Execute with setReadOnly true, verify reader is the same as original
	instanceId, err = executeInstanceQueryReadOnly(env, conn, 5)
	assert.NoError(t, err)
	assert.False(t, auroraTestUtility.IsDbInstanceWriter(instanceId, ""))
	assert.Equal(t, firstReaderId, instanceId)

	// repeat one more time
	// Execute with setReadOnly false
	instanceId, err = executeInstanceQueryWrite(env, conn, 5)
	assert.NoError(t, err)
	assert.True(t, auroraTestUtility.IsDbInstanceWriter(instanceId, ""))
	assert.NotEqual(t, firstReaderId, instanceId)

	// Execute with setReadOnly true, verify reader is the same as original
	instanceId, err = executeInstanceQueryReadOnly(env, conn, 5)
	assert.NoError(t, err)
	assert.False(t, auroraTestUtility.IsDbInstanceWriter(instanceId, ""))
	assert.Equal(t, firstReaderId, instanceId)
}

func TestReadWriteSplitting_NoReaders(t *testing.T) {
	err := test_utils.BasicSetup(t.Name())
	require.NoError(t, err)
	defer test_utils.BasicCleanup(t.Name())
	// Setup + cleanup
	env, err := test_utils.GetCurrentTestEnvironment()
	require.NoError(t, err)
	skipIfInsufficientInstances(t, env, 3)
	auroraTestUtility := test_utils.NewAuroraTestUtility(env.Info().Region)

	require.NoError(t, err)
	instances := env.Info().DatabaseInfo.Instances

	dsn := rwsplit_getDsnForTestsWithProxy(env, env.Info().ProxyDatabaseInfo.ClusterEndpoint, "readWriteSplitting,efm,failover", 2)

	db, err := test_utils.OpenDb(env.Info().Request.Engine, dsn)
	require.NoError(t, err)
	defer db.Close()
	conn, err := db.Conn(context.TODO())
	require.NoError(t, err)
	defer conn.Close()

	// Test switching to read-only mode
	writerId, err := executeInstanceQueryWrite(env, conn, 5)
	require.NoError(t, err)
	assert.True(t, auroraTestUtility.IsDbInstanceWriter(writerId, ""))

	for i := 0; i < len(instances); i++ {
		instance := instances[i]
		if writerId == instance.InstanceId() {
			continue
		}
		test_utils.DisableProxyConnectivity(env.ProxyInfos()[instance.InstanceId()])
	}

	// Test switching to read-only mode
	readerId, err := executeInstanceQueryReadOnly(env, conn, 60)
	require.NoError(t, err)
	assert.True(t, auroraTestUtility.IsDbInstanceWriter(readerId, ""))
}

func TestReadWriteSplitting_WriterFailover(t *testing.T) {
	utils.SetPreparedHostFunc(func(host string) string {
		preparedHost := host
		const suffix = ".proxied"
		if strings.HasSuffix(host, suffix) {
			preparedHost = strings.TrimSuffix(host, suffix)
		}
		return preparedHost
	})
	defer utils.ResetPreparedHostFunc()
	err := test_utils.BasicSetup(t.Name())
	require.NoError(t, err)
	defer test_utils.BasicCleanup(t.Name())
	env, err := test_utils.GetCurrentTestEnvironment()
	require.NoError(t, err)
	skipIfInsufficientInstances(t, env, 3)

	auroraTestUtility := test_utils.NewAuroraTestUtility(env.Info().Region)

	require.NoError(t, err)
	instances := env.Info().DatabaseInfo.Instances

	dsn := rwsplit_getDsnForTestsWithProxy(env, env.Info().ProxyDatabaseInfo.ClusterEndpoint, "readWriteSplitting,efm,failover", 5)

	db, err := test_utils.OpenDb(env.Info().Request.Engine, dsn)
	require.NoError(t, err)
	defer db.Close()

	conn, err := db.Conn(context.TODO())
	require.NoError(t, err)
	defer conn.Close()

	// Test switching to read-only mode
	originalWriterId, err := executeInstanceQueryWrite(env, conn, 5)
	require.NoError(t, err)
	assert.True(t, auroraTestUtility.IsDbInstanceWriter(originalWriterId, ""))

	// Disable connections from everything but the writer
	for i := 0; i < len(instances); i++ {
		instance := instances[i]
		if originalWriterId == instance.InstanceId() {
			continue
		}
		test_utils.DisableProxyConnectivity(env.ProxyInfos()[instance.InstanceId()])
	}

	// Connect to writer id from read-write
	instanceId, err := executeInstanceQueryReadOnly(env, conn, 60)
	require.NoError(t, err)
	assert.Equal(t, originalWriterId, instanceId)

	// switch back to write mode
	// Connect to writer id from read-write
	instanceId, err = executeInstanceQueryWrite(env, conn, 60)
	require.NoError(t, err)
	assert.Equal(t, originalWriterId, instanceId)

	// failover
	test_utils.EnableAllConnectivity(true)
	err = auroraTestUtility.FailoverClusterAndWaitTillWriterChanged(originalWriterId, "", "")
	require.NoError(t, err)

	// failover error
	_, err = executeInstanceQueryWrite(env, conn, 60)
	assert.Error(t, err)
	awsWrapperError, ok := err.(*error_util.AwsWrapperError)
	assert.True(t, ok)
	assert.True(t, awsWrapperError.IsFailoverErrorType())
	assert.True(t, awsWrapperError.IsType(error_util.FailoverSuccessErrorType))

	instanceId, err = executeInstanceQueryWrite(env, conn, 60)
	require.NoError(t, err)
	assert.True(t, auroraTestUtility.IsDbInstanceWriter(instanceId, ""))

	// Assert we are now connected to a reader in read-mode
	// Connect to writer id from read-write
	instanceId, err = executeInstanceQueryReadOnly(env, conn, 60)
	require.NoError(t, err)
	assert.False(t, auroraTestUtility.IsDbInstanceWriter(instanceId, ""))
}

func TestReadWriteSplitting_FailoverToNewReader(t *testing.T) {
	utils.SetPreparedHostFunc(func(host string) string {
		preparedHost := host
		const suffix = ".proxied"
		if strings.HasSuffix(host, suffix) {
			preparedHost = strings.TrimSuffix(host, suffix)
		}
		return preparedHost
	})
	defer utils.ResetPreparedHostFunc()
	err := test_utils.BasicSetup(t.Name())
	require.NoError(t, err)
	defer test_utils.BasicCleanup(t.Name())
	env, err := test_utils.GetCurrentTestEnvironment()
	require.NoError(t, err)

	auroraTestUtility := test_utils.NewAuroraTestUtility(env.Info().Region)

	skipIfInsufficientInstances(t, env, 3)

	props := rwsplit_getPropsForTestsWithProxy(env, env.Info().ProxyDatabaseInfo.ClusterEndpoint, "readWriteSplitting,efm,failover", 5)
	property_util.FAILOVER_MODE.Set(props, "reader-or-writer")
	property_util.CLUSTER_TOPOLOGY_REFRESH_RATE_MS.Set(props, "5000")
	dsn := test_utils.GetDsn(env, props)

	db, err := test_utils.OpenDb(env.Info().Request.Engine, dsn)
	require.NoError(t, err)
	defer db.Close()

	conn, err := db.Conn(context.TODO())
	require.NoError(t, err)
	defer conn.Close()

	originalWriterId, err := executeInstanceQueryWrite(env, conn, 60)
	require.NoError(t, err)
	assert.True(t, auroraTestUtility.IsDbInstanceWriter(originalWriterId, ""))
	slog.Info("originalWriterId", "writerId", originalWriterId)
	originalReaderId, err := executeInstanceQueryReadOnly(env, conn, 60)
	require.NoError(t, err)

	var otherReaderId string = ""

	instances := env.Info().DatabaseInfo.Instances

	for i := 1; i < len(instances); i++ {
		if instances[i].InstanceId() != originalReaderId {
			otherReaderId = instances[i].InstanceId()
			break
		}
	}

	require.NotEmpty(t, otherReaderId)

	// Kill all instances except for one other reader
	for i := 0; i < len(instances); i++ {
		if instances[i].InstanceId() != otherReaderId {
			test_utils.DisableProxyConnectivity(env.ProxyInfos()[instances[i].InstanceId()])
		}
	}

	_, err = executeInstanceQueryReadOnly(env, conn, 60)
	assert.Error(t, err)
	awsWrapperError, ok := err.(*error_util.AwsWrapperError)
	assert.True(t, ok)
	assert.True(t, awsWrapperError.IsFailoverErrorType())
	assert.True(t, awsWrapperError.IsType(error_util.FailoverSuccessErrorType))

	currentReaderId, err := executeInstanceQueryReadOnly(env, conn, 10)
	require.NoError(t, err)

	assert.Equal(t, otherReaderId, currentReaderId)
	assert.NotEqual(t, originalReaderId, currentReaderId)

	test_utils.EnableAllConnectivity(true)
	time.Sleep(time.Duration(20) * time.Second)
	currentId, err := executeInstanceQueryWrite(env, conn, 10)
	require.NoError(t, err)
	assert.Equal(t, originalWriterId, currentId)

	currentId, err = executeInstanceQueryReadOnly(env, conn, 10)
	require.NoError(t, err)
	assert.Equal(t, otherReaderId, currentId)
}

func TestReadWriteSplitting_FailoverReaderToWriter(t *testing.T) {
	utils.SetPreparedHostFunc(func(host string) string {
		preparedHost := host
		const suffix = ".proxied"
		if strings.HasSuffix(host, suffix) {
			preparedHost = strings.TrimSuffix(host, suffix)
		}
		return preparedHost
	})
	defer utils.ResetPreparedHostFunc()

	err := test_utils.BasicSetup(t.Name())
	require.NoError(t, err)
	defer test_utils.BasicCleanup(t.Name())
	env, err := test_utils.GetCurrentTestEnvironment()
	require.NoError(t, err)
	skipIfInsufficientInstances(t, env, 3)

	auroraTestUtility := test_utils.NewAuroraTestUtility(env.Info().Region)

	require.NoError(t, err)
	instances := env.Info().DatabaseInfo.Instances

	props := rwsplit_getPropsForTestsWithProxy(env, env.Info().ProxyDatabaseInfo.ClusterEndpoint, "readWriteSplitting,efm,failover", 2)
	property_util.CLUSTER_TOPOLOGY_REFRESH_RATE_MS.Set(props, "5000")
	dsn := test_utils.GetDsn(env, props)

	db, err := test_utils.OpenDb(env.Info().Request.Engine, dsn)
	require.NoError(t, err)
	defer db.Close()

	conn, err := db.Conn(context.TODO())
	require.NoError(t, err)
	defer conn.Close()

	// Get writer
	originalWriterId, err := executeInstanceQueryWrite(env, conn, 5)
	assert.NoError(t, err)
	assert.True(t, auroraTestUtility.IsDbInstanceWriter(originalWriterId, ""))

	// Set read only
	readerId, err := executeInstanceQueryReadOnly(env, conn, 60)
	assert.NoError(t, err)
	assert.NotEqual(t, originalWriterId, readerId)

	// kill all connections except for writer
	for i := 0; i < len(instances); i++ {
		instance := instances[i]
		if originalWriterId == instance.InstanceId() {
			continue
		}
		test_utils.DisableProxyConnectivity(env.ProxyInfos()[instance.InstanceId()])
	}

	time.Sleep(time.Duration(5) * time.Second)

	// connect to reader
	_, err = executeInstanceQueryReadOnly(env, conn, 60)
	assert.Error(t, err)
	awsWrapperError, ok := err.(*error_util.AwsWrapperError)
	assert.True(t, ok)
	assert.True(t, awsWrapperError.IsFailoverErrorType())
	assert.True(t, awsWrapperError.IsType(error_util.FailoverSuccessErrorType))

	instanceId, err := executeInstanceQueryReadOnly(env, conn, 60)
	assert.NoError(t, err)
	assert.Equal(t, originalWriterId, instanceId)

	test_utils.EnableAllConnectivity(true)
	// Wait for hosts to be set to AVAILABLE
	plugin_helpers.ClearCaches()
	time.Sleep(time.Duration(40) * time.Second)

	instanceId, err = executeInstanceQueryWrite(env, conn, 60)
	assert.NoError(t, err)
	assert.Equal(t, originalWriterId, instanceId)

	instanceId, err = executeInstanceQueryReadOnly(env, conn, 60)
	assert.NoError(t, err)
	assert.NotEqual(t, originalWriterId, instanceId)
}

func setInternalPoolProvider(poolOptions ...internal_pool.InternalPoolOption) *internal_pool.InternalPooledConnectionProvider {
	options := internal_pool.NewInternalPoolOptions(poolOptions...)
	provider := internal_pool.NewInternalPooledConnectionProvider(
		options,
		0,
	)
	driver_infrastructure.SetCustomConnectionProvider(provider)
	return provider
}

func TestPooledConnection_Failover(t *testing.T) {
	err := test_utils.BasicSetup(t.Name())
	require.NoError(t, err)
	defer test_utils.BasicCleanup(t.Name())
	utils.SetPreparedHostFunc(func(host string) string {
		preparedHost := host
		const suffix = ".proxied"
		if strings.HasSuffix(host, suffix) {
			preparedHost = strings.TrimSuffix(host, suffix)
		}
		return preparedHost
	})
	defer utils.ResetPreparedHostFunc()
	env, err := test_utils.GetCurrentTestEnvironment()
	require.NoError(t, err)
	skipIfInsufficientInstances(t, env, 3)

	provider := setInternalPoolProvider()
	defer driver_infrastructure.ResetCustomConnectionProvider()
	defer provider.ReleaseResources()

	auroraTestUtility := test_utils.NewAuroraTestUtility(env.Info().Region)

	require.NoError(t, err)

	dsn := rwsplit_getDsnForTestsWithProxy(env, env.Info().ProxyDatabaseInfo.ClusterEndpoint, "readWriteSplitting,efm,failover", 4)

	db, err := test_utils.OpenDb(env.Info().Request.Engine, dsn)
	require.NoError(t, err)
	defer db.Close()

	conn, err := db.Conn(context.TODO())
	require.NoError(t, err)
	defer conn.Close()

	// Test switching to read-only mode
	originalWriterId, err := executeInstanceQueryWrite(env, conn, 5)
	require.NoError(t, err)
	assert.True(t, auroraTestUtility.IsDbInstanceWriter(originalWriterId, ""))

	test_utils.DisableAllConnectivity()
	time.Sleep(time.Duration(5) * time.Second)
	// Expect error
	_, err = executeInstanceQueryReadOnly(env, conn, 5)
	require.Error(t, err)
	awsWrapperError, ok := err.(*error_util.AwsWrapperError)
	assert.True(t, ok)
	assert.True(t, awsWrapperError.IsFailoverErrorType())
	assert.True(t, awsWrapperError.IsType(error_util.FailoverFailedErrorType))

	test_utils.EnableAllConnectivity(true)
	err = test_utils.VerifyClusterStatus()
	require.NoError(t, err)
	conn2, err := db.Conn(context.TODO())
	require.NoError(t, err)
	defer conn2.Close()

	newWriterId, err := executeInstanceQueryWrite(env, conn2, 2)
	require.NoError(t, err)
	assert.Equal(t, originalWriterId, newWriterId)
}

func TestPooledConnection_FailoverInTransaction(t *testing.T) {
	err := test_utils.BasicSetup(t.Name())
	require.NoError(t, err)
	defer test_utils.BasicCleanup(t.Name())
	utils.SetPreparedHostFunc(func(host string) string {
		preparedHost := host
		const suffix = ".proxied"
		if strings.HasSuffix(host, suffix) {
			preparedHost = strings.TrimSuffix(host, suffix)
		}
		return preparedHost
	})
	defer utils.ResetPreparedHostFunc()

	env, err := test_utils.GetCurrentTestEnvironment()
	require.NoError(t, err)
	skipIfInsufficientInstances(t, env, 3)

	setInternalPoolProvider()
	defer driver_infrastructure.ResetCustomConnectionProvider()
	auroraTestUtility := test_utils.NewAuroraTestUtility(env.Info().Region)

	require.NoError(t, err)

	dsn := rwsplit_getDsnForTestsWithProxy(env, env.Info().ProxyDatabaseInfo.ClusterEndpoint, "readWriteSplitting,efm,failover", 5)

	db, err := test_utils.OpenDb(env.Info().Request.Engine, dsn)
	require.NoError(t, err)
	defer db.Close()

	conn, err := db.Conn(context.TODO())
	require.NoError(t, err)
	defer conn.Close()

	// Test switching to read-only mode
	originalWriterId, err := executeInstanceQueryWrite(env, conn, 5)
	require.NoError(t, err)
	assert.True(t, auroraTestUtility.IsDbInstanceWriter(originalWriterId, ""))

	_, err = conn.ExecContext(context.TODO(), "START TRANSACTION READ ONLY")
	require.NoError(t, err)

	_, err = executeInstanceQueryReadOnly(env, conn, 5)
	require.NoError(t, err)

	err = auroraTestUtility.FailoverClusterAndWaitTillWriterChanged(originalWriterId, "", "")
	require.NoError(t, err)

	// Expect error
	_, err = executeInstanceQueryReadOnly(env, conn, 60)
	assert.Error(t, err)
	awsWrapperError, ok := err.(*error_util.AwsWrapperError)
	assert.True(t, ok)
	assert.True(t, awsWrapperError.IsFailoverErrorType())
	assert.True(t, awsWrapperError.IsType(error_util.TransactionResolutionUnknownErrorType))

	newWriter, err := executeInstanceQueryReadOnly(env, conn, 5)
	assert.NoError(t, err)
	assert.NotEqual(t, originalWriterId, newWriter)
	assert.True(t, auroraTestUtility.IsDbInstanceWriter(newWriter, ""))

	_, err = conn.ExecContext(context.TODO(), "COMMIT")
	assert.NoError(t, err)
}
