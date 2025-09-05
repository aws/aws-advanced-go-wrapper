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

type testSetup struct {
	env               *test_utils.TestEnvironment
	auroraTestUtility *test_utils.AuroraTestUtility
	db                *sql.DB
	conn              *sql.Conn
}

func setupTest(t *testing.T, minInstances int) *testSetup {
	err := test_utils.BasicSetup(t.Name())
	require.NoError(t, err)

	env, err := test_utils.GetCurrentTestEnvironment()
	require.NoError(t, err)
	test_utils.SkipIfInsufficientInstances(t, env, minInstances)

	auroraTestUtility := test_utils.NewAuroraTestUtility(env.Info().Region)
	props := map[string]string{"plugins": "readWriteSplitting,efm,failover"}
	dsn := test_utils.GetDsn(env, props)

	db, err := test_utils.OpenDb(env.Info().Request.Engine, dsn)
	require.NoError(t, err)

	conn, err := db.Conn(context.TODO())
	require.NoError(t, err)

	return &testSetup{env, auroraTestUtility, db, conn}
}

func (s *testSetup) cleanup(t *testing.T) {
	if s.conn != nil {
		s.conn.Close()
	}
	if s.db != nil {
		s.db.Close()
	}
	test_utils.BasicCleanup(t.Name())
}

func setupProxiedTest(t *testing.T, minInstances int) *testSetup {
	utils.SetPreparedHostFunc(func(host string) string {
		if strings.HasSuffix(host, ".proxied") {
			return strings.TrimSuffix(host, ".proxied")
		}
		return host
	})
	t.Cleanup(utils.ResetPreparedHostFunc)

	err := test_utils.BasicSetup(t.Name())
	require.NoError(t, err)

	env, err := test_utils.GetCurrentTestEnvironment()
	require.NoError(t, err)
	test_utils.SkipIfInsufficientInstances(t, env, minInstances)

	return &testSetup{env: env, auroraTestUtility: test_utils.NewAuroraTestUtility(env.Info().Region)}
}

func executeInstanceQuery(env *test_utils.TestEnvironment, dbOrConn interface{}, ctx context.Context, timeout int) (string, error) {
	return test_utils.ExecuteInstanceQueryContextWithTimeout(
		env.Info().Request.Engine, env.Info().Request.Deployment, dbOrConn, timeout, ctx)
}

func executeInstanceQueryReadOnly(env *test_utils.TestEnvironment, dbOrConn interface{}, timeout int) (string, error) {
	return executeInstanceQuery(env, dbOrConn, readOnlyCtx, timeout)
}

func executeInstanceQueryWrite(env *test_utils.TestEnvironment, dbOrConn interface{}, timeout int) (string, error) {
	return executeInstanceQuery(env, dbOrConn, writeCtx, timeout)
}

func TestReadWriteSplitting_SetReadOnlyCtxTrue(t *testing.T) {
	setup := setupTest(t, 2)
	defer setup.cleanup(t)

	instanceId, err := executeInstanceQueryReadOnly(setup.env, setup.conn, 5)
	assert.NoError(t, err)
	assert.False(t, setup.auroraTestUtility.IsDbInstanceWriter(instanceId, ""))
}

func TestReadWriteSplitting_SetReadOnlyCtxFalse(t *testing.T) {
	setup := setupTest(t, 2)
	defer setup.cleanup(t)

	instanceId, err := executeInstanceQueryWrite(setup.env, setup.db, 5)
	assert.NoError(t, err)
	assert.True(t, setup.auroraTestUtility.IsDbInstanceWriter(instanceId, ""))
}

func TestReadWriteSplitting_SetReadOnlyCtxNoCtx(t *testing.T) {
	setup := setupTest(t, 2)
	defer setup.cleanup(t)

	instanceId, err := executeInstanceQuery(setup.env, setup.db, context.TODO(), 5)
	assert.NoError(t, err)
	assert.True(t, setup.auroraTestUtility.IsDbInstanceWriter(instanceId, ""))
}

func TestReadWriteSplitting_SetReadOnlyCtxSwitchFalseTrueNoCtx(t *testing.T) {
	setup := setupTest(t, 2)
	defer setup.cleanup(t)

	// Test no context (should be writer)
	instanceId, err := executeInstanceQuery(setup.env, setup.db, context.TODO(), 5)
	assert.NoError(t, err)
	assert.True(t, setup.auroraTestUtility.IsDbInstanceWriter(instanceId, ""))

	// Test switching to read-only mode
	instanceId, err = executeInstanceQueryReadOnly(setup.env, setup.db, 5)
	assert.NoError(t, err)
	assert.False(t, setup.auroraTestUtility.IsDbInstanceWriter(instanceId, ""))

	// Test switching back to write mode
	instanceId, err = executeInstanceQueryWrite(setup.env, setup.db, 5)
	assert.NoError(t, err)
	assert.True(t, setup.auroraTestUtility.IsDbInstanceWriter(instanceId, ""))
}

func TestReadWriteSplitting_SetReadOnlyFalseInReadOnlyTransaction(t *testing.T) {
	setup := setupTest(t, 2)
	defer setup.cleanup(t)

	// Start read-only transaction
	_, err := setup.conn.ExecContext(context.TODO(), "START TRANSACTION READ ONLY")
	require.NoError(t, err)

	// Test query that we are still connected to writer
	instanceId, err := executeInstanceQueryWrite(setup.env, setup.conn, 5)
	assert.NoError(t, err)
	assert.True(t, setup.auroraTestUtility.IsDbInstanceWriter(instanceId, ""))
}

func TestReadWriteSplitting_SetReadOnlyTrueInTransaction(t *testing.T) {
	setup := setupTest(t, 2)
	defer setup.cleanup(t)

	// Start read-only transaction
	_, err := setup.conn.ExecContext(readOnlyCtx, "START TRANSACTION READ ONLY")
	require.NoError(t, err)

	// Test that we cannot switch to read only true while in transaction
	_, err = executeInstanceQueryWrite(setup.env, setup.conn, 5)
	assert.Error(t, err)
	assert.Equal(t, error_util.GetMessage("ReadWriteSplittingPlugin.setReadOnlyFalseInTransaction"), err.Error())
}

func TestReadWriteSplitting_ExecuteWithCachedConnection(t *testing.T) {
	setup := setupTest(t, 2)
	defer setup.cleanup(t)

	// Execute query with cached connection
	firstReaderId, err := executeInstanceQueryReadOnly(setup.env, setup.conn, 5)
	assert.NoError(t, err)
	assert.False(t, setup.auroraTestUtility.IsDbInstanceWriter(firstReaderId, ""))

	// Execute with setReadOnly false
	instanceId, err := executeInstanceQueryWrite(setup.env, setup.conn, 5)
	assert.NoError(t, err)
	assert.True(t, setup.auroraTestUtility.IsDbInstanceWriter(instanceId, ""))
	assert.NotEqual(t, firstReaderId, instanceId)

	// Execute with setReadOnly true, verify reader is the same as original
	instanceId, err = executeInstanceQueryReadOnly(setup.env, setup.conn, 5)
	assert.NoError(t, err)
	assert.False(t, setup.auroraTestUtility.IsDbInstanceWriter(instanceId, ""))
	assert.Equal(t, firstReaderId, instanceId)

	// Repeat one more time
	instanceId, err = executeInstanceQueryWrite(setup.env, setup.conn, 5)
	assert.NoError(t, err)
	assert.True(t, setup.auroraTestUtility.IsDbInstanceWriter(instanceId, ""))
	assert.NotEqual(t, firstReaderId, instanceId)

	instanceId, err = executeInstanceQueryReadOnly(setup.env, setup.conn, 5)
	assert.NoError(t, err)
	assert.False(t, setup.auroraTestUtility.IsDbInstanceWriter(instanceId, ""))
	assert.Equal(t, firstReaderId, instanceId)
}

func TestReadWriteSplitting_OneInstance(t *testing.T) {
	setup := setupTest(t, 1)
	defer setup.cleanup(t)

	if setup.env.Info().Request.InstanceCount != 1 {
		t.Skipf("Skipping integration test %s, instanceCount = %v.", t.Name(), setup.env.Info().Request.InstanceCount)
	}

	readerInstance, err := executeInstanceQueryReadOnly(setup.env, setup.conn, 5)
	assert.NoError(t, err)
	writerInstance, err := executeInstanceQueryWrite(setup.env, setup.conn, 5)
	assert.NoError(t, err)
	assert.Equal(t, readerInstance, writerInstance)
}

func TestReadWriteSplitting_NoReaders(t *testing.T) {
	setup := setupProxiedTest(t, 3)
	defer setup.cleanup(t)

	dsn := test_utils.GetDsn(setup.env, test_utils.GetPropsForProxy(setup.env, setup.env.Info().ProxyDatabaseInfo.ClusterEndpoint, "readWriteSplitting,efm,failover", 2))
	db, err := test_utils.OpenDb(setup.env.Info().Request.Engine, dsn)
	require.NoError(t, err)
	defer db.Close()

	conn, err := db.Conn(context.TODO())
	require.NoError(t, err)
	defer conn.Close()

	// Get writer and disable all readers
	writerId, err := executeInstanceQueryWrite(setup.env, conn, 5)
	require.NoError(t, err)
	assert.True(t, setup.auroraTestUtility.IsDbInstanceWriter(writerId, ""))

	for _, instance := range setup.env.Info().DatabaseInfo.Instances {
		if writerId != instance.InstanceId() {
			test_utils.DisableProxyConnectivity(setup.env.ProxyInfos()[instance.InstanceId()])
		}
	}

	// Test switching to read-only mode (should fallback to writer)
	readerId, err := executeInstanceQueryReadOnly(setup.env, conn, 60)
	require.NoError(t, err)
	assert.True(t, setup.auroraTestUtility.IsDbInstanceWriter(readerId, ""))
}

func TestReadWriteSplitting_WriterFailover(t *testing.T) {
	setup := setupProxiedTest(t, 3)
	defer setup.cleanup(t)

	dsn := test_utils.GetDsn(setup.env, test_utils.GetPropsForProxy(setup.env, setup.env.Info().ProxyDatabaseInfo.ClusterEndpoint, "readWriteSplitting,efm,failover", 5))
	db, err := test_utils.OpenDb(setup.env.Info().Request.Engine, dsn)
	require.NoError(t, err)
	defer db.Close()

	conn, err := db.Conn(context.TODO())
	require.NoError(t, err)
	defer conn.Close()

	originalWriterId, err := executeInstanceQueryWrite(setup.env, conn, 5)
	require.NoError(t, err)
	assert.True(t, setup.auroraTestUtility.IsDbInstanceWriter(originalWriterId, ""))

	// Disable all readers
	for _, instance := range setup.env.Info().DatabaseInfo.Instances {
		if originalWriterId != instance.InstanceId() {
			test_utils.DisableProxyConnectivity(setup.env.ProxyInfos()[instance.InstanceId()])
		}
	}

	// Verify fallback to writer for reads
	instanceId, err := executeInstanceQueryReadOnly(setup.env, conn, 60)
	require.NoError(t, err)
	assert.Equal(t, originalWriterId, instanceId)

	// Perform failover
	test_utils.EnableAllConnectivity(true)
	err = setup.auroraTestUtility.FailoverClusterAndWaitTillWriterChanged(originalWriterId, "", "")
	require.NoError(t, err)

	// Expect failover error
	_, err = executeInstanceQueryWrite(setup.env, conn, 60)
	assert.Error(t, err)
	awsWrapperError, ok := err.(*error_util.AwsWrapperError)
	assert.True(t, ok)
	assert.True(t, awsWrapperError.IsFailoverErrorType())

	// Verify new writer
	instanceId, err = executeInstanceQueryWrite(setup.env, conn, 60)
	require.NoError(t, err)
	assert.True(t, setup.auroraTestUtility.IsDbInstanceWriter(instanceId, ""))

	// Verify reader connection
	instanceId, err = executeInstanceQueryReadOnly(setup.env, conn, 60)
	require.NoError(t, err)
	assert.False(t, setup.auroraTestUtility.IsDbInstanceWriter(instanceId, ""))
}

func TestReadWriteSplitting_FailoverToNewReader(t *testing.T) {
	setup := setupProxiedTest(t, 3)
	defer setup.cleanup(t)

	props := test_utils.GetPropsForProxy(setup.env, setup.env.Info().ProxyDatabaseInfo.ClusterEndpoint, "readWriteSplitting,efm,failover", 5)
	property_util.FAILOVER_MODE.Set(props, "reader-or-writer")
	property_util.CLUSTER_TOPOLOGY_REFRESH_RATE_MS.Set(props, "5000")
	dsn := test_utils.GetDsn(setup.env, props)

	db, err := test_utils.OpenDb(setup.env.Info().Request.Engine, dsn)
	require.NoError(t, err)
	defer db.Close()

	conn, err := db.Conn(context.TODO())
	require.NoError(t, err)
	defer conn.Close()

	originalWriterId, err := executeInstanceQueryWrite(setup.env, conn, 60)
	require.NoError(t, err)
	slog.Info("originalWriterId", "writerId", originalWriterId)

	originalReaderId, err := executeInstanceQueryReadOnly(setup.env, conn, 60)
	require.NoError(t, err)

	// Find another reader
	var otherReaderId string
	for i := 1; i < len(setup.env.Info().DatabaseInfo.Instances); i++ {
		if setup.env.Info().DatabaseInfo.Instances[i].InstanceId() != originalReaderId {
			otherReaderId = setup.env.Info().DatabaseInfo.Instances[i].InstanceId()
			break
		}
	}
	require.NotEmpty(t, otherReaderId)

	// Kill all instances except the other reader
	for _, instance := range setup.env.Info().DatabaseInfo.Instances {
		if instance.InstanceId() != otherReaderId {
			test_utils.DisableProxyConnectivity(setup.env.ProxyInfos()[instance.InstanceId()])
		}
	}

	// Expect failover to new reader
	_, err = executeInstanceQueryReadOnly(setup.env, conn, 60)
	assert.Error(t, err)
	awsWrapperError, ok := err.(*error_util.AwsWrapperError)
	assert.True(t, ok)
	assert.True(t, awsWrapperError.IsFailoverErrorType())

	currentReaderId, err := executeInstanceQueryReadOnly(setup.env, conn, 10)
	require.NoError(t, err)
	assert.Equal(t, otherReaderId, currentReaderId)
	assert.NotEqual(t, originalReaderId, currentReaderId)

	// Restore connectivity and verify
	test_utils.EnableAllConnectivity(true)
	time.Sleep(20 * time.Second)

	currentId, err := executeInstanceQueryWrite(setup.env, conn, 10)
	require.NoError(t, err)
	assert.Equal(t, originalWriterId, currentId)

	currentId, err = executeInstanceQueryReadOnly(setup.env, conn, 10)
	require.NoError(t, err)
	assert.Equal(t, otherReaderId, currentId)
}

func TestReadWriteSplitting_FailoverReaderToWriter(t *testing.T) {
	setup := setupProxiedTest(t, 3)
	defer setup.cleanup(t)

	props := test_utils.GetPropsForProxy(setup.env, setup.env.Info().ProxyDatabaseInfo.ClusterEndpoint, "readWriteSplitting,efm,failover", 2)
	property_util.CLUSTER_TOPOLOGY_REFRESH_RATE_MS.Set(props, "5000")
	dsn := test_utils.GetDsn(setup.env, props)

	db, err := test_utils.OpenDb(setup.env.Info().Request.Engine, dsn)
	require.NoError(t, err)
	defer db.Close()

	conn, err := db.Conn(context.TODO())
	require.NoError(t, err)
	defer conn.Close()

	originalWriterId, err := executeInstanceQueryWrite(setup.env, conn, 5)
	require.NoError(t, err)

	readerId, err := executeInstanceQueryReadOnly(setup.env, conn, 60)
	require.NoError(t, err)
	assert.NotEqual(t, originalWriterId, readerId)

	// Kill all readers
	for _, instance := range setup.env.Info().DatabaseInfo.Instances {
		if originalWriterId != instance.InstanceId() {
			test_utils.DisableProxyConnectivity(setup.env.ProxyInfos()[instance.InstanceId()])
		}
	}

	time.Sleep(5 * time.Second)

	// Expect failover to writer for reads
	_, err = executeInstanceQueryReadOnly(setup.env, conn, 60)
	assert.Error(t, err)
	awsWrapperError, ok := err.(*error_util.AwsWrapperError)
	assert.True(t, ok)
	assert.True(t, awsWrapperError.IsFailoverErrorType())

	instanceId, err := executeInstanceQueryReadOnly(setup.env, conn, 60)
	assert.NoError(t, err)
	assert.Equal(t, originalWriterId, instanceId)

	// Restore connectivity
	test_utils.EnableAllConnectivity(true)
	plugin_helpers.ClearCaches()
	time.Sleep(40 * time.Second)

	instanceId, err = executeInstanceQueryWrite(setup.env, conn, 60)
	assert.NoError(t, err)
	assert.Equal(t, originalWriterId, instanceId)

	instanceId, err = executeInstanceQueryReadOnly(setup.env, conn, 60)
	assert.NoError(t, err)
	assert.NotEqual(t, originalWriterId, instanceId)
}

func setInternalPoolProvider(poolOptions ...internal_pool.InternalPoolOption) *internal_pool.InternalPooledConnectionProvider {
	options := internal_pool.NewInternalPoolOptions(poolOptions...)
	provider := internal_pool.NewInternalPooledConnectionProvider(options, 0)
	driver_infrastructure.SetCustomConnectionProvider(provider)
	return provider
}

func TestPooledConnection_Failover(t *testing.T) {
	setup := setupProxiedTest(t, 3)
	defer setup.cleanup(t)

	provider := setInternalPoolProvider()
	defer driver_infrastructure.ResetCustomConnectionProvider()
	defer provider.ReleaseResources()

	dsn := test_utils.GetDsn(setup.env, test_utils.GetPropsForProxy(setup.env, setup.env.Info().ProxyDatabaseInfo.ClusterEndpoint, "readWriteSplitting,efm,failover", 4))
	db, err := test_utils.OpenDb(setup.env.Info().Request.Engine, dsn)
	require.NoError(t, err)
	defer db.Close()

	conn, err := db.Conn(context.TODO())
	require.NoError(t, err)
	defer conn.Close()

	originalWriterId, err := executeInstanceQueryWrite(setup.env, conn, 5)
	require.NoError(t, err)
	assert.True(t, setup.auroraTestUtility.IsDbInstanceWriter(originalWriterId, ""))

	test_utils.DisableAllConnectivity()
	time.Sleep(5 * time.Second)

	_, err = executeInstanceQueryReadOnly(setup.env, conn, 5)
	require.Error(t, err)
	awsWrapperError, ok := err.(*error_util.AwsWrapperError)
	assert.True(t, ok)
	assert.True(t, awsWrapperError.IsFailoverErrorType())

	test_utils.EnableAllConnectivity(true)
	err = test_utils.VerifyClusterStatus()
	require.NoError(t, err)

	conn2, err := db.Conn(context.TODO())
	require.NoError(t, err)
	defer conn2.Close()

	newWriterId, err := executeInstanceQueryWrite(setup.env, conn2, 2)
	require.NoError(t, err)
	assert.Equal(t, originalWriterId, newWriterId)
}

func TestPooledConnection_FailoverInTransaction(t *testing.T) {
	setup := setupProxiedTest(t, 3)
	defer setup.cleanup(t)

	setInternalPoolProvider()
	defer driver_infrastructure.ResetCustomConnectionProvider()

	dsn := test_utils.GetDsn(setup.env, test_utils.GetPropsForProxy(setup.env, setup.env.Info().ProxyDatabaseInfo.ClusterEndpoint, "readWriteSplitting,efm,failover", 5))
	db, err := test_utils.OpenDb(setup.env.Info().Request.Engine, dsn)
	require.NoError(t, err)
	defer db.Close()

	conn, err := db.Conn(context.TODO())
	require.NoError(t, err)
	defer conn.Close()

	originalWriterId, err := executeInstanceQueryWrite(setup.env, conn, 5)
	require.NoError(t, err)
	assert.True(t, setup.auroraTestUtility.IsDbInstanceWriter(originalWriterId, ""))

	_, err = conn.ExecContext(context.TODO(), "START TRANSACTION READ ONLY")
	require.NoError(t, err)

	_, err = executeInstanceQueryReadOnly(setup.env, conn, 5)
	require.NoError(t, err)

	err = setup.auroraTestUtility.FailoverClusterAndWaitTillWriterChanged(originalWriterId, "", "")
	require.NoError(t, err)

	_, err = executeInstanceQueryReadOnly(setup.env, conn, 60)
	assert.Error(t, err)
	awsWrapperError, ok := err.(*error_util.AwsWrapperError)
	assert.True(t, ok)
	assert.True(t, awsWrapperError.IsFailoverErrorType())

	newWriter, err := executeInstanceQueryReadOnly(setup.env, conn, 5)
	assert.NoError(t, err)
	assert.NotEqual(t, originalWriterId, newWriter)
	assert.True(t, setup.auroraTestUtility.IsDbInstanceWriter(newWriter, ""))

	_, err = conn.ExecContext(context.TODO(), "COMMIT")
	assert.NoError(t, err)
}
