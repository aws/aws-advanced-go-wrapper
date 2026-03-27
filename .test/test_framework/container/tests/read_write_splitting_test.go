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

type rwsTestSetup struct {
	env               *test_utils.TestEnvironment
	auroraTestUtility *test_utils.AuroraTestUtility
	db                *sql.DB
	conn              *sql.Conn
}

func rwsSetupTest(t *testing.T, cfg readWriteSplittingTestConfig, minInstances int) *rwsTestSetup {
	err := test_utils.BasicSetup(t.Name())
	require.NoError(t, err)

	env, err := test_utils.GetCurrentTestEnvironment()
	require.NoError(t, err)
	test_utils.SkipForTestEnvironmentFeatures(t, env.Info().Request.Features, test_utils.LIMITLESS_DEPLOYMENT)
	test_utils.SkipIfInsufficientInstances(t, env, minInstances)

	auroraTestUtility := test_utils.NewAuroraTestUtility(env.Info())
	props := cfg.setupFn(t, env)
	dsn := test_utils.GetDsn(env, props)

	db, err := test_utils.OpenDb(env.Info().Request.Engine, dsn)
	require.NoError(t, err)

	conn, err := db.Conn(context.TODO())
	require.NoError(t, err)

	return &rwsTestSetup{env, auroraTestUtility, db, conn}
}

func (s *rwsTestSetup) cleanup(t *testing.T) {
	if s.conn != nil {
		_ = s.conn.Close()
	}
	if s.db != nil {
		_ = s.db.Close()
	}
	test_utils.BasicCleanup(t.Name())
}

func rwsSetupProxiedTest(t *testing.T, minInstances int) *rwsTestSetup {
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
	test_utils.SkipForTestEnvironmentFeatures(t, env.Info().Request.Features, test_utils.LIMITLESS_DEPLOYMENT)
	test_utils.SkipIfInsufficientInstances(t, env, minInstances)

	return &rwsTestSetup{env: env, auroraTestUtility: test_utils.NewAuroraTestUtility(env.Info())}
}

func executeInstanceQuery(env *test_utils.TestEnvironment, rowQuerier test_utils.RowQuerier, ctx context.Context, timeout int) (string, error) {
	return test_utils.ExecuteInstanceQueryContextWithTimeout(
		env.Info().Request.Engine, env.Info().Request.Deployment, rowQuerier, timeout, ctx)
}

func executeInstanceQueryReadOnly(env *test_utils.TestEnvironment, rowQuerier test_utils.RowQuerier, timeout int) (string, error) {
	return executeInstanceQuery(env, rowQuerier, readOnlyCtx, timeout)
}

func executeInstanceQueryWrite(env *test_utils.TestEnvironment, rowQuerier test_utils.RowQuerier, timeout int) (string, error) {
	return executeInstanceQuery(env, rowQuerier, writeCtx, timeout)
}

func TestReadWriteSplittingDB(t *testing.T) {
	for _, cfg := range readWriteSplittingConfigs {
		cfg := cfg // capture range variable
		t.Run(cfg.name+"/SetReadOnlyCtxTrue", func(t *testing.T) {
			rwsSetReadOnlyCtxTrueDBTest(t, cfg)
		})
		t.Run(cfg.name+"/SetReadOnlyCtxFalse", func(t *testing.T) {
			rwsSetReadOnlyCtxFalseDBTest(t, cfg)
		})
		t.Run(cfg.name+"/SetReadOnlyCtxNoCtx", func(t *testing.T) {
			rwsSetReadOnlyCtxNoCtxDBTest(t, cfg)
		})
		t.Run(cfg.name+"/TxSetReadOnlyTrueCtx", func(t *testing.T) {
			rwsTxSetReadOnlyTrueCtxDBTest(t, cfg)
		})
		t.Run(cfg.name+"/TxSetReadOnlyFalseCtx", func(t *testing.T) {
			rwsTxSetReadOnlyFalseCtxDBTest(t, cfg)
		})
		t.Run(cfg.name+"/TxSetReadOnlyNoCtx", func(t *testing.T) {
			rwsTxSetReadOnlyNoCtxDBTest(t, cfg)
		})
		t.Run(cfg.name+"/TxSetReadOnlyTrueOpt", func(t *testing.T) {
			rwsTxSetReadOnlyTrueOptDBTest(t, cfg)
		})
		t.Run(cfg.name+"/TxSetReadOnlyFalseOpt", func(t *testing.T) {
			rwsTxSetReadOnlyFalseOptDBTest(t, cfg)
		})
		t.Run(cfg.name+"/TxNoOpt", func(t *testing.T) {
			rwsTxNoOptDBTest(t, cfg)
		})
		t.Run(cfg.name+"/StmtDb", func(t *testing.T) {
			rwsStmtDbDBTest(t, cfg)
		})
		t.Run(cfg.name+"/StmtConn", func(t *testing.T) {
			rwsStmtConnDBTest(t, cfg)
		})
		t.Run(cfg.name+"/StmtTx", func(t *testing.T) {
			rwsStmtTxDBTest(t, cfg)
		})
		t.Run(cfg.name+"/SetReadOnlyCtxSwitchFalseTrueNoCtx", func(t *testing.T) {
			rwsSetReadOnlyCtxSwitchFalseTrueNoCtxDBTest(t, cfg)
		})
		t.Run(cfg.name+"/SetReadOnlyFalseInReadOnlyTransaction", func(t *testing.T) {
			rwsSetReadOnlyFalseInReadOnlyTransactionDBTest(t, cfg)
		})
		t.Run(cfg.name+"/SetReadOnlyTrueInTransaction", func(t *testing.T) {
			rwsSetReadOnlyTrueInTransactionDBTest(t, cfg)
		})
		t.Run(cfg.name+"/ExecuteWithCachedConnection", func(t *testing.T) {
			rwsExecuteWithCachedConnectionDBTest(t, cfg)
		})
		t.Run(cfg.name+"/OneInstance", func(t *testing.T) {
			rwsOneInstanceDBTest(t, cfg)
		})
	}
}

func TestReadWriteSplittingConn(t *testing.T) {
	for _, cfg := range readWriteSplittingConfigs {
		cfg := cfg // capture range variable
		t.Run(cfg.name+"/NoReaders", func(t *testing.T) {
			rwsNoReadersConnTest(t, cfg)
		})
		t.Run(cfg.name+"/WriterFailover", func(t *testing.T) {
			rwsWriterFailoverConnTest(t, cfg)
		})
		t.Run(cfg.name+"/FailoverToNewReader", func(t *testing.T) {
			rwsFailoverToNewReaderConnTest(t, cfg)
		})
		t.Run(cfg.name+"/FailoverReaderToWriter", func(t *testing.T) {
			rwsFailoverReaderToWriterConnTest(t, cfg)
		})
		t.Run(cfg.name+"/PooledConnectionFailover", func(t *testing.T) {
			rwsPooledConnectionFailoverConnTest(t, cfg)
		})
		t.Run(cfg.name+"/PooledConnectionFailoverInTransaction", func(t *testing.T) {
			rwsPooledConnectionFailoverInTransactionConnTest(t, cfg)
		})
	}
}

func rwsSetReadOnlyCtxTrueDBTest(t *testing.T, cfg readWriteSplittingTestConfig) {
	setup := rwsSetupTest(t, cfg, 2)
	defer setup.cleanup(t)

	instanceId, err := executeInstanceQueryReadOnly(setup.env, setup.conn, 5)
	assert.NoError(t, err)
	assert.False(t, setup.auroraTestUtility.IsDbInstanceWriter(instanceId, ""))

	// Verify that subsequent queries without context has expected results
	instanceId, err = executeInstanceQuery(setup.env, setup.conn, context.TODO(), 5)
	assert.NoError(t, err)
	assert.False(t, setup.auroraTestUtility.IsDbInstanceWriter(instanceId, ""))
}

func rwsSetReadOnlyCtxFalseDBTest(t *testing.T, cfg readWriteSplittingTestConfig) {
	setup := rwsSetupTest(t, cfg, 2)
	defer setup.cleanup(t)

	instanceId, err := executeInstanceQueryWrite(setup.env, setup.db, 5)
	assert.NoError(t, err)
	assert.True(t, setup.auroraTestUtility.IsDbInstanceWriter(instanceId, ""))

	// Verify that subsequent queries without context has expected results
	instanceId, err = executeInstanceQuery(setup.env, setup.conn, context.TODO(), 5)
	assert.NoError(t, err)
	assert.True(t, setup.auroraTestUtility.IsDbInstanceWriter(instanceId, ""))
}

func rwsSetReadOnlyCtxNoCtxDBTest(t *testing.T, cfg readWriteSplittingTestConfig) {
	setup := rwsSetupTest(t, cfg, 2)
	defer setup.cleanup(t)

	instanceId, err := executeInstanceQuery(setup.env, setup.db, context.TODO(), 5)
	assert.NoError(t, err)
	assert.True(t, setup.auroraTestUtility.IsDbInstanceWriter(instanceId, ""))

	// Verify that subsequent queries without context has expected results
	instanceId, err = executeInstanceQuery(setup.env, setup.conn, context.TODO(), 5)
	assert.NoError(t, err)
	assert.True(t, setup.auroraTestUtility.IsDbInstanceWriter(instanceId, ""))
}

func rwsTxSetReadOnlyTrueCtxDBTest(t *testing.T, cfg readWriteSplittingTestConfig) {
	setup := rwsSetupTest(t, cfg, 2)
	defer setup.cleanup(t)

	tx, err := setup.db.BeginTx(readOnlyCtx, nil)
	require.NoError(t, err)

	instanceId, err := executeInstanceQuery(setup.env, tx, context.TODO(), 5)
	assert.NoError(t, err)
	err = tx.Commit()
	assert.NoError(t, err)
	assert.False(t, setup.auroraTestUtility.IsDbInstanceWriter(instanceId, ""))
}

func rwsTxSetReadOnlyFalseCtxDBTest(t *testing.T, cfg readWriteSplittingTestConfig) {
	setup := rwsSetupTest(t, cfg, 2)
	defer setup.cleanup(t)

	tx, err := setup.db.BeginTx(writeCtx, nil)
	require.NoError(t, err)

	instanceId, err := executeInstanceQuery(setup.env, tx, context.TODO(), 5)
	assert.NoError(t, err)
	err = tx.Commit()
	assert.NoError(t, err)
	assert.True(t, setup.auroraTestUtility.IsDbInstanceWriter(instanceId, ""))
}

func rwsTxSetReadOnlyNoCtxDBTest(t *testing.T, cfg readWriteSplittingTestConfig) {
	setup := rwsSetupTest(t, cfg, 2)
	defer setup.cleanup(t)

	tx, err := setup.db.BeginTx(context.TODO(), nil)
	require.NoError(t, err)

	instanceId, err := executeInstanceQuery(setup.env, tx, context.TODO(), 5)
	assert.NoError(t, err)
	err = tx.Commit()
	assert.NoError(t, err)
	assert.True(t, setup.auroraTestUtility.IsDbInstanceWriter(instanceId, ""))
}

func rwsTxSetReadOnlyTrueOptDBTest(t *testing.T, cfg readWriteSplittingTestConfig) {
	setup := rwsSetupTest(t, cfg, 2)
	defer setup.cleanup(t)

	tx, err := setup.db.BeginTx(context.TODO(), &sql.TxOptions{ReadOnly: true})
	require.NoError(t, err)

	instanceId, err := executeInstanceQuery(setup.env, tx, context.TODO(), 5)
	assert.NoError(t, err)
	err = tx.Commit()
	assert.NoError(t, err)
	assert.False(t, setup.auroraTestUtility.IsDbInstanceWriter(instanceId, ""))
}

func rwsTxSetReadOnlyFalseOptDBTest(t *testing.T, cfg readWriteSplittingTestConfig) {
	setup := rwsSetupTest(t, cfg, 2)
	defer setup.cleanup(t)

	tx, err := setup.db.BeginTx(context.TODO(), &sql.TxOptions{ReadOnly: false})
	require.NoError(t, err)

	instanceId, err := executeInstanceQuery(setup.env, tx, context.TODO(), 5)
	assert.NoError(t, err)
	err = tx.Commit()
	assert.NoError(t, err)
	assert.True(t, setup.auroraTestUtility.IsDbInstanceWriter(instanceId, ""))
}

func rwsTxNoOptDBTest(t *testing.T, cfg readWriteSplittingTestConfig) {
	setup := rwsSetupTest(t, cfg, 2)
	defer setup.cleanup(t)

	tx, err := setup.db.BeginTx(context.TODO(), &sql.TxOptions{ReadOnly: false})
	require.NoError(t, err)

	instanceId, err := executeInstanceQuery(setup.env, tx, context.TODO(), 5)
	assert.NoError(t, err)
	err = tx.Commit()
	assert.NoError(t, err)
	assert.True(t, setup.auroraTestUtility.IsDbInstanceWriter(instanceId, ""))
}

func rwsStmtDbDBTest(t *testing.T, cfg readWriteSplittingTestConfig) {
	setup := rwsSetupTest(t, cfg, 2)
	defer setup.cleanup(t)

	instanceQuery, err := test_utils.GetInstanceIdSql(
		setup.env.Info().Request.Engine,
		setup.env.Info().Request.Deployment)

	require.NoError(t, err)

	var instanceId string

	stmt, err := setup.db.PrepareContext(readOnlyCtx, instanceQuery)
	require.NoError(t, err)
	err = stmt.QueryRowContext(context.TODO()).Scan(&instanceId)
	assert.NoError(t, err)
	assert.False(t, setup.auroraTestUtility.IsDbInstanceWriter(instanceId, ""))
	assert.NoError(t, stmt.Close())

	stmt, err = setup.db.PrepareContext(writeCtx, instanceQuery)
	require.NoError(t, err)
	err = stmt.QueryRowContext(context.TODO()).Scan(&instanceId)
	assert.NoError(t, err)
	assert.True(t, setup.auroraTestUtility.IsDbInstanceWriter(instanceId, ""))

	assert.NoError(t, stmt.Close())
}

func rwsStmtConnDBTest(t *testing.T, cfg readWriteSplittingTestConfig) {
	setup := rwsSetupTest(t, cfg, 2)
	defer setup.cleanup(t)

	instanceQuery, err := test_utils.GetInstanceIdSql(
		setup.env.Info().Request.Engine,
		setup.env.Info().Request.Deployment)

	require.NoError(t, err)

	var instanceId string

	// Test Read/Write from stmt level
	conn, err := setup.db.Conn(context.TODO())
	require.NoError(t, err)

	stmt, err := conn.PrepareContext(readOnlyCtx, instanceQuery)
	require.NoError(t, err)
	err = stmt.QueryRowContext(context.TODO()).Scan(&instanceId)

	assert.NoError(t, err)
	assert.False(t, setup.auroraTestUtility.IsDbInstanceWriter(instanceId, ""))
	assert.NoError(t, stmt.Close())

	// Assert that Conn has switched to reader after statement
	instanceId, err = executeInstanceQuery(setup.env, conn, context.TODO(), 5)
	require.NoError(t, err)
	assert.False(t, setup.auroraTestUtility.IsDbInstanceWriter(instanceId, ""))

	stmt, err = conn.PrepareContext(writeCtx, instanceQuery)
	require.NoError(t, err)
	err = stmt.QueryRowContext(context.TODO()).Scan(&instanceId)
	assert.NoError(t, err)
	assert.True(t, setup.auroraTestUtility.IsDbInstanceWriter(instanceId, ""))

	assert.NoError(t, stmt.Close())

	// Assert that Conn has switched to writer after statement
	instanceId, err = executeInstanceQuery(setup.env, conn, context.TODO(), 5)
	require.NoError(t, err)
	assert.True(t, setup.auroraTestUtility.IsDbInstanceWriter(instanceId, ""))
	_ = conn.Close()

	// Test Read/Write from Conn level
	conn, err = setup.db.Conn(context.TODO())
	require.NoError(t, err)

	// switch conn to reader
	readerId, err := executeInstanceQueryReadOnly(setup.env, conn, 5)
	assert.NoError(t, err)
	assert.False(t, setup.auroraTestUtility.IsDbInstanceWriter(readerId, ""))

	stmt, err = conn.PrepareContext(context.TODO(), instanceQuery)
	require.NoError(t, err)
	err = stmt.QueryRowContext(context.TODO()).Scan(&instanceId)
	assert.NoError(t, err)
	assert.Equal(t, readerId, instanceId)
	assert.NoError(t, stmt.Close())

	// Assert that Conn is still connected to the reader
	instanceId, err = executeInstanceQuery(setup.env, conn, context.TODO(), 5)
	require.NoError(t, err)
	assert.Equal(t, readerId, instanceId)

	// switch conn to writer
	writerId, err := executeInstanceQueryWrite(setup.env, conn, 5)
	assert.NoError(t, err)
	assert.True(t, setup.auroraTestUtility.IsDbInstanceWriter(writerId, ""))

	stmt, err = conn.PrepareContext(context.TODO(), instanceQuery)
	require.NoError(t, err)
	err = stmt.QueryRowContext(context.TODO()).Scan(&instanceId)
	assert.NoError(t, err)
	assert.Equal(t, writerId, instanceId)

	assert.NoError(t, stmt.Close())
	// Assert that Conn is still connected to the writer
	instanceId, err = executeInstanceQuery(setup.env, conn, context.TODO(), 5)
	require.NoError(t, err)
	assert.Equal(t, writerId, instanceId)
	_ = conn.Close()

	// Test Read/Write from conn level, but switch when creating statement
	conn, err = setup.db.Conn(context.TODO())
	require.NoError(t, err)

	// switch conn to reader
	readerId, err = executeInstanceQueryReadOnly(setup.env, conn, 5)
	assert.NoError(t, err)
	assert.False(t, setup.auroraTestUtility.IsDbInstanceWriter(readerId, ""))

	// switch to writer
	stmt, err = conn.PrepareContext(writeCtx, instanceQuery)
	require.NoError(t, err)
	err = stmt.QueryRowContext(context.TODO()).Scan(&instanceId)
	assert.NoError(t, err)
	assert.NotEqual(t, readerId, instanceId)
	assert.True(t, setup.auroraTestUtility.IsDbInstanceWriter(instanceId, ""))
	assert.NoError(t, stmt.Close())

	// Assert that Conn is still connected to the writer
	instanceId, err = executeInstanceQuery(setup.env, conn, context.TODO(), 5)
	require.NoError(t, err)
	assert.True(t, setup.auroraTestUtility.IsDbInstanceWriter(instanceId, ""))

	// switch conn to writer
	writerId, err = executeInstanceQueryWrite(setup.env, conn, 5)
	assert.NoError(t, err)
	assert.True(t, setup.auroraTestUtility.IsDbInstanceWriter(writerId, ""))

	// Switch to reader
	stmt, err = conn.PrepareContext(readOnlyCtx, instanceQuery)
	require.NoError(t, err)
	err = stmt.QueryRowContext(context.TODO()).Scan(&instanceId)
	assert.NoError(t, err)
	assert.NotEqual(t, writerId, instanceId)
	assert.False(t, setup.auroraTestUtility.IsDbInstanceWriter(instanceId, ""))

	assert.NoError(t, stmt.Close())

	// Assert that Conn is still connected to the reader
	instanceId, err = executeInstanceQuery(setup.env, conn, context.TODO(), 5)
	require.NoError(t, err)
	assert.False(t, setup.auroraTestUtility.IsDbInstanceWriter(instanceId, ""))
	assert.NoError(t, conn.Close())
}

func rwsStmtTxDBTest(t *testing.T, cfg readWriteSplittingTestConfig) {
	setup := rwsSetupTest(t, cfg, 2)
	defer setup.cleanup(t)

	instanceQuery, err := test_utils.GetInstanceIdSql(
		setup.env.Info().Request.Engine,
		setup.env.Info().Request.Deployment)

	require.NoError(t, err)

	var instanceId string

	// Test Read Only True through TxOptions
	tx, err := setup.db.BeginTx(context.TODO(), &sql.TxOptions{ReadOnly: true})
	require.NoError(t, err)
	stmt, err := tx.PrepareContext(context.TODO(), instanceQuery)
	require.NoError(t, err)
	err = stmt.QueryRowContext(context.TODO()).Scan(&instanceId)
	assert.NoError(t, err)
	assert.False(t, setup.auroraTestUtility.IsDbInstanceWriter(instanceId, ""))
	assert.NoError(t, stmt.Close())
	err = tx.Commit()
	assert.NoError(t, err)

	// Test Read Only False through TxOptions
	tx, err = setup.db.BeginTx(context.TODO(), &sql.TxOptions{ReadOnly: false})
	require.NoError(t, err)
	stmt, err = tx.PrepareContext(context.TODO(), instanceQuery)
	require.NoError(t, err)
	err = stmt.QueryRowContext(context.TODO()).Scan(&instanceId)
	assert.NoError(t, err)
	assert.True(t, setup.auroraTestUtility.IsDbInstanceWriter(instanceId, ""))
	assert.NoError(t, stmt.Close())
	err = tx.Commit()
	assert.NoError(t, err)

	// Test Read Only True through context
	tx, err = setup.db.BeginTx(readOnlyCtx, nil)
	require.NoError(t, err)
	stmt, err = tx.PrepareContext(context.TODO(), instanceQuery)
	require.NoError(t, err)
	err = stmt.QueryRowContext(context.TODO()).Scan(&instanceId)
	assert.NoError(t, err)
	assert.False(t, setup.auroraTestUtility.IsDbInstanceWriter(instanceId, ""))
	assert.NoError(t, stmt.Close())
	err = tx.Commit()
	assert.NoError(t, err)

	// Test Read Only False through context
	tx, err = setup.db.BeginTx(writeCtx, nil)
	require.NoError(t, err)
	stmt, err = tx.PrepareContext(context.TODO(), instanceQuery)
	require.NoError(t, err)
	err = stmt.QueryRowContext(context.TODO()).Scan(&instanceId)
	assert.NoError(t, err)
	assert.True(t, setup.auroraTestUtility.IsDbInstanceWriter(instanceId, ""))
	assert.NoError(t, stmt.Close())
	err = tx.Commit()
	assert.NoError(t, err)
}

func rwsSetReadOnlyCtxSwitchFalseTrueNoCtxDBTest(t *testing.T, cfg readWriteSplittingTestConfig) {
	setup := rwsSetupTest(t, cfg, 2)
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

func rwsSetReadOnlyFalseInReadOnlyTransactionDBTest(t *testing.T, cfg readWriteSplittingTestConfig) {
	setup := rwsSetupTest(t, cfg, 2)
	defer setup.cleanup(t)

	// Start read-only transaction
	_, err := setup.conn.ExecContext(context.TODO(), "START TRANSACTION READ ONLY")
	require.NoError(t, err)

	// Test query that we are still connected to writer
	instanceId, err := executeInstanceQueryWrite(setup.env, setup.conn, 5)
	assert.NoError(t, err)
	assert.True(t, setup.auroraTestUtility.IsDbInstanceWriter(instanceId, ""))
}

func rwsSetReadOnlyTrueInTransactionDBTest(t *testing.T, cfg readWriteSplittingTestConfig) {
	setup := rwsSetupTest(t, cfg, 2)
	defer setup.cleanup(t)

	// Start read-only transaction
	_, err := setup.conn.ExecContext(readOnlyCtx, "START TRANSACTION READ ONLY")
	require.NoError(t, err)

	// Test that we cannot switch to read only true while in transaction
	_, err = executeInstanceQueryWrite(setup.env, setup.conn, 5)
	assert.Error(t, err)
	assert.Equal(t, error_util.GetMessage("ReadWriteSplittingPlugin.setReadOnlyFalseInTransaction"), err.Error())
}

func rwsExecuteWithCachedConnectionDBTest(t *testing.T, cfg readWriteSplittingTestConfig) {
	setup := rwsSetupTest(t, cfg, 2)
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

func rwsOneInstanceDBTest(t *testing.T, cfg readWriteSplittingTestConfig) {
	setup := rwsSetupTest(t, cfg, 1)
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

func rwsNoReadersConnTest(t *testing.T, cfg readWriteSplittingTestConfig) {
	setup := rwsSetupProxiedTest(t, 3)
	defer setup.cleanup(t)

	dsn := test_utils.GetDsn(setup.env,
		cfg.setupProxiedFn(t, setup.env, setup.env.Info().ProxyDatabaseInfo.ClusterEndpoint, 2))
	db, err := test_utils.OpenDb(setup.env.Info().Request.Engine, dsn)
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	conn, err := db.Conn(context.TODO())
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

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

func rwsWriterFailoverConnTest(t *testing.T, cfg readWriteSplittingTestConfig) {
	setup := rwsSetupProxiedTest(t, 3)
	defer setup.cleanup(t)

	props := cfg.setupProxiedFn(t, setup.env, setup.env.Info().ProxyDatabaseInfo.ClusterEndpoint, 2)
	props[property_util.CLUSTER_TOPOLOGY_REFRESH_RATE_MS.Name] = "3000"
	dsn := test_utils.GetDsn(setup.env, props)
	db, err := test_utils.OpenDb(setup.env.Info().Request.Engine, dsn)
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	conn, err := db.Conn(context.TODO())
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

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
	err = setup.auroraTestUtility.TriggerFailover(originalWriterId, "", "")
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

	// Allow topology to refresh and mark readers as available
	time.Sleep(5 * time.Second)

	// Verify reader connection
	instanceId, err = executeInstanceQueryReadOnly(setup.env, conn, 60)
	require.NoError(t, err)
	assert.False(t, setup.auroraTestUtility.IsDbInstanceWriter(instanceId, ""))
}

func rwsFailoverToNewReaderConnTest(t *testing.T, cfg readWriteSplittingTestConfig) {
	setup := rwsSetupProxiedTest(t, 3)
	defer setup.cleanup(t)

	test_utils.SkipForMultiAzMySql(t, setup.env.Info().Request.Deployment, setup.env.Info().Request.Engine)

	props := cfg.setupProxiedFn(t, setup.env, setup.env.Info().ProxyDatabaseInfo.ClusterEndpoint, 2)
	if cfg.pluginType == GdbReadWriteSplittingPluginMode {
		props["activeHomeFailoverMode"] = "home-reader-or-writer"
		props["inactiveHomeFailoverMode"] = "home-reader-or-writer"
	} else {
		props[property_util.FAILOVER_MODE.Name] = "reader-or-writer"
	}
	props[property_util.CLUSTER_TOPOLOGY_REFRESH_RATE_MS.Name] = "5000"
	dsn := test_utils.GetDsn(setup.env, props)

	db, err := test_utils.OpenDb(setup.env.Info().Request.Engine, dsn)
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	conn, err := db.Conn(context.TODO())
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

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
	_, err = executeInstanceQuery(setup.env, conn, context.TODO(), 60)
	assert.Error(t, err)
	awsWrapperError, ok := err.(*error_util.AwsWrapperError)
	assert.True(t, ok)
	assert.True(t, awsWrapperError.IsFailoverErrorType())
	assert.Equal(t, error_util.FailoverSuccessError, awsWrapperError)

	currentReaderId, err := executeInstanceQuery(setup.env, conn, context.TODO(), 20)
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

func rwsFailoverReaderToWriterConnTest(t *testing.T, cfg readWriteSplittingTestConfig) {
	setup := rwsSetupProxiedTest(t, 3)
	defer setup.cleanup(t)

	test_utils.SkipForDeployment(t, test_utils.RDS_MULTI_AZ_CLUSTER, setup.env.Info().Request.Deployment)

	props := cfg.setupProxiedFn(t, setup.env, setup.env.Info().ProxyDatabaseInfo.ClusterEndpoint, 2)
	props[property_util.CLUSTER_TOPOLOGY_REFRESH_RATE_MS.Name] = "5000"
	dsn := test_utils.GetDsn(setup.env, props)

	db, err := test_utils.OpenDb(setup.env.Info().Request.Engine, dsn)
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	conn, err := db.Conn(context.TODO())
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

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
	_, err = executeInstanceQuery(setup.env, conn, context.TODO(), 60)
	assert.Error(t, err)
	awsWrapperError, ok := err.(*error_util.AwsWrapperError)
	assert.True(t, ok)
	assert.True(t, awsWrapperError.IsFailoverErrorType())

	instanceId, err := executeInstanceQuery(setup.env, conn, context.TODO(), 60)
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

func rwsSetInternalPoolProvider(poolOptions ...internal_pool.InternalPoolOption) *internal_pool.InternalPooledConnectionProvider {
	options := internal_pool.NewInternalPoolOptions(poolOptions...)
	provider := internal_pool.NewInternalPooledConnectionProvider(options, 0)
	driver_infrastructure.SetCustomConnectionProvider(provider)
	return provider
}

func rwsPooledConnectionFailoverConnTest(t *testing.T, cfg readWriteSplittingTestConfig) {
	setup := rwsSetupProxiedTest(t, 3)
	defer setup.cleanup(t)

	provider := rwsSetInternalPoolProvider()
	defer driver_infrastructure.ResetCustomConnectionProvider()
	defer provider.ReleaseResources()

	props := cfg.setupProxiedFn(t, setup.env, setup.env.Info().ProxyDatabaseInfo.ClusterEndpoint, 2)
	props["failoverTimeoutMs"] = "10000"
	dsn := test_utils.GetDsn(setup.env,
		props)
	db, err := test_utils.OpenDb(setup.env.Info().Request.Engine, dsn)
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	conn, err := db.Conn(context.TODO())
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	originalWriterId, err := executeInstanceQueryWrite(setup.env, conn, 60)
	require.NoError(t, err)
	assert.True(t, setup.auroraTestUtility.IsDbInstanceWriter(originalWriterId, ""))

	test_utils.DisableAllConnectivity()
	time.Sleep(5 * time.Second)

	_, err = executeInstanceQueryReadOnly(setup.env, conn, 60)
	require.Error(t, err)
	awsWrapperError, ok := err.(*error_util.AwsWrapperError)
	require.True(t, ok)
	require.True(t, awsWrapperError.IsFailoverErrorType())

	test_utils.EnableAllConnectivity(true)
	err = test_utils.VerifyClusterStatus()
	require.NoError(t, err)

	conn2, err := db.Conn(context.TODO())
	require.NoError(t, err)
	defer func() { _ = conn2.Close() }()

	newWriterId, err := executeInstanceQueryWrite(setup.env, conn2, 2)
	require.NoError(t, err)
	assert.Equal(t, originalWriterId, newWriterId)
}

func rwsPooledConnectionFailoverInTransactionConnTest(t *testing.T, cfg readWriteSplittingTestConfig) {
	setup := rwsSetupProxiedTest(t, 3)
	defer setup.cleanup(t)

	rwsSetInternalPoolProvider()
	defer driver_infrastructure.ResetCustomConnectionProvider()

	dsn := test_utils.GetDsn(setup.env,
		cfg.setupProxiedFn(t, setup.env, setup.env.Info().ProxyDatabaseInfo.ClusterEndpoint, 2))
	db, err := test_utils.OpenDb(setup.env.Info().Request.Engine, dsn)
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	conn, err := db.Conn(context.TODO())
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	originalWriterId, err := executeInstanceQueryWrite(setup.env, conn, 5)
	require.NoError(t, err)
	assert.True(t, setup.auroraTestUtility.IsDbInstanceWriter(originalWriterId, ""))

	_, err = conn.ExecContext(context.TODO(), "START TRANSACTION READ ONLY")
	require.NoError(t, err)

	_, err = executeInstanceQueryReadOnly(setup.env, conn, 5)
	require.NoError(t, err)

	err = setup.auroraTestUtility.TriggerFailover(originalWriterId, "", "")
	require.NoError(t, err)

	_, err = executeInstanceQuery(setup.env, conn, context.TODO(), 60)
	assert.Error(t, err)
	awsWrapperError, ok := err.(*error_util.AwsWrapperError)
	assert.True(t, ok)
	assert.True(t, awsWrapperError.IsFailoverErrorType())

	newWriter, err := executeInstanceQuery(setup.env, conn, context.TODO(), 5)
	assert.NoError(t, err)
	// Different behaviour for multi-AZ b/c it simulates failover which reconnects to the original instance.
	if setup.env.Info().Request.Deployment == test_utils.RDS_MULTI_AZ_CLUSTER {
		assert.Equal(t, originalWriterId, newWriter)
	} else {
		assert.NotEqual(t, originalWriterId, newWriter)
	}
	assert.True(t, setup.auroraTestUtility.IsDbInstanceWriter(newWriter, ""))

	_, err = conn.ExecContext(context.TODO(), "COMMIT")
	assert.NoError(t, err)
}
