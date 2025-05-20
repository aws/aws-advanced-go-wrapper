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
	"log/slog"

	"awssql/error_util"
	"awssql/test_framework/container/test_utils"
	"database/sql"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFailoverWriterDB(t *testing.T) {
	auroraTestUtility, err := failoverSetup(t)
	assert.Nil(t, err)
	environment, err := test_utils.GetCurrentTestEnvironment()
	assert.Nil(t, err)
	dsn := test_utils.GetDsn(environment, map[string]string{
		"host":    environment.Info().DatabaseInfo.ClusterEndpoint,
		"port":    strconv.Itoa(environment.Info().DatabaseInfo.InstanceEndpointPort()),
		"plugins": "failover",
	})
	db, err := sql.Open("awssql", dsn)
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
		triggerFailoverError = auroraTestUtility.FailoverClusterAndWaitTillWriterChanged("", "", "")
		close(failoverComplete)
	}()
	_, queryError := test_utils.ExecuteQuery(environment.Info().Request.Engine, db, test_utils.GetSleepSql(environment.Info().Request.Engine, 60))
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
	basicCleanup(t.Name())
}

func TestFailoverWriterInTransactionWithBegin(t *testing.T) {
	auroraTestUtility, err := failoverSetup(t)
	assert.Nil(t, err)
	environment, err := test_utils.GetCurrentTestEnvironment()
	assert.Nil(t, err)
	dsn := test_utils.GetDsn(environment, map[string]string{
		"host":    environment.Info().DatabaseInfo.ClusterEndpoint,
		"port":    strconv.Itoa(environment.Info().DatabaseInfo.InstanceEndpointPort()),
		"plugins": "failover",
	})
	db, err := sql.Open("awssql", dsn)
	assert.Nil(t, err)
	assert.NotNil(t, db)
	defer db.Close()

	// Verify connection works.
	err = db.Ping()
	require.NoError(t, err, "Failed to open database connection.")

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
	triggerFailoverError := auroraTestUtility.FailoverClusterAndWaitTillWriterChanged("", "", "")
	assert.Nil(t, triggerFailoverError)

	_, txErr := tx.Exec("INSERT INTO test_failover_tx_rollback VALUES (2, 'should fail to execute')")
	require.Error(t, txErr)
	assert.Equal(t, error_util.GetMessage("Failover.transactionResolutionUnknownError"), txErr.Error())

	// Verify that the transaction was rolled back by checking the table contents.
	var count int = -1
	err = db.QueryRow("SELECT COUNT(*) FROM test_failover_tx_rollback").Scan(&count)
	assert.Nil(t, err)
	assert.Equal(t, 0, count, "Transaction should have been rolled back, table should be empty.")

	instanceId, err := test_utils.ExecuteInstanceQueryDB(environment.Info().Request.Engine, environment.Info().Request.Deployment, db)
	assert.Nil(t, err)
	currWriterId, err := auroraTestUtility.GetClusterWriterInstanceId("")
	assert.Nil(t, err)
	if instanceId != currWriterId {
		slog.Error(fmt.Sprintf("We did not failover to the writer. Connected to: %s. Writer: %s.", instanceId, currWriterId))
		db, err := sql.Open("awssql", dsn)
		assert.Nil(t, err)
		assert.NotNil(t, db)
		defer db.Close()
	}

	_, err = db.Exec("DROP TABLE IF EXISTS test_failover_tx_rollback")
	assert.Nil(t, err)

	basicCleanup(t.Name())
}
