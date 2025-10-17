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
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-advanced-go-wrapper/.test/test_framework/container/test_utils"
	"github.com/aws/aws-advanced-go-wrapper/awssql/property_util"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

const STRESS_TEST_NUM_ROUTINES = 5

func setupStressTest(t *testing.T) (map[string]string, *test_utils.TestEnvironment) {
	err := test_utils.BasicSetupInfoLog(t.Name())
	assert.NoError(t, err)
	env, err := test_utils.GetCurrentTestEnvironment()
	assert.NoError(t, err)
	test_utils.SkipForTestEnvironmentFeatures(t, env.Info().Request.Features, test_utils.LIMITLESS_DEPLOYMENT)
	test_utils.SkipIfInsufficientInstances(t, env, 2)

	props := map[string]string{
		"host":                       env.Info().ProxyDatabaseInfo.Instances[0].Host(),
		"port":                       strconv.Itoa(env.Info().ProxyDatabaseInfo.InstanceEndpointPort),
		"clusterInstanceHostPattern": "?." + env.Info().ProxyDatabaseInfo.InstanceEndpointSuffix,
	}

	return props, env
}

func runConcurrentQueries(queryFunc func()) {
	var wg sync.WaitGroup
	var counter int32
	for i := 0; i < STRESS_TEST_NUM_ROUTINES; i++ {
		time.Sleep(10 * time.Millisecond)
		wg.Add(1)
		go func() {
			defer wg.Done()
			routineNum := atomic.AddInt32(&counter, 1)
			slog.Info(fmt.Sprintf("Routine %d starting...\n", routineNum))
			queryFunc()
			slog.Info(fmt.Sprintf("Routine %d done\n", routineNum))
		}()
	}
	wg.Wait()
}

func runStressTest(t *testing.T, queryFunc func(*sql.DB, *test_utils.TestEnvironment)) {
	props, env := setupStressTest(t)
	defer test_utils.BasicCleanup(t.Name())

	dsn := test_utils.GetDsn(env, props)
	db, err := test_utils.OpenDb(env.Info().Request.Engine, dsn)
	assert.NoError(t, err)

	runConcurrentQueries(func() {
		queryFunc(db, env)
	})
}

func TestStress_MultipleDbQueryWithSleep(t *testing.T) {
	runStressTest(t, func(db *sql.DB, env *test_utils.TestEnvironment) {
		_, err := test_utils.ExecuteInstanceQueryWithSleep(env.Info().Request.Engine, env.Info().Request.Deployment, db, 10)
		assert.NoError(t, err)
	})
}

func TestStress_MultipleConnSleep(t *testing.T) {
	runStressTest(t, func(db *sql.DB, env *test_utils.TestEnvironment) {
		conn, err := db.Conn(context.TODO())
		assert.NoError(t, err)
		defer conn.Close()
		_, err = test_utils.ExecuteInstanceQueryWithSleep(env.Info().Request.Engine, env.Info().Request.Deployment, conn, 10)
		assert.NoError(t, err)
	})
}

func TestStress_MultipleBeginTxSleep(t *testing.T) {
	runStressTest(t, func(db *sql.DB, env *test_utils.TestEnvironment) {
		tx, err := db.BeginTx(context.TODO(), nil)
		assert.NoError(t, err)
		_, err = test_utils.ExecuteInstanceQueryWithSleep(env.Info().Request.Engine, env.Info().Request.Deployment, tx, 10)
		assert.NoError(t, err)
		err = tx.Commit()
		assert.NoError(t, err)
	})
}

func setupFailoverTestWithProps(t *testing.T, props map[string]string, env *test_utils.TestEnvironment) (*sql.DB, *test_utils.AuroraTestUtility, string) {
	dsn := test_utils.GetDsn(env, props)
	db, err := test_utils.OpenDb(env.Info().Request.Engine, dsn)
	assert.NoError(t, err)

	auroraTestUtility := test_utils.NewAuroraTestUtility(env.Info())
	tableName := "test_stress_table"

	_, err = db.ExecContext(context.TODO(), "DROP TABLE IF EXISTS "+tableName)
	assert.NoError(t, err)
	_, err = db.ExecContext(context.TODO(), "CREATE TABLE "+tableName+" (id INT, name VARCHAR(255))")
	assert.NoError(t, err)

	return db, auroraTestUtility, tableName
}

func setupFailoverTest(t *testing.T) (*sql.DB, *test_utils.AuroraTestUtility, string) {
	props, env := setupStressTest(t)
	return setupFailoverTestWithProps(t, props, env)
}

func setupSecretsTest(t *testing.T) (map[string]string, *test_utils.TestEnvironment, string) {
	props, env := setupStressTest(t)
	cfg, _ := config.LoadDefaultConfig(context.TODO())
	client := secretsmanager.NewFromConfig(cfg, func(o *secretsmanager.Options) {
		o.Region = env.Info().Region
	})
	secretName := fmt.Sprintf("TestSecret-%s", uuid.New().String())
	secretArn := CreateSecret(t, client, env, secretName)

	props[property_util.SECRETS_MANAGER_SECRET_ID.Name] = secretName
	props[property_util.SECRETS_MANAGER_REGION.Name] = env.Info().Region
	props[property_util.USER.Name] = "incorrectUser"
	props[property_util.PASSWORD.Name] = "incorrectPassword"
	props[property_util.PLUGINS.Name] = "failover,efm,awsSecretsManager"

	t.Cleanup(func() { DeleteSecret(t, client, secretArn) })
	return props, env, secretArn
}
func setupMultiplePluginsTest(t *testing.T) (map[string]string, *test_utils.TestEnvironment, string) {
	props, env := setupStressTest(t)
	cfg, _ := config.LoadDefaultConfig(context.TODO())
	client := secretsmanager.NewFromConfig(cfg, func(o *secretsmanager.Options) {
		o.Region = env.Info().Region
	})
	secretName := fmt.Sprintf("TestSecret-%s", uuid.New().String())
	secretArn := CreateSecret(t, client, env, secretName)

	props[property_util.SECRETS_MANAGER_SECRET_ID.Name] = secretName
	props[property_util.SECRETS_MANAGER_REGION.Name] = env.Info().Region
	props[property_util.USER.Name] = "incorrectUser"
	props[property_util.PASSWORD.Name] = "incorrectPassword"
	props[property_util.PLUGINS.Name] = "failover,efm,awsSecretsManager,readWriteSplitting,executionTime,connectTime,bg"

	t.Cleanup(func() { DeleteSecret(t, client, secretArn) })
	return props, env, secretArn
}

func runFailoverTest(
	t *testing.T,
	db *sql.DB,
	auroraTestUtility *test_utils.AuroraTestUtility,
	functions ...func()) {
	stop := make(chan struct{})
	var wg sync.WaitGroup

	// Create and start a goroutine for each function
	for _, fn := range functions {
		wg.Add(1)
		go func(f func()) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					f()
					time.Sleep(100 * time.Millisecond)
				}
			}
		}(fn)
	}

	time.Sleep(10 * time.Second)
	err := auroraTestUtility.TriggerFailover("", "", "")
	assert.NoError(t, err)
	time.Sleep(10 * time.Second)
	err = auroraTestUtility.TriggerFailover("", "", "")
	assert.NoError(t, err)
	time.Sleep(10 * time.Second)

	close(stop)
	wg.Wait()
}

func runFailoverTestWithInsert(t *testing.T, setupFunc func(*testing.T) (*sql.DB, *test_utils.AuroraTestUtility, string), insertFunc func(*sql.DB, string)) {
	assert.NotPanics(t, func() {
		db, auroraTestUtility, tableName := setupFunc(t)
		defer test_utils.BasicCleanup(t.Name())

		runFailoverTest(t, db, auroraTestUtility, func() {
			insertFunc(db, tableName)
		})
	})
}

func runFailoverTestWithSelectInsert(
	t *testing.T,
	setupFunc func(*testing.T) (*sql.DB, *test_utils.AuroraTestUtility, string),
	selectFunc func(*sql.DB, string), insertFunc func(*sql.DB, string)) {
	assert.NotPanics(t, func() {
		db, auroraTestUtility, tableName := setupFunc(t)
		defer test_utils.BasicCleanup(t.Name())
		runFailoverTest(t, db, auroraTestUtility, func() {
			selectFunc(db, tableName)
		}, func() {
			insertFunc(db, tableName)
		})
	})
}

func TestStress_ContinuousInsertWithFailoverTx(t *testing.T) {
	runFailoverTestWithInsert(t, setupFailoverTest, func(db *sql.DB, tableName string) {
		tx, err := db.Begin()
		if err != nil {
			slog.Info("failed to begin transaction", "error", err)
			return
		}

		_, err = tx.ExecContext(context.TODO(), "INSERT INTO "+tableName+" (id, name) VALUES (1, 'test')")
		if err != nil {
			slog.Info("insert failed", "error", err)
			return
		}

		if err := tx.Commit(); err != nil {
			slog.Info("failed to commit transaction", "error", err)
			if err := tx.Rollback(); err != nil {
				slog.Info("failed to rollback transaction", "error", err)
			}
			return
		}
	})
}

func TestStress_ContinuousInsertWithFailoverDb(t *testing.T) {
	runFailoverTestWithInsert(t, setupFailoverTest, func(db *sql.DB, tableName string) {
		_, err := db.ExecContext(context.TODO(), "INSERT INTO "+tableName+" (id, name) VALUES (1, 'test')")
		if err != nil {
			slog.Info("insert failed", "error", err)
			return
		}
	})
}

func TestStress_ContinuousInsertWithFailoverConn(t *testing.T) {
	runFailoverTestWithInsert(t, setupFailoverTest, func(db *sql.DB, tableName string) {
		conn, err := db.Conn(context.TODO())
		if err != nil {
			slog.Info("failed to get connection", "error", err)
			return
		}
		defer conn.Close()

		_, err = conn.ExecContext(context.TODO(), "INSERT INTO "+tableName+" (id, name) VALUES (1, 'test')")
		if err != nil {
			slog.Info("insert failed", "error", err)
			return
		}
	})
}

func TestStress_ContinuousInsertWithFailoverAndSecretsTx(t *testing.T) {
	props, env, _ := setupSecretsTest(t)
	runFailoverTestWithInsert(t, func(t *testing.T) (*sql.DB, *test_utils.AuroraTestUtility, string) {
		return setupFailoverTestWithProps(t, props, env)
	}, func(db *sql.DB, tableName string) {
		tx, err := db.Begin()
		if err != nil {
			slog.Info("failed to begin transaction", "error", err)
			return
		}
		_, err = tx.ExecContext(context.TODO(), "INSERT INTO "+tableName+" (id, name) VALUES (1, 'test')")
		if err != nil {
			slog.Info("insert failed", "error", err)
			return
		}

		if err := tx.Commit(); err != nil {
			slog.Info("failed to commit transaction", "error", err)
			if err := tx.Rollback(); err != nil {
				slog.Info("failed to rollback transaction", "error", err)
			}
			return
		}
	})
}

func TestStress_ContinuousInsertWithFailoverAndSecretsDb(t *testing.T) {
	props, env, _ := setupSecretsTest(t)
	runFailoverTestWithInsert(t, func(t *testing.T) (*sql.DB, *test_utils.AuroraTestUtility, string) {
		return setupFailoverTestWithProps(t, props, env)
	}, func(db *sql.DB, tableName string) {
		_, err := db.ExecContext(context.TODO(), "INSERT INTO "+tableName+" (id, name) VALUES (1, 'test')")
		if err != nil {
			slog.Info("insert failed", "error", err)
			return
		}
	})
}

func TestStress_ContinuousInsertWithFailoverAndSecretsConn(t *testing.T) {
	props, env, _ := setupSecretsTest(t)
	runFailoverTestWithInsert(t, func(t *testing.T) (*sql.DB, *test_utils.AuroraTestUtility, string) {
		return setupFailoverTestWithProps(t, props, env)
	}, func(db *sql.DB, tableName string) {
		conn, err := db.Conn(context.TODO())
		if err != nil {
			slog.Info("failed to get connection", "error", err)
			return
		}
		defer conn.Close()

		_, err = conn.ExecContext(context.TODO(), "INSERT INTO "+tableName+" (id, name) VALUES (1, 'test')")
		if err != nil {
			slog.Info("insert failed", "error", err)
			return
		}
	})
}

func TestStress_ContinuousSelectInsertWithFailoverAndMultiplePluginsConn(t *testing.T) {
	props, env, _ := setupMultiplePluginsTest(t)
	runFailoverTestWithSelectInsert(t, func(t *testing.T) (*sql.DB, *test_utils.AuroraTestUtility, string) {
		return setupFailoverTestWithProps(t, props, env)
	}, func(db *sql.DB, tableName string) {
		conn, err := db.Conn(context.TODO())
		if err != nil {
			slog.Info("failed to get connection", "error", err)
			return
		}
		defer conn.Close()

		rows, err := conn.QueryContext(context.TODO(), "SELECT * FROM "+tableName+" LIMIT 1")
		if err != nil {
			slog.Info("select failed", "error", err)
			return
		}
		defer rows.Close()
		if rows.Next() {
			slog.Info("select succeeded")
		}
	}, func(db *sql.DB, tableName string) {
		conn, err := db.Conn(context.TODO())
		if err != nil {
			slog.Info("failed to get connection", "error", err)
			return
		}
		defer conn.Close()

		_, err = conn.ExecContext(context.TODO(), "INSERT INTO "+tableName+" (id, name) VALUES (1, 'test')")
		if err != nil {
			slog.Info("insert failed", "error", err)
			return
		}
		slog.Info("insert succeeded")
	})
}

func TestStress_ContinuousSelectInsertWithFailoverAndMultiplePluginsDb(t *testing.T) {
	props, env, _ := setupMultiplePluginsTest(t)
	runFailoverTestWithSelectInsert(t, func(t *testing.T) (*sql.DB, *test_utils.AuroraTestUtility, string) {
		return setupFailoverTestWithProps(t, props, env)
	}, func(db *sql.DB, tableName string) {
		rows, err := db.QueryContext(context.TODO(), "SELECT * FROM "+tableName+" LIMIT 1")
		if err != nil {
			slog.Info("select failed", "error", err)
			return
		}
		defer rows.Close()
		if rows.Next() {
			slog.Info("select succeeded")
		}
	}, func(db *sql.DB, tableName string) {
		_, err := db.ExecContext(context.TODO(), "INSERT INTO "+tableName+" (id, name) VALUES (1, 'test')")
		if err != nil {
			slog.Info("insert failed", "error", err)
			return
		}
		slog.Info("insert succeeded")
	})
}

func TestStress_ContinuousSelectInsertWithFailoverAndMultiplePluginsTx(t *testing.T) {
	props, env, _ := setupMultiplePluginsTest(t)
	runFailoverTestWithSelectInsert(t, func(t *testing.T) (*sql.DB, *test_utils.AuroraTestUtility, string) {
		return setupFailoverTestWithProps(t, props, env)
	}, func(db *sql.DB, tableName string) {
		tx, err := db.BeginTx(context.TODO(), &sql.TxOptions{ReadOnly: true})
		if err != nil {
			slog.Info("failed to begin transaction", "error", err)
			return
		}

		rows, err := tx.QueryContext(context.TODO(), "SELECT * FROM "+tableName+" LIMIT 1")
		if err != nil {
			slog.Info("select failed", "error", err)
			return
		}
		defer rows.Close()
		if rows.Next() {
			slog.Info("select succeeded")
		}

		if err := tx.Commit(); err != nil {
			slog.Info("failed to commit transaction", "error", err)
			if err := tx.Rollback(); err != nil {
				slog.Info("failed to rollback transaction", "error", err)
			}
			return
		}
	}, func(db *sql.DB, tableName string) {
		tx, err := db.BeginTx(context.TODO(), &sql.TxOptions{ReadOnly: false})
		if err != nil {
			slog.Info("failed to begin transaction", "error", err)
			return
		}

		_, err = tx.ExecContext(context.TODO(), "INSERT INTO "+tableName+" (id, name) VALUES (1, 'test')")
		if err != nil {
			slog.Info("insert failed", "error", err)
			return
		}

		if err := tx.Commit(); err != nil {
			slog.Info("failed to commit transaction", "error", err)
			if err := tx.Rollback(); err != nil {
				slog.Info("failed to rollback transaction", "error", err)
			}
			return
		}
	})
}
