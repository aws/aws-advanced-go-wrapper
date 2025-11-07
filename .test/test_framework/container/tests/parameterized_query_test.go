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
	"errors"
	"slices"
	"strconv"
	"testing"

	"github.com/aws/aws-advanced-go-wrapper/.test/test_framework/container/test_utils"
	"github.com/aws/aws-advanced-go-wrapper/awssql/driver_infrastructure"
	"github.com/aws/aws-advanced-go-wrapper/awssql/property_util"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testValue   = "42"
	testName    = "jane_doe"
	queryValue  = "40"
	queryStatus = "active"
	tableName   = "paramtest"
)

func setupParamTest(t *testing.T, testName string) (*test_utils.TestEnvironment, *sql.DB) {
	err := test_utils.BasicSetup(testName)
	require.NoError(t, err)

	env, err := test_utils.GetCurrentTestEnvironment()
	require.NoError(t, err)
	plugins := property_util.DEFAULT_PLUGINS
	if slices.Contains(env.Info().Request.Features, test_utils.LIMITLESS_DEPLOYMENT) {
		plugins = driver_infrastructure.LIMITLESS_PLUGIN_CODE
	}

	props := map[string]string{"plugins": plugins}
	dsn := test_utils.GetDsn(env, props)
	db, err := test_utils.OpenDb(env.Info().Request.Engine, dsn)
	require.NoError(t, err)

	return env, db
}

func getPreparedStatement(env *test_utils.TestEnvironment) string {
	switch env.Info().Request.Engine {
	case test_utils.PG:
		return "SELECT $1::int as value, $2::text as name"
	case test_utils.MYSQL:
		return "SELECT ? as value, ? as name"
	default:
		return ""
	}
}

func runPositionalArgsQuery(env *test_utils.TestEnvironment, db *sql.DB) (*sql.Row, error) {
	if env.Info().Request.Engine == test_utils.PG {
		row := db.QueryRow("select @val as value, @status as status", pgx.NamedArgs{
			"val":    queryValue,
			"status": queryStatus,
		})
		return row, nil
	} else if env.Info().Request.Engine == test_utils.MYSQL {
		row := db.QueryRow("select ? as value, ? as status", queryValue, queryStatus)
		return row, nil
	}
	return nil, errors.New("Invalid DB engine")
}

func createParamTestTable(env *test_utils.TestEnvironment, db *sql.DB) error {
	_, err := db.Exec("DROP TABLE IF EXISTS " + tableName)
	if err != nil {
		return err
	}
	if env.Info().Request.Engine == test_utils.PG {
		_, err = db.Exec("CREATE TABLE " + tableName + "(val int primary key, status text)")
	} else {
		_, err = db.Exec("CREATE TABLE " + tableName + "(val int primary key, status varchar(255))")
	}
	return err
}

func getInsertStatement(env *test_utils.TestEnvironment) string {
	switch env.Info().Request.Engine {
	case test_utils.PG:
		return "INSERT INTO " + tableName + "(val, status) VALUES ($1, $2)"
	case test_utils.MYSQL:
		return "INSERT INTO " + tableName + "(val, status) VALUES (?, ?)"
	default:
		return ""
	}
}

func runPositionalArgsExec(env *test_utils.TestEnvironment, db *sql.DB) (sql.Result, error) {
	if env.Info().Request.Engine == test_utils.PG {
		return db.Exec("INSERT INTO "+tableName+"(val, status) VALUES (@val, @status)", pgx.NamedArgs{
			"val":    queryValue,
			"status": queryStatus,
		})
	} else if env.Info().Request.Engine == test_utils.MYSQL {
		return db.Exec("INSERT INTO "+tableName+"(val, status) VALUES (?, ?)", queryValue, queryStatus)
	}
	return nil, errors.New("Invalid DB engine")
}

func verifyInsertedData(t *testing.T, env *test_utils.TestEnvironment, db *sql.DB, expectedVal int, expectedStatus string) {
	var insertedVal int
	var insertedStatus string
	var err error
	if env.Info().Request.Engine == test_utils.PG {
		err = db.QueryRow("SELECT val, status FROM "+tableName+" WHERE val = $1", expectedVal).Scan(&insertedVal, &insertedStatus)
	} else {
		err = db.QueryRow("SELECT val, status FROM "+tableName+" WHERE val = ?", expectedVal).Scan(&insertedVal, &insertedStatus)
	}
	require.NoError(t, err)
	assert.Equal(t, expectedVal, insertedVal)
	assert.Equal(t, expectedStatus, insertedStatus)
}

func TestParameterizedQuery(t *testing.T) {
	t.Run("Parameterized Query With Named/Positional Args Query", func(t *testing.T) {
		env, db := setupParamTest(t, t.Name())
		defer db.Close()
		row, err := runPositionalArgsQuery(env, db)
		require.NoError(t, err)

		var value int
		var status string
		err = row.Scan(&value, &status)
		require.NoError(t, err)

		queryValueInt, _ := strconv.Atoi(queryValue)
		assert.Equal(t, queryValueInt, value)
		assert.Equal(t, queryStatus, status)
		test_utils.BasicCleanup(t.Name())
	})

	t.Run("Parameterized Query With Named/Positional Args Exec", func(t *testing.T) {
		env, db := setupParamTest(t, t.Name())
		defer db.Close()

		err := createParamTestTable(env, db)
		require.NoError(t, err)

		result, err := runPositionalArgsExec(env, db)
		require.NoError(t, err)

		rowsAffected, err := result.RowsAffected()
		require.NoError(t, err)
		assert.Equal(t, int64(1), rowsAffected)

		queryValueInt, _ := strconv.Atoi(queryValue)
		verifyInsertedData(t, env, db, queryValueInt, queryStatus)
		test_utils.BasicCleanup(t.Name())
	})

	t.Run("Parameterized Query With Prepared Statements Stmt", func(t *testing.T) {
		env, db := setupParamTest(t, t.Name())
		defer db.Close()

		preparedStatement := getPreparedStatement(env)
		stmt, err := db.PrepareContext(context.TODO(), preparedStatement)
		require.NoError(t, err)
		defer stmt.Close()

		var returnedValue int
		var returnedName string
		err = stmt.QueryRow(testValue, testName).Scan(&returnedValue, &returnedName)
		require.NoError(t, err)
		testValueInt, _ := strconv.Atoi(testValue)
		assert.Equal(t, testValueInt, returnedValue)
		assert.Equal(t, testName, returnedName)
		test_utils.BasicCleanup(t.Name())
	})

	t.Run("Parameterized Query With Prepared Statements Conn", func(t *testing.T) {
		env, db := setupParamTest(t, t.Name())
		defer db.Close()

		conn, err := db.Conn(context.TODO())
		require.NoError(t, err)
		defer conn.Close()

		preparedStatement := getPreparedStatement(env)
		stmt, err := conn.PrepareContext(context.TODO(), preparedStatement)
		require.NoError(t, err)
		defer stmt.Close()

		var returnedValue int
		var returnedName string
		err = stmt.QueryRow(testValue, testName).Scan(&returnedValue, &returnedName)
		require.NoError(t, err)
		testValueInt, _ := strconv.Atoi(testValue)
		assert.Equal(t, testValueInt, returnedValue)
		assert.Equal(t, testName, returnedName)
		test_utils.BasicCleanup(t.Name())
	})

	t.Run("Parameterized Query With Prepared Statements Tx", func(t *testing.T) {
		env, db := setupParamTest(t, t.Name())
		defer db.Close()

		tx, err := db.BeginTx(context.TODO(), nil)
		require.NoError(t, err)
		defer func() {
			_ = tx.Rollback()
		}()

		preparedStatement := getPreparedStatement(env)
		stmt, err := tx.PrepareContext(context.TODO(), preparedStatement)
		require.NoError(t, err)
		defer stmt.Close()

		var returnedValue int
		var returnedName string
		err = stmt.QueryRow(testValue, testName).Scan(&returnedValue, &returnedName)
		require.NoError(t, err)
		testValueInt, _ := strconv.Atoi(testValue)
		assert.Equal(t, testValueInt, returnedValue)
		assert.Equal(t, testName, returnedName)

		err = tx.Commit()
		require.NoError(t, err)
		test_utils.BasicCleanup(t.Name())
	})

	t.Run("Parameterized Query With Prepared Statements Stmt Exec", func(t *testing.T) {
		env, db := setupParamTest(t, t.Name())
		defer db.Close()

		err := createParamTestTable(env, db)
		require.NoError(t, err)

		insertStatement := getInsertStatement(env)
		stmt, err := db.PrepareContext(context.TODO(), insertStatement)
		require.NoError(t, err)
		defer stmt.Close()

		result, err := stmt.Exec(testValue, testName)
		require.NoError(t, err)

		rowsAffected, err := result.RowsAffected()
		require.NoError(t, err)
		assert.Equal(t, int64(1), rowsAffected)

		testValueInt, _ := strconv.Atoi(testValue)
		verifyInsertedData(t, env, db, testValueInt, testName)
		test_utils.BasicCleanup(t.Name())
	})

	t.Run("Parameterized Query With Prepared Statements Conn Exec", func(t *testing.T) {
		env, db := setupParamTest(t, t.Name())
		defer db.Close()

		err := createParamTestTable(env, db)
		require.NoError(t, err)

		conn, err := db.Conn(context.TODO())
		require.NoError(t, err)
		defer conn.Close()

		insertStatement := getInsertStatement(env)
		stmt, err := conn.PrepareContext(context.TODO(), insertStatement)
		require.NoError(t, err)
		defer stmt.Close()

		result, err := stmt.Exec(testValue, testName)
		require.NoError(t, err)

		rowsAffected, err := result.RowsAffected()
		require.NoError(t, err)
		assert.Equal(t, int64(1), rowsAffected)

		testValueInt, _ := strconv.Atoi(testValue)
		verifyInsertedData(t, env, db, testValueInt, testName)
		test_utils.BasicCleanup(t.Name())
	})

	t.Run("Parameterized Query With Prepared Statements Tx Exec", func(t *testing.T) {
		env, db := setupParamTest(t, t.Name())
		defer db.Close()

		err := createParamTestTable(env, db)
		require.NoError(t, err)

		tx, err := db.BeginTx(context.TODO(), nil)
		require.NoError(t, err)
		defer func() {
			_ = tx.Rollback()
		}()

		insertStatement := getInsertStatement(env)
		stmt, err := tx.PrepareContext(context.TODO(), insertStatement)
		require.NoError(t, err)
		defer stmt.Close()

		result, err := stmt.Exec(testValue, testName)
		require.NoError(t, err)

		rowsAffected, err := result.RowsAffected()
		require.NoError(t, err)
		assert.Equal(t, int64(1), rowsAffected)

		err = tx.Commit()
		require.NoError(t, err)

		testValueInt, _ := strconv.Atoi(testValue)
		verifyInsertedData(t, env, db, testValueInt, testName)
		test_utils.BasicCleanup(t.Name())
	})
}
