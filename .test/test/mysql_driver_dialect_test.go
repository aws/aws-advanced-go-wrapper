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
	"testing"

	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/property_util"
	mysql_driver "github.com/aws/aws-advanced-go-wrapper/mysql-driver"
	"github.com/go-sql-driver/mysql"

	"github.com/stretchr/testify/assert"
)

func TestPrepareDsnWithoutUser(t *testing.T) {
	driverDialect := &mysql_driver.MySQLDriverDialect{}

	properties := map[string]string{
		property_util.PASSWORD.Name: "password",
		property_util.PORT.Name:     "3306",
		property_util.HOST.Name:     "host",
		property_util.DATABASE.Name: "dbName",
		property_util.NET.Name:      "tcp",
		property_util.PLUGINS.Name:  "test",
		"monitoring-user":           "monitor-user",
	}

	dsn := driverDialect.PrepareDsn(properties, nil)
	assert.Equal(t, "tcp(host:3306)/dbName", dsn)
}

func TestPrepareDsnWithoutNet(t *testing.T) {
	driverDialect := &mysql_driver.MySQLDriverDialect{}

	properties := map[string]string{
		property_util.USER.Name:     "user",
		property_util.PASSWORD.Name: "password",
		property_util.PORT.Name:     "3306",
		property_util.HOST.Name:     "host",
		property_util.DATABASE.Name: "dbName",
		property_util.PLUGINS.Name:  "test",
	}

	dsn := driverDialect.PrepareDsn(properties, nil)
	assert.Equal(t, "user:password@(host:3306)/dbName", dsn)
}

func TestPrepareDsnWithEscapedDatabase(t *testing.T) {
	driverDialect := &mysql_driver.MySQLDriverDialect{}

	properties := map[string]string{
		property_util.USER.Name:     "user",
		property_util.PASSWORD.Name: "password",
		property_util.PORT.Name:     "3306",
		property_util.HOST.Name:     "host",
		property_util.DATABASE.Name: "dbName/name",
		property_util.NET.Name:      "tcp",
		property_util.PLUGINS.Name:  "test",
	}

	dsn := driverDialect.PrepareDsn(properties, nil)
	assert.Equal(t, "user:password@tcp(host:3306)/dbName%2Fname", dsn)
}

func TestPrepareDsnWithoutPasswordOrPort(t *testing.T) {
	driverDialect := &mysql_driver.MySQLDriverDialect{}

	properties := map[string]string{
		property_util.USER.Name:     "user",
		property_util.HOST.Name:     "host",
		property_util.DATABASE.Name: "dbName",
		property_util.NET.Name:      "tcp",
		property_util.PLUGINS.Name:  "test",
	}

	dsn := driverDialect.PrepareDsn(properties, nil)
	assert.Equal(t, "user@tcp(host)/dbName", dsn)
}

func TestMySQLErrorHandler_IsNetworkError(t *testing.T) {
	handler := mysql_driver.MySQLErrorHandler{}

	t.Run("substring network errors", func(t *testing.T) {
		for _, message := range mysql_driver.MySqlNetworkErrorMessages {
			assert.True(t, handler.IsNetworkError(errors.New(message)), "expected network error for: %s", message)
		}
	})

	t.Run("SQLSTATE 08 class is a network error", func(t *testing.T) {
		err := &mysql.MySQLError{SQLState: [5]byte{'0', '8', '0', '0', '6'}, Message: "conn"}
		assert.True(t, handler.IsNetworkError(err))
	})

	t.Run("caller cancellation is not a network error", func(t *testing.T) {
		assert.False(t, handler.IsNetworkError(context.Canceled))
		assert.False(t, handler.IsNetworkError(fmt.Errorf("query aborted: %w", context.Canceled)))
		assert.False(t, handler.IsNetworkError(context.DeadlineExceeded))
		assert.False(t, handler.IsNetworkError(fmt.Errorf("read timed out: %w", context.DeadlineExceeded)))
	})

	t.Run("driver.ErrBadConn is not a network error", func(t *testing.T) {
		assert.False(t, handler.IsNetworkError(driver.ErrBadConn))
		assert.False(t, handler.IsNetworkError(fmt.Errorf("wrapped: %w", driver.ErrBadConn)))
	})

	t.Run("mysql.ErrInvalidConn is a network error", func(t *testing.T) {
		// go-sql-driver/mysql returns ErrInvalidConn from readPacket/writePacket
		// on non-cancellation I/O failures — a genuine network failure signal.
		assert.True(t, handler.IsNetworkError(mysql.ErrInvalidConn))
		assert.True(t, handler.IsNetworkError(fmt.Errorf("wrapped: %w", mysql.ErrInvalidConn)))
	})

	t.Run("non-network errors", func(t *testing.T) {
		assert.False(t, handler.IsNetworkError(errors.New("duplicate entry")))
	})
}

func TestPrepareDsnWithoutHost(t *testing.T) {
	driverDialect := &mysql_driver.MySQLDriverDialect{}

	properties := map[string]string{
		property_util.USER.Name:     "user",
		property_util.PASSWORD.Name: "password",
		property_util.DATABASE.Name: "dbName",
		property_util.NET.Name:      "tcp",
		property_util.PLUGINS.Name:  "test",
	}

	dsn := driverDialect.PrepareDsn(properties, nil)
	assert.Equal(t, "user:password@tcp/dbName", dsn)
}
