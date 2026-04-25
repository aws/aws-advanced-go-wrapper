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

package dialect_manager_test

import (
	"database/sql"
	"strings"
	"testing"

	"github.com/aws/aws-advanced-go-wrapper/.test/test"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/driver_infrastructure"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/property_util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var pgTestDsn = "postgres://someUser:somePassword@mydatabase.cluster-xyz.us-east-2.rds.amazonaws.com:5432/pgx_test?sslmode=disable&foo=bar"

func TestGetDialectFromConnectionParameter(t *testing.T) {
	dialectManager := driver_infrastructure.DialectManager{}

	props := test.MakeMapFromKeysAndVals("databaseDialect", "mysql")
	dialect, err := dialectManager.GetDialect(pgTestDsn, props)
	assert.Nil(t, err)
	assert.Equal(t, dialect, driver_infrastructure.KnownDialectsByCode["mysql"])

	props = test.MakeMapFromKeysAndVals("databaseDialect", "incorrect-code")
	_, err = dialectManager.GetDialect(pgTestDsn, props)
	require.NotNil(t, err)
	assert.True(t, strings.Contains(err.Error(), "Unknown database dialect code"))
}

// Verifies that GetDialect resolves dialects based on driver protocol
// without requiring the wrapper driver to be registered.
func TestGetDialectWithoutDriverRegistered(t *testing.T) {
	tests := []struct {
		name       string
		dsn        string
		driverName string
	}{
		{
			name:       "Pg",
			dsn:        pgTestDsn,
			driverName: driver_infrastructure.AWS_PGX_DRIVER_CODE,
		},
		{
			name:       "MySql",
			dsn:        "someUser:somePassword@tcp(mydatabase.cluster-xyz.us-east-2.rds.amazonaws.com:3306)/myDatabase",
			driverName: driver_infrastructure.AWS_MYSQL_DRIVER_CODE,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Precondition: verify the driver is not registered.
			_, err := sql.Open(tc.driverName, tc.dsn)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "unknown driver")

			// GetDialect should succeed even without the driver registered.
			props, parseErr := property_util.ParseDsn(tc.dsn)
			require.NoError(t, parseErr)

			dialectManager := driver_infrastructure.DialectManager{}
			_, err = dialectManager.GetDialect(tc.dsn, props)
			assert.NoError(t, err)
			driver_infrastructure.ClearCaches()
		})
	}
}
