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
	"strings"
	"testing"

	"github.com/aws/aws-advanced-go-wrapper/.test/test"
	"github.com/aws/aws-advanced-go-wrapper/awssql/driver_infrastructure"
	"github.com/aws/aws-advanced-go-wrapper/awssql/error_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/property_util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var mysqlTestDsn = "someUser:somePassword@tcp(mydatabase.cluster-xyz.us-east-2.rds.amazonaws.com:3306)/myDatabase?foo=bar&pop=snap"
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

func TestGetDialectUnregisteredDriverPgx(t *testing.T) {
	findRegisteredDriver := func(dialectCode string) bool {
		return false
	}
	dialectManager := driver_infrastructure.DialectManager{FindRegisteredDriver: findRegisteredDriver}

	props := test.MakeMapFromKeysAndVals(
		property_util.DRIVER_PROTOCOL.Name, property_util.PGX_DRIVER_PROTOCOL,
	)
	dialect, err := dialectManager.GetDialect(pgTestDsn, props)
	assert.Nil(t, dialect)
	assert.Equal(t, error_util.NewGenericAwsWrapperError(error_util.GetMessage("DatabaseDialectManager.missingWrapperDriver", driver_infrastructure.AWS_PGX_DRIVER_CODE)), err)
	driver_infrastructure.ClearCaches()
}

func TestGetDialectUnregisteredDriverMysql(t *testing.T) {
	findRegisteredDriver := func(dialectCode string) bool {
		return false
	}
	dialectManager := driver_infrastructure.DialectManager{FindRegisteredDriver: findRegisteredDriver}

	props := test.MakeMapFromKeysAndVals(
		property_util.DRIVER_PROTOCOL.Name, property_util.MYSQL_DRIVER_PROTOCOL,
	)
	dialect, err := dialectManager.GetDialect(mysqlTestDsn, props)
	assert.Nil(t, dialect)
	assert.Equal(t, err, error_util.NewGenericAwsWrapperError(error_util.GetMessage("DatabaseDialectManager.missingWrapperDriver", driver_infrastructure.AWS_MYSQL_DRIVER_CODE)))
	driver_infrastructure.ClearCaches()
}
