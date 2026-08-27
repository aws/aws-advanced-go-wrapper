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
	"database/sql"
	"reflect"
	"strconv"
	"testing"

	"github.com/aws/aws-advanced-go-wrapper/.test/test_framework/container/test_utils"
	awsDriver "github.com/aws/aws-advanced-go-wrapper/awssql/v2/driver"
	bun_pg_driver "github.com/aws/aws-advanced-go-wrapper/bun-pg-driver"
	mysql_driver "github.com/aws/aws-advanced-go-wrapper/mysql-driver"
	pgx_driver "github.com/aws/aws-advanced-go-wrapper/pgx-driver"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// expectedDriverTypes names the concrete wrapper and underlying driver types for
// each target driver, so a run cannot claim to exercise one driver while opening
// connections through another.
var expectedDriverTypes = map[test_utils.TargetDriver]struct {
	wrapper          string
	underlying       string
	registrationName string
}{
	test_utils.PGX_DRIVER: {
		wrapper:          "*pgx_driver.PgxDriver",
		underlying:       "*stdlib.Driver",
		registrationName: pgx_driver.PGX_DRIVER_REGISTRATION_NAME,
	},
	test_utils.BUN_PG: {
		wrapper: "*bun_pg_driver.BunPgDriver",
		// Not pgdriver.Driver itself: this module registers pgdriver behind a wrapper that
		// adds driver.NamedValueChecker, without which no query carrying an argument runs.
		underlying:       "bun_pg_driver.BunPgUnderlyingDriver",
		registrationName: bun_pg_driver.BUN_PG_DRIVER_REGISTRATION_NAME,
	},
	test_utils.MYSQL_DRIVER: {
		wrapper:          "*mysql_driver.MySQLDriver",
		underlying:       "*mysql.MySQLDriver",
		registrationName: mysql_driver.MYSQL_DRIVER_REGISTRATION_NAME,
	},
}

// TestTargetDriverIsWiredUp checks that the driver the tests below open their
// connections through is the one the test environment selected, so that a run
// against the wrong driver is reported as such rather than as a connectivity
// result for a driver nobody asked about.
//
// Note that it reads the expected driver from the same test environment the driver
// seam reads it from, so it would pass even if the selection never reached the
// container.
//
// Unlike every other test in this file there is no BasicSetup: this reaches no
// database, and running a cluster health check here would let a cluster problem
// masquerade as a driver selection problem.
func TestTargetDriverIsWiredUp(t *testing.T) {
	environment, err := test_utils.GetCurrentTestEnvironment()
	require.NoError(t, err)
	engine := environment.Info().Request.Engine

	targetDriver, err := test_utils.TargetDriverForEngine(engine)
	require.NoError(t, err)

	expected, ok := expectedDriverTypes[targetDriver]
	require.True(t, ok, "no expected driver types recorded for target driver %s", targetDriver)

	assert.Equal(t, engine, targetDriver.Engine())
	assert.Equal(t, expected.wrapper, reflect.TypeOf(test_utils.NewWrapperDriver(engine)).String())

	// The underlying driver is what actually speaks to the database, so this is
	// the assertion that distinguishes bun-pg from pgx rather than merely
	// distinguishing their wrappers.
	underlying := awsDriver.GetUnderlyingDriver(expected.registrationName)
	require.NotNil(t, underlying, "underlying driver %s is not registered", expected.registrationName)
	assert.Equal(t, expected.underlying, reflect.TypeOf(underlying).String())

	assert.Contains(t, sql.Drivers(), targetDriver.WrapperDriverCode())
}

func TestBasicConnectivityWrapper(t *testing.T) {
	defer test_utils.BasicCleanupAfterBasicSetup(t)()

	environment, err := test_utils.GetCurrentTestEnvironment()
	assert.Nil(t, err)
	test_utils.SkipIfNoInstanceIdentity(t, environment)
	dsn := test_utils.GetDsn(environment, map[string]string{"plugins": "none"})
	db, err := test_utils.OpenDb(environment.Info().Request.Engine, dsn)
	assert.Nil(t, err)
	assert.NotNil(t, db)
	defer func() { _ = db.Close() }()

	instanceId, err := test_utils.ExecuteInstanceQueryDB(environment.Info().Request.Engine, environment.Info().Request.Deployment, db)
	assert.Nil(t, err)
	assert.NotZero(t, instanceId)
}

func TestBasicConnectivityWrapperProxy(t *testing.T) {
	defer test_utils.BasicCleanupAfterBasicSetup(t)()

	environment, err := test_utils.GetCurrentTestEnvironment()
	assert.Nil(t, err)
	test_utils.SkipIfNoInstanceIdentity(t, environment)
	test_utils.RequireTestEnvironmentFeatures(t, environment.Info().Request.Features, test_utils.NETWORK_OUTAGES_ENABLED)
	test_utils.SkipForTestEnvironmentFeatures(t, environment.Info().Request.Features, test_utils.LIMITLESS_DEPLOYMENT)
	dsn := test_utils.GetDsn(environment, map[string]string{
		"host":                       environment.Info().ProxyDatabaseInfo.Instances[0].Host(),
		"port":                       strconv.Itoa(environment.Info().ProxyDatabaseInfo.InstanceEndpointPort),
		"clusterInstanceHostPattern": "?." + environment.Info().ProxyDatabaseInfo.InstanceEndpointSuffix,
		"plugins":                    "none",
	})
	db, err := test_utils.OpenDb(environment.Info().Request.Engine, dsn)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Ping()
	assert.Nil(t, err)

	test_utils.DisableAllConnectivity()
	instanceId, err := test_utils.ExecuteInstanceQueryDbWithTimeout(environment.Info().Request.Engine, environment.Info().Request.Deployment, db, 10)
	assert.NotNil(t, err)
	assert.Zero(t, instanceId)
	defer func() { _ = db.Close() }()

	test_utils.EnableAllConnectivity(true)
	instanceId, err2 := test_utils.ExecuteInstanceQueryDbWithTimeout(environment.Info().Request.Engine, environment.Info().Request.Deployment, db, 10)
	assert.Nil(t, err2)
	assert.NotZero(t, instanceId)
}

func TestBasicConnectivityFailoverClusterEndpoint(t *testing.T) {
	defer test_utils.BasicCleanupAfterBasicSetup(t)()

	environment, err := test_utils.GetCurrentTestEnvironment()
	assert.Nil(t, err)
	test_utils.SkipIfNoInstanceIdentity(t, environment)
	test_utils.SkipForTestEnvironmentFeatures(t, environment.Info().Request.Features, test_utils.LIMITLESS_DEPLOYMENT)

	dsn := test_utils.GetDsn(environment, map[string]string{})
	db, err := test_utils.OpenDb(environment.Info().Request.Engine, dsn)
	assert.Nil(t, err)
	assert.NotNil(t, db)
	defer func() { _ = db.Close() }()

	instanceId, err := test_utils.ExecuteInstanceQueryDB(environment.Info().Request.Engine, environment.Info().Request.Deployment, db)
	assert.Nil(t, err)
	assert.NotZero(t, instanceId)
}

func TestBasicConnectivityFailoverInstanceEndpoint(t *testing.T) {
	defer test_utils.BasicCleanupAfterBasicSetup(t)()

	environment, err := test_utils.GetCurrentTestEnvironment()
	assert.Nil(t, err)
	test_utils.SkipIfNoInstanceIdentity(t, environment)
	test_utils.SkipForTestEnvironmentFeatures(t, environment.Info().Request.Features, test_utils.LIMITLESS_DEPLOYMENT)

	dsn := test_utils.GetDsn(environment, map[string]string{
		"host": environment.Info().DatabaseInfo.Instances[0].Host(),
	})
	db, err := test_utils.OpenDb(environment.Info().Request.Engine, dsn)
	assert.Nil(t, err)
	assert.NotNil(t, db)
	defer func() { _ = db.Close() }()

	instanceId, err := test_utils.ExecuteInstanceQueryDB(environment.Info().Request.Engine, environment.Info().Request.Deployment, db)
	assert.Nil(t, err)
	assert.NotZero(t, instanceId)
}

func TestBasicConnectivityFailoverReaderEndpoint(t *testing.T) {
	defer test_utils.BasicCleanupAfterBasicSetup(t)()

	environment, err := test_utils.GetCurrentTestEnvironment()
	assert.Nil(t, err)
	test_utils.SkipIfNoInstanceIdentity(t, environment)
	test_utils.SkipForTestEnvironmentFeatures(t, environment.Info().Request.Features, test_utils.LIMITLESS_DEPLOYMENT)

	dsn := test_utils.GetDsn(environment, map[string]string{
		"host": environment.Info().DatabaseInfo.ClusterReadOnlyEndpoint,
	})
	db, err := test_utils.OpenDb(environment.Info().Request.Engine, dsn)
	assert.Nil(t, err)
	assert.NotNil(t, db)
	defer func() { _ = db.Close() }()

	instanceId, err := test_utils.ExecuteInstanceQueryDB(environment.Info().Request.Engine, environment.Info().Request.Deployment, db)
	assert.Nil(t, err)
	assert.NotZero(t, instanceId)
}
