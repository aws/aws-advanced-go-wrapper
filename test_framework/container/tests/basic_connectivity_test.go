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
	awsDriver "awssql/driver"
	"awssql/test_framework/container/test_utils"
	"database/sql"
	"fmt"
	"log/slog"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func basicSetup(name string) (*test_utils.AuroraTestUtility, error) {
	slog.Info(fmt.Sprintf("Test started: %s.", name))
	env, err := test_utils.GetCurrentTestEnvironment()
	if err != nil {
		return nil, err
	}
	auroraTestUtility := test_utils.NewAuroraTestUtility(env.Info().Region)
	test_utils.EnableAllConnectivity()
	err = test_utils.VerifyClusterStatus()
	if err != nil {
		return nil, err
	}
	return auroraTestUtility, nil
}

func basicCleanup(name string) {
	awsDriver.ClearCaches()
	slog.Info(fmt.Sprintf("Test finished: %s.", name))
}

func TestBasicConnectivityWrapper(t *testing.T) {
	defer test_utils.BasicCleanupAfterBasicSetup(t)

	environment, err := test_utils.GetCurrentTestEnvironment()
	assert.Nil(t, err)
	dsn := test_utils.GetDsn(environment, map[string]string{"plugins": "none"})
	db, err := sql.Open("awssql", dsn)
	assert.Nil(t, err)
	assert.NotNil(t, db)
	defer db.Close()

	instanceId, err := test_utils.ExecuteInstanceQueryDB(environment.Info().Request.Engine, environment.Info().Request.Deployment, db)
	assert.Nil(t, err)
	assert.NotZero(t, instanceId)
}

func TestBasicConnectivityWrapperProxy(t *testing.T) {
	defer test_utils.BasicCleanupAfterBasicSetup(t)

	environment, err := test_utils.GetCurrentTestEnvironment()
	assert.Nil(t, err)
	dsn := test_utils.GetDsn(environment, map[string]string{
		"host":                       environment.Info().ProxyDatabaseInfo.Instances[0].Host(),
		"port":                       strconv.Itoa(environment.Info().ProxyDatabaseInfo.InstanceEndpointPort()),
		"clusterInstanceHostPattern": "?." + environment.Info().ProxyDatabaseInfo.InstanceEndpointSuffix(),
		"plugins":                    "none",
	})
	db, err := sql.Open("awssql", dsn)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Ping()
	assert.Nil(t, err)

	test_utils.DisableAllConnectivity()
	instanceId, err := test_utils.ExecuteInstanceQueryDB(environment.Info().Request.Engine, environment.Info().Request.Deployment, db)
	assert.NotNil(t, err)
	assert.Zero(t, instanceId)
	defer db.Close()

	test_utils.EnableAllConnectivity()
	instanceId, err2 := test_utils.ExecuteInstanceQueryDB(environment.Info().Request.Engine, environment.Info().Request.Deployment, db)
	assert.Nil(t, err2)
	assert.NotZero(t, instanceId)
}

func TestBasicConnectivityFailoverClusterEndpoint(t *testing.T) {
	defer test_utils.BasicCleanupAfterBasicSetup(t)

	environment, err := test_utils.GetCurrentTestEnvironment()
	assert.Nil(t, err)
	dsn := test_utils.GetDsn(environment, map[string]string{})
	db, err := sql.Open("awssql", dsn)
	assert.Nil(t, err)
	assert.NotNil(t, db)
	defer db.Close()

	instanceId, err := test_utils.ExecuteInstanceQueryDB(environment.Info().Request.Engine, environment.Info().Request.Deployment, db)
	assert.Nil(t, err)
	assert.NotZero(t, instanceId)
}

func TestBasicConnectivityFailoverInstanceEndpoint(t *testing.T) {
	defer test_utils.BasicCleanupAfterBasicSetup(t)

	environment, err := test_utils.GetCurrentTestEnvironment()
	assert.Nil(t, err)
	dsn := test_utils.GetDsn(environment, map[string]string{
		"host": environment.Info().DatabaseInfo.Instances[0].Host(),
	})
	db, err := sql.Open("awssql", dsn)
	assert.Nil(t, err)
	assert.NotNil(t, db)
	defer db.Close()

	instanceId, err := test_utils.ExecuteInstanceQueryDB(environment.Info().Request.Engine, environment.Info().Request.Deployment, db)
	assert.Nil(t, err)
	assert.NotZero(t, instanceId)
}

func TestBasicConnectivityFailoverReaderEndpoint(t *testing.T) {
	defer test_utils.BasicCleanupAfterBasicSetup(t)

	environment, err := test_utils.GetCurrentTestEnvironment()
	assert.Nil(t, err)
	dsn := test_utils.GetDsn(environment, map[string]string{
		"host": environment.Info().DatabaseInfo.ClusterReadOnlyEndpoint,
	})
	db, err := sql.Open("awssql", dsn)
	assert.Nil(t, err)
	assert.NotNil(t, db)
	defer db.Close()

	instanceId, err := test_utils.ExecuteInstanceQueryDB(environment.Info().Request.Engine, environment.Info().Request.Deployment, db)
	assert.Nil(t, err)
	assert.NotZero(t, instanceId)
}
