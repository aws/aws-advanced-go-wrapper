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
	"database/sql/driver"
	"fmt"
	"testing"
	"time"

	aws_secrets_manager "github.com/aws/aws-advanced-go-wrapper/aws-secrets-manager"
	"github.com/aws/aws-advanced-go-wrapper/awssql/driver_infrastructure"
	"github.com/aws/aws-advanced-go-wrapper/awssql/error_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/host_info_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/plugin_helpers"
	"github.com/aws/aws-advanced-go-wrapper/awssql/property_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils/telemetry"
	mysql_driver "github.com/aws/aws-advanced-go-wrapper/mysql-driver"

	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
)

func beforeAwsSecretsManagerConnectionPluginTests(props *utils.RWMap[string, string]) driver_infrastructure.PluginService {
	aws_secrets_manager.SecretsCache.Clear()
	mockTargetDriver := &MockTargetDriver{}
	telemetryFactory, _ := telemetry.NewDefaultTelemetryFactory(props)
	mockPluginManager := plugin_helpers.NewPluginManagerImpl(mockTargetDriver, props, driver_infrastructure.ConnectionProviderManager{}, telemetryFactory)
	pluginServiceImpl, _ := plugin_helpers.NewPluginServiceImpl(mockPluginManager, mysql_driver.NewMySQLDriverDialect(), props, mysqlTestDsn)
	mockPluginService := pluginServiceImpl
	return mockPluginService
}

func TestAwsSecretsManagerConnectionPluginConnect(t *testing.T) {
	hostInfo, err := host_info_util.NewHostInfoBuilder().SetHost("database-test-name.cluster-XYZ.us-east-2.rds.amazonaws.com").SetPort(1234).Build()
	assert.Nil(t, err)
	var resultProps *utils.RWMap[string, string]
	mockConnFunc := func(props *utils.RWMap[string, string]) (driver.Conn, error) {
		resultProps = props
		return &MockConn{throwError: true}, nil
	}

	props := MakeMapFromKeysAndVals(
		property_util.SECRETS_MANAGER_REGION.Name, "us-west-2",
		property_util.SECRETS_MANAGER_SECRET_ID.Name, "myId",
		property_util.DRIVER_PROTOCOL.Name, "mysql",
	)
	mockPluginService := beforeAwsSecretsManagerConnectionPluginTests(props)

	awsSecretsManagerConnectionPlugin, _ := aws_secrets_manager.NewAwsSecretsManagerPlugin(mockPluginService, props, NewMockAwsSecretsManagerClient)

	_, err = awsSecretsManagerConnectionPlugin.Connect(hostInfo, props, false, mockConnFunc)

	assert.Nil(t, err)
	assert.Equal(t, "testuser", property_util.USER.Get(resultProps))
	assert.Equal(t, "testpassword", property_util.PASSWORD.Get(resultProps))
}

func TestAwsSecretsManagerConnectionPluginForceConnect(t *testing.T) {
	hostInfo, err := host_info_util.NewHostInfoBuilder().SetHost("database-test-name.cluster-XYZ.us-east-2.rds.amazonaws.com").SetPort(1234).Build()
	assert.Nil(t, err)
	var resultProps *utils.RWMap[string, string]
	mockConnFunc := func(props *utils.RWMap[string, string]) (driver.Conn, error) {
		resultProps = props
		return &MockConn{throwError: true}, nil
	}

	props := MakeMapFromKeysAndVals(
		property_util.SECRETS_MANAGER_REGION.Name, "us-west-2",
		property_util.SECRETS_MANAGER_SECRET_ID.Name, "myId",
		property_util.DRIVER_PROTOCOL.Name, "mysql",
	)
	mockPluginService := beforeAwsSecretsManagerConnectionPluginTests(props)

	awsSecretsManagerConnectionPlugin, _ := aws_secrets_manager.NewAwsSecretsManagerPlugin(mockPluginService, props, NewMockAwsSecretsManagerClient)

	_, err = awsSecretsManagerConnectionPlugin.ForceConnect(hostInfo, props, false, mockConnFunc)

	assert.Nil(t, err)
	assert.Equal(t, "testuser", property_util.USER.Get(resultProps))
	assert.Equal(t, "testpassword", property_util.PASSWORD.Get(resultProps))
}

func TestAwsSecretsManagerConnectionPluginProps(t *testing.T) {
	hostInfo, err := host_info_util.NewHostInfoBuilder().SetHost("database-test-name.cluster-XYZ.us-east-2.rds.amazonaws.com").SetPort(1234).Build()
	assert.Nil(t, err)
	var resultProps *utils.RWMap[string, string]
	mockConnFunc := func(props *utils.RWMap[string, string]) (driver.Conn, error) {
		resultProps = props
		return &MockConn{throwError: true}, nil
	}

	props := MakeMapFromKeysAndVals(
		property_util.SECRETS_MANAGER_REGION.Name, "us-west-2",
		property_util.SECRETS_MANAGER_SECRET_ID.Name, "myId",
		property_util.SECRETS_MANAGER_ENDPOINT.Name, "https://someEndpoint.com",
		property_util.DRIVER_PROTOCOL.Name, "mysql",
	)
	mockPluginService := beforeAwsSecretsManagerConnectionPluginTests(props)

	awsSecretsManagerConnectionPlugin, _ := aws_secrets_manager.NewAwsSecretsManagerPlugin(mockPluginService, props, NewMockAwsSecretsManagerClient)

	_, err = awsSecretsManagerConnectionPlugin.ForceConnect(hostInfo, props, false, mockConnFunc)

	assert.Nil(t, err)
	assert.Equal(t, "testuser", property_util.USER.Get(resultProps))
	assert.Equal(t, "testpassword", property_util.PASSWORD.Get(resultProps))
	assert.Equal(t, "https://someEndpoint.com", property_util.SECRETS_MANAGER_ENDPOINT.Get(resultProps))
}

func TestAwsSecretsManagerConnectionPluginMissingSecretId(t *testing.T) {
	props := MakeMapFromKeysAndVals(
		property_util.DRIVER_PROTOCOL.Name, "mysql",
	)
	mockPluginService := beforeAwsSecretsManagerConnectionPluginTests(props)

	_, err := aws_secrets_manager.NewAwsSecretsManagerPlugin(mockPluginService, props, NewMockAwsSecretsManagerClient)

	assert.Equal(t,
		error_util.GetMessage("AwsSecretsManagerConnectionPlugin.secretIdMissing", property_util.SECRETS_MANAGER_SECRET_ID.Name),
		err.Error())
}

func TestAwsSecretsManagerConnectionPluginInvalidRegion(t *testing.T) {
	props := MakeMapFromKeysAndVals(
		property_util.SECRETS_MANAGER_SECRET_ID.Name, "myId",
		property_util.SECRETS_MANAGER_REGION.Name, "invalidRegion",
		property_util.DRIVER_PROTOCOL.Name, "mysql",
	)
	mockPluginService := beforeAwsSecretsManagerConnectionPluginTests(props)

	_, err := aws_secrets_manager.NewAwsSecretsManagerPlugin(mockPluginService, props, NewMockAwsSecretsManagerClient)

	assert.Equal(t,
		error_util.GetMessage("AwsSecretsManagerConnectionPlugin.invalidRegion", "invalidRegion"),
		err.Error())
}

func TestAwsSecretsManagerConnectionPluginValidRegion(t *testing.T) {
	props := MakeMapFromKeysAndVals(
		property_util.SECRETS_MANAGER_SECRET_ID.Name, "myId",
		property_util.SECRETS_MANAGER_REGION.Name, "us-west-2",
		property_util.DRIVER_PROTOCOL.Name, "mysql",
	)
	mockPluginService := beforeAwsSecretsManagerConnectionPluginTests(props)

	_, err := aws_secrets_manager.NewAwsSecretsManagerPlugin(mockPluginService, props, NewMockAwsSecretsManagerClient)

	assert.NoError(t, err)
}

func TestAwsSecretsManagerConnectionPluginValidIdDefaultRegion(t *testing.T) {
	props := MakeMapFromKeysAndVals(
		property_util.SECRETS_MANAGER_SECRET_ID.Name, "myId",
		property_util.DRIVER_PROTOCOL.Name, "mysql",
	)
	mockPluginService := beforeAwsSecretsManagerConnectionPluginTests(props)

	_, err := aws_secrets_manager.NewAwsSecretsManagerPlugin(mockPluginService, props, NewMockAwsSecretsManagerClient)

	assert.NoError(t, err)
}

func TestAwsSecretsManagerConnectionPluginValidRegionThroughArn(t *testing.T) {
	hostInfo, err := host_info_util.NewHostInfoBuilder().SetHost("database-test-name.cluster-XYZ.us-east-2.rds.amazonaws.com").SetPort(1234).Build()
	assert.Nil(t, err)
	var resultProps *utils.RWMap[string, string]
	mockConnFunc := func(props *utils.RWMap[string, string]) (driver.Conn, error) {
		resultProps = props
		return &MockConn{throwError: true}, nil
	}

	props := MakeMapFromKeysAndVals(
		property_util.SECRETS_MANAGER_SECRET_ID.Name, "arn:aws:secretsmanager:us-west-2:account-id:secret:default",
		property_util.DRIVER_PROTOCOL.Name, "mysql",
	)
	mockPluginService := beforeAwsSecretsManagerConnectionPluginTests(props)

	awsSecretsManagerConnectionPlugin, _ := aws_secrets_manager.NewAwsSecretsManagerPlugin(mockPluginService, props, NewMockAwsSecretsManagerClient)

	_, err = awsSecretsManagerConnectionPlugin.ForceConnect(hostInfo, props, false, mockConnFunc)

	assert.Nil(t, err)
	assert.Equal(t, "testuser", property_util.USER.Get(resultProps))
	assert.Equal(t, "testpassword", property_util.PASSWORD.Get(resultProps))
}

func TestAwsSecretsManagerConnectionPluginInvalidEndpoint(t *testing.T) {
	props := MakeMapFromKeysAndVals(
		property_util.SECRETS_MANAGER_SECRET_ID.Name, "arn:aws:secretsmanager:us-west-2:account-id:secret:default",
		property_util.SECRETS_MANAGER_ENDPOINT.Name, "NotAValidEndpoi nt?-!=",
		property_util.DRIVER_PROTOCOL.Name, "mysql",
	)
	mockPluginService := beforeAwsSecretsManagerConnectionPluginTests(props)

	_, err := aws_secrets_manager.NewAwsSecretsManagerPlugin(mockPluginService, props, NewMockAwsSecretsManagerClient)

	assert.Equal(t,
		error_util.GetMessage("AwsSecretsManagerConnectionPlugin.endpointOverrideMisconfigured", "NotAValidEndpoi nt?-!="),
		err.Error())
}

func TestAwsSecretsManagerConnectionPluginCacheSize1(t *testing.T) {
	// assert cache is of size 0.
	assert.Equal(t, 0, aws_secrets_manager.SecretsCache.Size())

	hostInfo, err := host_info_util.NewHostInfoBuilder().SetHost("database-test-name.cluster-XYZ.us-east-2.rds.amazonaws.com").SetPort(1234).Build()
	assert.Nil(t, err)
	var resultProps *utils.RWMap[string, string]
	mockConnFunc := func(props *utils.RWMap[string, string]) (driver.Conn, error) {
		resultProps = props
		return &MockConn{throwError: true}, nil
	}

	props := MakeMapFromKeysAndVals(
		property_util.SECRETS_MANAGER_REGION.Name, "us-west-2",
		property_util.SECRETS_MANAGER_SECRET_ID.Name, "myId",
		property_util.DRIVER_PROTOCOL.Name, "mysql",
	)
	mockPluginService := beforeAwsSecretsManagerConnectionPluginTests(props)

	awsSecretsManagerConnectionPlugin, _ := aws_secrets_manager.NewAwsSecretsManagerPlugin(mockPluginService, props, NewMockAwsSecretsManagerClient)

	_, err = awsSecretsManagerConnectionPlugin.ForceConnect(hostInfo, props, false, mockConnFunc)

	assert.Nil(t, err)
	assert.Equal(t, "testuser", property_util.USER.Get(resultProps))
	assert.Equal(t, "testpassword", property_util.PASSWORD.Get(resultProps))
	assert.Equal(t, 1, aws_secrets_manager.SecretsCache.Size())
}

func TestAwsSecretsManagerConnectionPluginUsingExpiredSecret(t *testing.T) {
	hostInfo, err := host_info_util.NewHostInfoBuilder().SetHost("database-test-name.cluster-XYZ.us-east-2.rds.amazonaws.com").SetPort(1234).Build()
	assert.Nil(t, err)
	var resultProps *utils.RWMap[string, string]
	mockConnFunc := func(props *utils.RWMap[string, string]) (driver.Conn, error) {
		resultProps = props
		return &MockConn{throwError: true}, nil
	}
	secretId := "myId"
	region := "us-west-2"
	cachedUsername := "cachedUsername"
	cachedPassword := "cachedPassword"

	props := MakeMapFromKeysAndVals(
		property_util.SECRETS_MANAGER_REGION.Name, region,
		property_util.SECRETS_MANAGER_SECRET_ID.Name, secretId,
		property_util.DRIVER_PROTOCOL.Name, "mysql",
	)
	mockPluginService := beforeAwsSecretsManagerConnectionPluginTests(props)

	cacheKey := fmt.Sprintf("%s:%s", secretId, region)
	awsRdsSecrets := aws_secrets_manager.AwsRdsSecrets{
		Username: cachedUsername,
		Password: cachedPassword,
	}
	aws_secrets_manager.SecretsCache.Put(cacheKey, awsRdsSecrets, time.Nanosecond)
	assert.Equal(t, 1, aws_secrets_manager.SecretsCache.Size())

	awsSecretsManagerConnectionPlugin, _ := aws_secrets_manager.NewAwsSecretsManagerPlugin(mockPluginService, props, NewMockAwsSecretsManagerClient)

	_, err = awsSecretsManagerConnectionPlugin.Connect(hostInfo, props, false, mockConnFunc)

	assert.Nil(t, err)
	assert.Equal(t, "testuser", property_util.USER.Get(resultProps))
	assert.Equal(t, "testpassword", property_util.PASSWORD.Get(resultProps))
	assert.Equal(t, 1, aws_secrets_manager.SecretsCache.Size())
}

func TestAwsSecretsManagerConnectionPluginConnectingUsingCache(t *testing.T) {
	hostInfo, err := host_info_util.NewHostInfoBuilder().SetHost("database-test-name.cluster-XYZ.us-east-2.rds.amazonaws.com").SetPort(1234).Build()
	assert.Nil(t, err)
	var resultProps *utils.RWMap[string, string]
	mockConnFunc := func(props *utils.RWMap[string, string]) (driver.Conn, error) {
		resultProps = props
		return &MockConn{throwError: true}, nil
	}
	secretId := "myId"
	region := "us-west-2"
	cachedUsername := "cachedUsername"
	cachedPassword := "cachedPassword"

	props := MakeMapFromKeysAndVals(
		property_util.SECRETS_MANAGER_REGION.Name, region,
		property_util.SECRETS_MANAGER_SECRET_ID.Name, secretId,
		property_util.DRIVER_PROTOCOL.Name, "mysql",
	)
	mockPluginService := beforeAwsSecretsManagerConnectionPluginTests(props)

	cacheKey := fmt.Sprintf("%s:%s", secretId, region)
	awsRdsSecrets := aws_secrets_manager.AwsRdsSecrets{
		Username: cachedUsername,
		Password: cachedPassword,
	}
	aws_secrets_manager.SecretsCache.Put(cacheKey, awsRdsSecrets, time.Minute)
	assert.Equal(t, 1, aws_secrets_manager.SecretsCache.Size())

	awsSecretsManagerConnectionPlugin, _ := aws_secrets_manager.NewAwsSecretsManagerPlugin(mockPluginService, props, NewMockAwsSecretsManagerClient)

	_, err = awsSecretsManagerConnectionPlugin.Connect(hostInfo, props, false, mockConnFunc)

	assert.Nil(t, err)
	assert.Equal(t, cachedUsername, property_util.USER.Get(resultProps))
	assert.Equal(t, cachedPassword, property_util.PASSWORD.Get(resultProps))
	assert.Equal(t, 1, aws_secrets_manager.SecretsCache.Size())
}

func TestAwsSecretsManagerConnectionPluginMultipleConnectionsCache(t *testing.T) {
	hostInfo, err := host_info_util.NewHostInfoBuilder().SetHost("database-test-name.cluster-XYZ.us-east-2.rds.amazonaws.com").SetPort(1234).Build()
	assert.Nil(t, err)
	mockConnFunc := func(props *utils.RWMap[string, string]) (driver.Conn, error) { return &MockConn{throwError: true}, nil }
	secretIds := [4]string{"id1", "id1", "id3", "id4"}
	region := [4]string{"us-west-2", "us-west-1", "us-west-2", "us-west-2"}

	var props [4]*utils.RWMap[string, string]
	var mockPluginServices [4]driver_infrastructure.PluginService

	// setup props map and mock plugin services
	for i := 0; i < 4; i++ {
		props[i] = MakeMapFromKeysAndVals(
			property_util.SECRETS_MANAGER_REGION.Name, region[i],
			property_util.SECRETS_MANAGER_SECRET_ID.Name, secretIds[i],
			property_util.DRIVER_PROTOCOL.Name, "mysql",
		)
		mockPluginServices[i] = beforeAwsSecretsManagerConnectionPluginTests(props[i])
	}

	assert.Equal(t, 0, aws_secrets_manager.SecretsCache.Size())

	for i := 0; i < 4; i++ {
		awsSecretsManagerConnectionPlugin, _ := aws_secrets_manager.NewAwsSecretsManagerPlugin(mockPluginServices[i], props[i], NewMockAwsSecretsManagerClient)
		_, err = awsSecretsManagerConnectionPlugin.Connect(hostInfo, props[i], false, mockConnFunc)
		assert.Nil(t, err)
	}
	assert.Equal(t, 4, aws_secrets_manager.SecretsCache.Size())
}

func TestAwsSecretsManagerConnectionPluginLoginError(t *testing.T) {
	hostInfo, err := host_info_util.NewHostInfoBuilder().SetHost("database-test-name.cluster-XYZ.us-east-2.rds.amazonaws.com").SetPort(1234).Build()
	assert.Nil(t, err)
	mockLoginError := &mysql.MySQLError{SQLState: [5]byte(([]byte(mysql_driver.SqlStateAccessError))[:5])}
	var resultProps *utils.RWMap[string, string]
	mockConnFunc := func(props *utils.RWMap[string, string]) (driver.Conn, error) {
		resultProps = props
		return nil, mockLoginError
	}

	props := MakeMapFromKeysAndVals(
		property_util.SECRETS_MANAGER_REGION.Name, "us-west-2",
		property_util.SECRETS_MANAGER_SECRET_ID.Name, "myId",
		property_util.DRIVER_PROTOCOL.Name, "mysql",
	)

	mockPluginService := beforeAwsSecretsManagerConnectionPluginTests(props)
	awsSecretsManagerConnectionPlugin, err := aws_secrets_manager.NewAwsSecretsManagerPlugin(mockPluginService, props, NewMockAwsSecretsManagerClient)
	assert.Nil(t, err)
	connection, err := awsSecretsManagerConnectionPlugin.Connect(hostInfo, props, false, mockConnFunc)

	assert.Nil(t, connection)
	assert.NotNil(t, err)
	assert.Equal(t, mockLoginError, err)
	assert.Equal(t, 1, aws_secrets_manager.SecretsCache.Size())
	assert.Equal(t, "testuser", property_util.USER.Get(resultProps))
	assert.Equal(t, "testpassword", property_util.PASSWORD.Get(resultProps))
}
