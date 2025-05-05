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
	"awssql/driver_infrastructure"
	"awssql/error_util"
	"awssql/host_info_util"
	"awssql/plugin_helpers"
	"awssql/plugins/aws_secrets_manager"
	"awssql/property_util"
	"awssql/utils"
	"database/sql/driver"
	"fmt"
	"testing"

	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
)

func beforeAwsSecretsManagerConnectionPluginTests(props map[string]string) driver_infrastructure.PluginService {
	aws_secrets_manager.SecretsCache.Clear()
	mockTargetDriver := &MockTargetDriver{}
	mockPluginManager := driver_infrastructure.PluginManager(plugin_helpers.NewPluginManagerImpl(mockTargetDriver, props, driver_infrastructure.ConnectionProviderManager{}))
	pluginServiceImpl, _ := plugin_helpers.NewPluginServiceImpl(mockPluginManager, driver_infrastructure.NewMySQLDriverDialect(), props, mysqlTestDsn)

	mockPluginService := driver_infrastructure.PluginService(pluginServiceImpl)
	return mockPluginService
}

func TestAwsSecretsManagerConnectionPluginConnect(t *testing.T) {
	hostInfo, err := host_info_util.NewHostInfoBuilder().SetHost("database-test-name.cluster-XYZ.us-east-2.rds.amazonaws.com").SetPort(1234).Build()
	assert.Nil(t, err)
	mockConnFunc := func() (driver.Conn, error) { return &MockConn{nil, nil, nil, nil, true, 0}, nil }

	props := map[string]string{
		property_util.SECRETS_MANAGER_REGION.Name:    "us-west-2",
		property_util.SECRETS_MANAGER_SECRET_ID.Name: "myId",
	}
	mockPluginService := beforeAwsSecretsManagerConnectionPluginTests(props)

	awsSecretsManagerConnectionPlugin, _ := aws_secrets_manager.NewAwsSecretsManagerPlugin(mockPluginService, props, NewMockAwsSecretsManagerClient)

	_, err = awsSecretsManagerConnectionPlugin.Connect(hostInfo, props, false, mockConnFunc)

	assert.Nil(t, err)
	assert.Equal(t, "testuser", props[property_util.USER.Name])
	assert.Equal(t, "testpassword", props[property_util.PASSWORD.Name])
}

func TestAwsSecretsManagerConnectionPluginForceConnect(t *testing.T) {
	hostInfo, err := host_info_util.NewHostInfoBuilder().SetHost("database-test-name.cluster-XYZ.us-east-2.rds.amazonaws.com").SetPort(1234).Build()
	assert.Nil(t, err)
	mockConnFunc := func() (driver.Conn, error) { return &MockConn{nil, nil, nil, nil, true, 0}, nil }

	props := map[string]string{
		property_util.SECRETS_MANAGER_REGION.Name:    "us-west-2",
		property_util.SECRETS_MANAGER_SECRET_ID.Name: "myId",
	}
	mockPluginService := beforeAwsSecretsManagerConnectionPluginTests(props)

	awsSecretsManagerConnectionPlugin, _ := aws_secrets_manager.NewAwsSecretsManagerPlugin(mockPluginService, props, NewMockAwsSecretsManagerClient)

	_, err = awsSecretsManagerConnectionPlugin.ForceConnect(hostInfo, props, false, mockConnFunc)

	assert.Nil(t, err)
	assert.Equal(t, "testuser", props[property_util.USER.Name])
	assert.Equal(t, "testpassword", props[property_util.PASSWORD.Name])
}

func TestAwsSecretsManagerConnectionPluginProps(t *testing.T) {
	hostInfo, err := host_info_util.NewHostInfoBuilder().SetHost("database-test-name.cluster-XYZ.us-east-2.rds.amazonaws.com").SetPort(1234).Build()
	assert.Nil(t, err)
	mockConnFunc := func() (driver.Conn, error) { return &MockConn{nil, nil, nil, nil, true, 0}, nil }

	props := map[string]string{
		property_util.SECRETS_MANAGER_REGION.Name:    "us-west-2",
		property_util.SECRETS_MANAGER_SECRET_ID.Name: "myId",
		property_util.SECRETS_MANAGER_ENDPOINT.Name:  "https://someEndpoint.com",
	}
	mockPluginService := beforeAwsSecretsManagerConnectionPluginTests(props)

	awsSecretsManagerConnectionPlugin, _ := aws_secrets_manager.NewAwsSecretsManagerPlugin(mockPluginService, props, NewMockAwsSecretsManagerClient)

	_, err = awsSecretsManagerConnectionPlugin.ForceConnect(hostInfo, props, false, mockConnFunc)

	assert.Nil(t, err)
	assert.Equal(t, "testuser", props[property_util.USER.Name])
	assert.Equal(t, "testpassword", props[property_util.PASSWORD.Name])
	assert.Equal(t, "https://someEndpoint.com", props[property_util.SECRETS_MANAGER_ENDPOINT.Name])
}

func TestAwsSecretsManagerConnectionPluginInvalidRegion(t *testing.T) {
	props := map[string]string{
		property_util.SECRETS_MANAGER_SECRET_ID.Name: "myId",
	}
	mockPluginService := beforeAwsSecretsManagerConnectionPluginTests(props)

	_, err := aws_secrets_manager.NewAwsSecretsManagerPlugin(mockPluginService, props, NewMockAwsSecretsManagerClient)

	assert.Equal(t,
		error_util.GetMessage("AwsSecretsManagerConnectionPlugin.unableToDetermineRegion", property_util.SECRETS_MANAGER_REGION.Name),
		err.Error())
}

func TestAwsSecretsManagerConnectionPluginValidRegionThroughArn(t *testing.T) {
	hostInfo, err := host_info_util.NewHostInfoBuilder().SetHost("database-test-name.cluster-XYZ.us-east-2.rds.amazonaws.com").SetPort(1234).Build()
	assert.Nil(t, err)
	mockConnFunc := func() (driver.Conn, error) { return &MockConn{nil, nil, nil, nil, true, 0}, nil }

	props := map[string]string{
		property_util.SECRETS_MANAGER_SECRET_ID.Name: "arn:aws:secretsmanager:us-west-2:account-id:secret:default",
	}
	mockPluginService := beforeAwsSecretsManagerConnectionPluginTests(props)

	awsSecretsManagerConnectionPlugin, _ := aws_secrets_manager.NewAwsSecretsManagerPlugin(mockPluginService, props, NewMockAwsSecretsManagerClient)

	_, err = awsSecretsManagerConnectionPlugin.ForceConnect(hostInfo, props, false, mockConnFunc)

	assert.Nil(t, err)
	assert.Equal(t, "testuser", props[property_util.USER.Name])
	assert.Equal(t, "testpassword", props[property_util.PASSWORD.Name])
}

func TestAwsSecretsManagerConnectionPluginInvalidEndpoint(t *testing.T) {
	props := map[string]string{
		property_util.SECRETS_MANAGER_SECRET_ID.Name: "arn:aws:secretsmanager:us-west-2:account-id:secret:default",
		property_util.SECRETS_MANAGER_ENDPOINT.Name:  "NotAValidEndpoi nt?-!=",
	}
	mockPluginService := beforeAwsSecretsManagerConnectionPluginTests(props)

	_, err := aws_secrets_manager.NewAwsSecretsManagerPlugin(mockPluginService, props, NewMockAwsSecretsManagerClient)

	assert.Equal(t,
		error_util.GetMessage("AwsSecretsManagerConnectionPlugin.endpointOverrideMisconfigured", "NotAValidEndpoi nt?-!="),
		err.Error())
}

func TestAwsSecretsManagerConnectionPluginCacheSize1(t *testing.T) {
	// assert cache is of size 0.
	assert.Equal(t, 0, getSizeOfSyncMap(&aws_secrets_manager.SecretsCache))

	hostInfo, err := host_info_util.NewHostInfoBuilder().SetHost("database-test-name.cluster-XYZ.us-east-2.rds.amazonaws.com").SetPort(1234).Build()
	assert.Nil(t, err)
	mockConnFunc := func() (driver.Conn, error) { return &MockConn{nil, nil, nil, nil, true, 0}, nil }

	props := map[string]string{
		property_util.SECRETS_MANAGER_REGION.Name:    "us-west-2",
		property_util.SECRETS_MANAGER_SECRET_ID.Name: "myId",
	}
	mockPluginService := beforeAwsSecretsManagerConnectionPluginTests(props)

	awsSecretsManagerConnectionPlugin, _ := aws_secrets_manager.NewAwsSecretsManagerPlugin(mockPluginService, props, NewMockAwsSecretsManagerClient)

	_, err = awsSecretsManagerConnectionPlugin.ForceConnect(hostInfo, props, false, mockConnFunc)

	assert.Nil(t, err)
	assert.Equal(t, "testuser", props[property_util.USER.Name])
	assert.Equal(t, "testpassword", props[property_util.PASSWORD.Name])
	assert.Equal(t, 1, getSizeOfSyncMap(&aws_secrets_manager.SecretsCache))
}

func TestAwsSecretsManagerConnectionPluginConnectingUsingCache(t *testing.T) {
	hostInfo, err := host_info_util.NewHostInfoBuilder().SetHost("database-test-name.cluster-XYZ.us-east-2.rds.amazonaws.com").SetPort(1234).Build()
	assert.Nil(t, err)
	mockConnFunc := func() (driver.Conn, error) { return &MockConn{nil, nil, nil, nil, true, 0}, nil }
	secretId := "myId"
	region := "us-west-2"
	cachedUsername := "cachedUsername"
	cachedPassword := "cachedPassword"

	props := map[string]string{
		property_util.SECRETS_MANAGER_REGION.Name:    region,
		property_util.SECRETS_MANAGER_SECRET_ID.Name: secretId,
	}
	mockPluginService := beforeAwsSecretsManagerConnectionPluginTests(props)

	cacheKey := fmt.Sprintf("%s:%s", secretId, region)
	awsRdsSecrets := aws_secrets_manager.AwsRdsSecrets{
		Username: cachedUsername,
		Password: cachedPassword,
	}
	aws_secrets_manager.SecretsCache.Store(cacheKey, awsRdsSecrets)
	assert.Equal(t, 1, getSizeOfSyncMap(&aws_secrets_manager.SecretsCache))

	awsSecretsManagerConnectionPlugin, _ := aws_secrets_manager.NewAwsSecretsManagerPlugin(mockPluginService, props, NewMockAwsSecretsManagerClient)

	_, err = awsSecretsManagerConnectionPlugin.Connect(hostInfo, props, false, mockConnFunc)

	assert.Nil(t, err)
	assert.Equal(t, cachedUsername, props[property_util.USER.Name])
	assert.Equal(t, cachedPassword, props[property_util.PASSWORD.Name])
	assert.Equal(t, 1, getSizeOfSyncMap(&aws_secrets_manager.SecretsCache))
}

func TestAwsSecretsManagerConnectionPluginMultipleConnectionsCache(t *testing.T) {
	hostInfo, err := host_info_util.NewHostInfoBuilder().SetHost("database-test-name.cluster-XYZ.us-east-2.rds.amazonaws.com").SetPort(1234).Build()
	assert.Nil(t, err)
	mockConnFunc := func() (driver.Conn, error) { return &MockConn{nil, nil, nil, nil, true, 0}, nil }
	secretIds := [4]string{"id1", "id1", "id3", "id4"}
	region := [4]string{"us-west-2", "us-west-1", "us-west-2", "us-west-2"}

	var props [4]map[string]string
	var mockPluginServices [4]driver_infrastructure.PluginService

	// setup props map and mock plugin services
	for i := 0; i < 4; i++ {
		props[i] = map[string]string{
			property_util.SECRETS_MANAGER_REGION.Name:    region[i],
			property_util.SECRETS_MANAGER_SECRET_ID.Name: secretIds[i],
		}
		mockPluginServices[i] = beforeAwsSecretsManagerConnectionPluginTests(props[i])
	}

	assert.Equal(t, 0, getSizeOfSyncMap(&aws_secrets_manager.SecretsCache))

	for i := 0; i < 4; i++ {
		awsSecretsManagerConnectionPlugin, _ := aws_secrets_manager.NewAwsSecretsManagerPlugin(mockPluginServices[i], props[i], NewMockAwsSecretsManagerClient)
		_, err = awsSecretsManagerConnectionPlugin.Connect(hostInfo, props[i], false, mockConnFunc)
		assert.Nil(t, err)
	}
	assert.Equal(t, 4, getSizeOfSyncMap(&aws_secrets_manager.SecretsCache))
}

func TestAwsSecretsManagerConnectionPluginLoginError(t *testing.T) {
	hostInfo, err := host_info_util.NewHostInfoBuilder().SetHost("database-test-name.cluster-XYZ.us-east-2.rds.amazonaws.com").SetPort(1234).Build()
	assert.Nil(t, err)
	mockLoginError := &mysql.MySQLError{SQLState: [5]byte(([]byte(driver_infrastructure.SqlStateAccessError))[:5])}
	mockConnFunc := func() (driver.Conn, error) { return nil, mockLoginError }

	props := map[string]string{
		property_util.SECRETS_MANAGER_REGION.Name:    "us-west-2",
		property_util.SECRETS_MANAGER_SECRET_ID.Name: "myId",
		property_util.DRIVER_PROTOCOL.Name:           "mysql",
	}

	mockPluginService := beforeAwsSecretsManagerConnectionPluginTests(props)
	awsSecretsManagerConnectionPlugin, err := aws_secrets_manager.NewAwsSecretsManagerPlugin(mockPluginService, props, NewMockAwsSecretsManagerClient)
	assert.Nil(t, err)
	connection, err := awsSecretsManagerConnectionPlugin.Connect(hostInfo, props, false, mockConnFunc)

	assert.Nil(t, connection)
	assert.NotNil(t, err)
	assert.Equal(t, mockLoginError, err)
	assert.Equal(t, 1, utils.LengthOfSyncMap(&aws_secrets_manager.SecretsCache))
	assert.Equal(t, "testuser", props[property_util.USER.Name])
	assert.Equal(t, "testpassword", props[property_util.PASSWORD.Name])
}
