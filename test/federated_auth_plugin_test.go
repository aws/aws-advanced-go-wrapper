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
	"awssql/plugins/federated_auth"
	"awssql/property_util"
	"awssql/region_util"
	"database/sql/driver"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var federatedAuthTestToken = "someToken"
var federatedAuthDbUser = "iamUser"
var federatedAuthHost = "pg.testdb.us-east-2.rds.amazonaws.com"
var federatedAuthIamHost = "pg.testdb.us-east-2.rds.amazonaws.com"
var federatedAuthRegion = region_util.US_EAST_1
var federatedAuthHostInfo1, _ = host_info_util.NewHostInfoBuilder().SetHost(federatedAuthHost).SetPort(1234).Build()
var federatedAuthHostInfo2, _ = host_info_util.NewHostInfoBuilder().SetHost("localhost").SetPort(1234).Build()
var mockIamTokenUtility = &MockIamTokenUtility{}
var credentialsProviderFactory = &MockCredentialsProviderFactory{}

func connectFunc() (driver.Conn, error) {
	return nil, nil
}

func setup(props map[string]string) *federated_auth.FederatedAuthPlugin {
	mockIamTokenUtility.Reset()
	credentialsProviderFactory.getAwsCredentialsProviderError = nil
	federated_auth.TokenCache.Clear()
	mockTargetDriver := &MockTargetDriver{}
	mockPluginManager := driver_infrastructure.PluginManager(plugin_helpers.NewPluginManagerImpl(mockTargetDriver, props, driver_infrastructure.ConnectionProviderManager{}))
	pluginServiceImpl, _ := plugin_helpers.NewPluginServiceImpl(mockPluginManager, driver_infrastructure.NewPgxDriverDialect(), props, pgTestDsn)
	mockPluginService := driver_infrastructure.PluginService(pluginServiceImpl)
	return federated_auth.NewFederatedAuthPlugin(mockPluginService, credentialsProviderFactory, mockIamTokenUtility)
}

func TestGetFederatedAuthPlugin(t *testing.T) {
	props := map[string]string{
		property_util.IDP_NAME.Name: "adfs",
	}
	pluginFactory := federated_auth.NewFederatedAuthPluginFactory()
	_, err := pluginFactory.GetInstance(&MockPluginServiceImpl{}, props)
	assert.Nil(t, err)

	props = map[string]string{
		property_util.IDP_NAME.Name: "",
	}
	pluginFactory = federated_auth.NewFederatedAuthPluginFactory()
	_, err = pluginFactory.GetInstance(&MockPluginServiceImpl{}, props)
	assert.Nil(t, err)

	props = map[string]string{
		property_util.IDP_NAME.Name: "any",
	}
	pluginFactory = federated_auth.NewFederatedAuthPluginFactory()
	_, err = pluginFactory.GetInstance(&MockPluginServiceImpl{}, props)
	assert.Equal(t, error_util.NewIllegalArgumentError(error_util.GetMessage("CredentialsProviderFactory.unsupportedIdp", "any")), err)
}

func TestCachedToken(t *testing.T) {
	props := map[string]string{
		property_util.DRIVER_PROTOCOL.Name: "postgresql",
		property_util.DB_USER.Name:         federatedAuthDbUser,
	}

	plugin := setup(props)
	key := "us-east-2:pg.testdb.us-east-2.rds.amazonaws.com:1234:iamUser"
	federated_auth.TokenCache.Put(key, federatedAuthTestToken, time.Millisecond*300000)
	_, _ = plugin.Connect(federatedAuthHostInfo1, props, true, connectFunc)

	assert.Equal(t, federatedAuthDbUser, props[property_util.USER.Name])
	assert.Equal(t, federatedAuthTestToken, props[property_util.PASSWORD.Name])
}

func TestExpiredCachedToken(t *testing.T) {
	props := map[string]string{
		property_util.DRIVER_PROTOCOL.Name: "postgresql",
		property_util.DB_USER.Name:         federatedAuthDbUser,
	}

	plugin := setup(props)
	key := "us-east-2:pg.testdb.us-east-2.rds.amazonaws.com:1234:iamUser"
	federated_auth.TokenCache.Put(key, federatedAuthTestToken, time.Nanosecond)
	_, _ = plugin.Connect(federatedAuthHostInfo1, props, true, connectFunc)

	assert.Equal(t, federatedAuthDbUser, props[property_util.USER.Name])
	assert.Equal(t, federatedAuthTestToken, props[property_util.PASSWORD.Name])
	assert.Equal(t, 1, mockIamTokenUtility.GenerateAuthenticationTokenCallCounter)
}

func TestNoCachedToken(t *testing.T) {
	props := map[string]string{
		property_util.DRIVER_PROTOCOL.Name: "postgresql",
		property_util.DB_USER.Name:         federatedAuthDbUser,
	}

	plugin := setup(props)
	_, _ = plugin.Connect(federatedAuthHostInfo1, props, true, connectFunc)

	assert.Equal(t, federatedAuthDbUser, props[property_util.USER.Name])
	assert.Equal(t, federatedAuthTestToken, props[property_util.PASSWORD.Name])
	assert.Equal(t, 1, mockIamTokenUtility.GenerateAuthenticationTokenCallCounter)
}

func TestSpecifiedIamHostPortRegion(t *testing.T) {
	expectedHost := "pg.testdb.us-west-2.rds.amazonaws.com"
	expectedPort := "9876"
	expectedRegion := "us-west-2"
	props := map[string]string{
		property_util.DRIVER_PROTOCOL.Name:  "postgresql",
		property_util.DB_USER.Name:          federatedAuthDbUser,
		property_util.IAM_HOST.Name:         expectedHost,
		property_util.IAM_DEFAULT_PORT.Name: expectedPort,
		property_util.IAM_REGION.Name:       expectedRegion,
	}

	plugin := setup(props)
	key := "us-west-2:pg.testdb.us-west-2.rds.amazonaws.com:" + expectedPort + ":iamUser"
	federated_auth.TokenCache.Put(key, federatedAuthTestToken, time.Millisecond*300000)
	_, _ = plugin.Connect(federatedAuthHostInfo1, props, true, connectFunc)

	assert.Equal(t, federatedAuthDbUser, props[property_util.USER.Name])
	assert.Equal(t, federatedAuthTestToken, props[property_util.PASSWORD.Name])
	assert.Equal(t, 0, mockIamTokenUtility.GenerateAuthenticationTokenCallCounter)
}

func TestIdpCredentialsFallback(t *testing.T) {
	expectedUser := "expectedUser"
	expectedPassword := "expectedPassword"
	props := map[string]string{
		property_util.DRIVER_PROTOCOL.Name: "postgresql",
		property_util.DB_USER.Name:         federatedAuthDbUser,
		property_util.USER.Name:            expectedUser,
		property_util.PASSWORD.Name:        expectedPassword,
	}

	plugin := setup(props)
	key := "us-east-2:pg.testdb.us-east-2.rds.amazonaws.com:1234:iamUser"
	federated_auth.TokenCache.Put(key, federatedAuthTestToken, time.Millisecond*300000)
	_, _ = plugin.Connect(federatedAuthHostInfo1, props, true, connectFunc)

	assert.Equal(t, federatedAuthDbUser, props[property_util.USER.Name])
	assert.Equal(t, federatedAuthTestToken, props[property_util.PASSWORD.Name])
	assert.Equal(t, 0, mockIamTokenUtility.GenerateAuthenticationTokenCallCounter)
	assert.Equal(t, expectedUser, props[property_util.IDP_USERNAME.Name])
	assert.Equal(t, expectedPassword, props[property_util.IDP_PASSWORD.Name])
}

func TestUsingIamHost(t *testing.T) {
	props := map[string]string{
		property_util.DRIVER_PROTOCOL.Name: "postgresql",
		property_util.DB_USER.Name:         federatedAuthDbUser,
		property_util.IAM_HOST.Name:        federatedAuthIamHost,
	}

	plugin := setup(props)
	_, _ = plugin.Connect(federatedAuthHostInfo1, props, true, connectFunc)

	assert.Equal(t, federatedAuthDbUser, props[property_util.USER.Name])
	assert.Equal(t, federatedAuthTestToken, props[property_util.PASSWORD.Name])
	assert.Equal(t, 1, mockIamTokenUtility.GenerateAuthenticationTokenCallCounter)
	assert.Equal(t, federatedAuthIamHost, mockIamTokenUtility.CapturedHost)
}

func TestInvalidRegionWithoutHost(t *testing.T) {
	props := map[string]string{
		property_util.DRIVER_PROTOCOL.Name: "postgresql",
		property_util.IAM_REGION.Name:      "invalid-region",
	}

	plugin := setup(props)
	conn, err := plugin.Connect(federatedAuthHostInfo2, props, true, connectFunc)
	assert.Nil(t, conn)
	assert.Equal(t, error_util.NewGenericAwsWrapperError(error_util.GetMessage("FederatedAuthPlugin.unableToDetermineRegion", property_util.IAM_REGION.Name)), err)

	props = map[string]string{
		property_util.DRIVER_PROTOCOL.Name: "postgresql",
		property_util.IAM_REGION.Name:      "",
	}

	plugin = setup(props)
	conn, err = plugin.Connect(federatedAuthHostInfo2, props, true, connectFunc)
	assert.Nil(t, conn)
	assert.Equal(t, error_util.NewGenericAwsWrapperError(error_util.GetMessage("FederatedAuthPlugin.unableToDetermineRegion", property_util.IAM_REGION.Name)), err)
}

func TestGenerateTokenFailure(t *testing.T) {
	props := map[string]string{
		property_util.DRIVER_PROTOCOL.Name: "postgresql",
		property_util.DB_USER.Name:         federatedAuthDbUser,
	}

	plugin := setup(props)
	mockIamTokenUtility.GenerateTokenError = errors.New("GenerateTokenError")
	_, err := plugin.Connect(federatedAuthHostInfo1, props, true, connectFunc)

	assert.Equal(t, 1, mockIamTokenUtility.GenerateAuthenticationTokenCallCounter)
	assert.Equal(t, errors.New("GenerateTokenError"), err)
}

func TestGetAwsCredentialsProviderError(t *testing.T) {
	props := map[string]string{
		property_util.DRIVER_PROTOCOL.Name: "postgresql",
		property_util.DB_USER.Name:         federatedAuthDbUser,
	}

	plugin := setup(props)
	credentialsProviderFactory.getAwsCredentialsProviderError = errors.New("getAwsCredentialsProviderError")
	_, err := plugin.Connect(federatedAuthHostInfo1, props, true, connectFunc)

	assert.Equal(t, 0, mockIamTokenUtility.GenerateAuthenticationTokenCallCounter)
	assert.Equal(t, errors.New("getAwsCredentialsProviderError"), err)
}
