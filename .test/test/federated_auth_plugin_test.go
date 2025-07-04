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
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-advanced-go-wrapper/awssql/driver_infrastructure"
	"github.com/aws/aws-advanced-go-wrapper/awssql/error_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/host_info_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/plugin_helpers"
	"github.com/aws/aws-advanced-go-wrapper/awssql/property_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/region_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils/telemetry"
	federated_auth "github.com/aws/aws-advanced-go-wrapper/federated-auth"
	pgx_driver "github.com/aws/aws-advanced-go-wrapper/pgx-driver"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

func setup(props map[string]string) *federated_auth.FederatedAuthPlugin {
	mockIamTokenUtility.Reset()
	credentialsProviderFactory.getAwsCredentialsProviderError = nil
	federated_auth.TokenCache.Clear()
	mockTargetDriver := &MockTargetDriver{}
	telemetryFactory, _ := telemetry.NewDefaultTelemetryFactory(props)
	mockPluginManager := plugin_helpers.NewPluginManagerImpl(mockTargetDriver, props, driver_infrastructure.ConnectionProviderManager{}, telemetryFactory)
	pluginServiceImpl, _ := plugin_helpers.NewPluginServiceImpl(mockPluginManager, pgx_driver.NewPgxDriverDialect(), props, pgTestDsn)
	mockPluginService := pluginServiceImpl
	federatedAuthPlugin, _ := federated_auth.NewFederatedAuthPlugin(mockPluginService, credentialsProviderFactory, mockIamTokenUtility)
	return federatedAuthPlugin
}

func TestFederatedAuthCachedToken(t *testing.T) {
	props := map[string]string{
		property_util.DRIVER_PROTOCOL.Name: "postgresql",
		property_util.DB_USER.Name:         federatedAuthDbUser,
		property_util.IDP_USERNAME.Name:    "username",
		property_util.IDP_PASSWORD.Name:    "password",
		property_util.IDP_ENDPOINT.Name:    "idpEndpoint",
		property_util.IAM_ROLE_ARN.Name:    "iamRoleArn",
		property_util.IAM_IDP_ARN.Name:     "iamIdpArn",
	}

	var resultProps map[string]string
	connectFunc := func(props map[string]string) (driver.Conn, error) {
		resultProps = props
		return &MockConn{throwError: true}, nil
	}

	plugin := setup(props)
	key := "us-east-2:pg.testdb.us-east-2.rds.amazonaws.com:1234:iamUser"
	federated_auth.TokenCache.Put(key, federatedAuthTestToken, time.Millisecond*300000)
	_, _ = plugin.Connect(federatedAuthHostInfo1, props, true, connectFunc)

	assert.Equal(t, federatedAuthDbUser, resultProps[property_util.USER.Name])
	assert.Equal(t, federatedAuthTestToken, resultProps[property_util.PASSWORD.Name])
}

func TestFederatedAuthConnectWithRetry(t *testing.T) {
	props := map[string]string{
		property_util.DRIVER_PROTOCOL.Name: "postgresql",
		property_util.DB_USER.Name:         federatedAuthDbUser,
		property_util.IDP_USERNAME.Name:    "username",
		property_util.IDP_PASSWORD.Name:    "password",
		property_util.IDP_ENDPOINT.Name:    "idpEndpoint",
		property_util.IAM_ROLE_ARN.Name:    "iamRoleArn",
		property_util.IAM_IDP_ARN.Name:     "iamIdpArn",
	}

	key := "us-east-2:pg.testdb.us-east-2.rds.amazonaws.com:1234:iamUser"
	plugin := setup(props)
	federated_auth.TokenCache.Put(key, "cachedToken", time.Minute)
	connAttempts := 0
	var resultProps map[string]string
	mockConnFunc := func(props map[string]string) (driver.Conn, error) {
		resultProps = props
		connAttempts++
		cachedToken, ok := federated_auth.TokenCache.Get(key)
		if ok && cachedToken == "cachedToken" {
			// Fails using cached token.
			return nil, errors.New(pgx_driver.AccessErrors[0])
		}
		// Succeeds on retry attempt with a new token.
		return &MockConn{}, nil
	}

	_, err := plugin.Connect(federatedAuthHostInfo1, props, true, mockConnFunc)

	assert.NoError(t, err)
	assert.Equal(t, federatedAuthDbUser, resultProps[property_util.USER.Name])
	assert.Equal(t, federatedAuthTestToken, resultProps[property_util.PASSWORD.Name])
	assert.Equal(t, 1, federated_auth.TokenCache.Size())
	assert.Equal(t, 2, connAttempts)
}

func TestFederatedAuthDoesNotRetryConnect(t *testing.T) {
	props := map[string]string{
		property_util.DRIVER_PROTOCOL.Name: "postgresql",
		property_util.DB_USER.Name:         federatedAuthDbUser,
		property_util.IDP_USERNAME.Name:    "username",
		property_util.IDP_PASSWORD.Name:    "password",
		property_util.IDP_ENDPOINT.Name:    "idpEndpoint",
		property_util.IAM_ROLE_ARN.Name:    "iamRoleArn",
		property_util.IAM_IDP_ARN.Name:     "iamIdpArn",
	}

	key := "us-east-2:pg.testdb.us-east-2.rds.amazonaws.com:1234:iamUser"
	plugin := setup(props)
	testErr := errors.New("test")
	connAttempts := 0
	var resultProps map[string]string
	mockConnFunc := func(props map[string]string) (driver.Conn, error) {
		resultProps = props
		connAttempts++
		cachedPassword, ok := federated_auth.TokenCache.Get(key)
		if ok && cachedPassword == "oldPassword" {
			// Fails with a non-login error when using cached password.
			return nil, errors.New(pgx_driver.NetworkErrors[0])
		}
		// Fails with a login error when using newly generated token.
		return nil, errors.New(pgx_driver.AccessErrors[0])
	}
	_, err := plugin.Connect(federatedAuthHostInfo1, props, false, mockConnFunc)

	// Should only generate a new token once, if it fails does not retry.
	require.Error(t, err)
	assert.Equal(t, pgx_driver.AccessErrors[0], err.Error())
	assert.Equal(t, federatedAuthDbUser, resultProps[property_util.USER.Name])
	assert.Equal(t, federatedAuthTestToken, resultProps[property_util.PASSWORD.Name])
	assert.Equal(t, 1, federated_auth.TokenCache.Size())
	assert.Equal(t, 1, connAttempts)

	federated_auth.TokenCache.Put(key, "oldPassword", time.Minute)
	connAttempts = 0
	_, err = plugin.Connect(federatedAuthHostInfo1, props, false, mockConnFunc)

	// Should not retry if the error is not a login error.
	assert.NotEqual(t, testErr, err)
	assert.Equal(t, "oldPassword", resultProps[property_util.PASSWORD.Name])
	assert.Equal(t, federatedAuthDbUser, resultProps[property_util.USER.Name])
	assert.Equal(t, 1, federated_auth.TokenCache.Size())
	assert.Equal(t, 1, connAttempts)
}

func TestFederatedAuthExpiredCachedToken(t *testing.T) {
	props := map[string]string{
		property_util.DRIVER_PROTOCOL.Name: "postgresql",
		property_util.DB_USER.Name:         federatedAuthDbUser,
		property_util.IDP_USERNAME.Name:    "username",
		property_util.IDP_PASSWORD.Name:    "password",
		property_util.IDP_ENDPOINT.Name:    "idpEndpoint",
		property_util.IAM_ROLE_ARN.Name:    "iamRoleArn",
		property_util.IAM_IDP_ARN.Name:     "iamIdpArn",
	}

	var resultProps map[string]string
	connectFunc := func(props map[string]string) (driver.Conn, error) {
		resultProps = props
		return &MockConn{throwError: true}, nil
	}

	plugin := setup(props)
	key := "us-east-2:pg.testdb.us-east-2.rds.amazonaws.com:1234:iamUser"
	federated_auth.TokenCache.Put(key, federatedAuthTestToken, time.Nanosecond)
	_, _ = plugin.Connect(federatedAuthHostInfo1, props, true, connectFunc)

	assert.Equal(t, federatedAuthDbUser, resultProps[property_util.USER.Name])
	assert.Equal(t, federatedAuthTestToken, resultProps[property_util.PASSWORD.Name])
	assert.Equal(t, 1, mockIamTokenUtility.GenerateAuthenticationTokenCallCounter)
}

func TestFederatedAuthNoCachedToken(t *testing.T) {
	props := map[string]string{
		property_util.DRIVER_PROTOCOL.Name: "postgresql",
		property_util.DB_USER.Name:         federatedAuthDbUser,
		property_util.IDP_USERNAME.Name:    "username",
		property_util.IDP_PASSWORD.Name:    "password",
		property_util.IDP_ENDPOINT.Name:    "idpEndpoint",
		property_util.IAM_ROLE_ARN.Name:    "iamRoleArn",
		property_util.IAM_IDP_ARN.Name:     "iamIdpArn",
	}

	var resultProps map[string]string
	connectFunc := func(props map[string]string) (driver.Conn, error) {
		resultProps = props
		return &MockConn{throwError: true}, nil
	}

	plugin := setup(props)
	_, _ = plugin.Connect(federatedAuthHostInfo1, props, true, connectFunc)

	assert.Equal(t, federatedAuthDbUser, resultProps[property_util.USER.Name])
	assert.Equal(t, federatedAuthTestToken, resultProps[property_util.PASSWORD.Name])
	assert.Equal(t, 1, mockIamTokenUtility.GenerateAuthenticationTokenCallCounter)
}

func TestFederatedAuthSpecifiedIamHostPortRegion(t *testing.T) {
	expectedHost := "pg.testdb.us-west-2.rds.amazonaws.com"
	expectedPort := "9876"
	expectedRegion := "us-west-2"
	props := map[string]string{
		property_util.DRIVER_PROTOCOL.Name:  "postgresql",
		property_util.DB_USER.Name:          federatedAuthDbUser,
		property_util.IDP_USERNAME.Name:     "username",
		property_util.IDP_PASSWORD.Name:     "password",
		property_util.IDP_ENDPOINT.Name:     "idpEndpoint",
		property_util.IAM_ROLE_ARN.Name:     "iamRoleArn",
		property_util.IAM_IDP_ARN.Name:      "iamIdpArn",
		property_util.IAM_HOST.Name:         expectedHost,
		property_util.IAM_DEFAULT_PORT.Name: expectedPort,
		property_util.IAM_REGION.Name:       expectedRegion,
	}

	var resultProps map[string]string
	connectFunc := func(props map[string]string) (driver.Conn, error) {
		resultProps = props
		return &MockConn{throwError: true}, nil
	}

	plugin := setup(props)
	key := "us-west-2:pg.testdb.us-west-2.rds.amazonaws.com:" + expectedPort + ":iamUser"
	federated_auth.TokenCache.Put(key, federatedAuthTestToken, time.Millisecond*300000)
	_, _ = plugin.Connect(federatedAuthHostInfo1, props, true, connectFunc)

	assert.Equal(t, federatedAuthDbUser, resultProps[property_util.USER.Name])
	assert.Equal(t, federatedAuthTestToken, resultProps[property_util.PASSWORD.Name])
	assert.Equal(t, 0, mockIamTokenUtility.GenerateAuthenticationTokenCallCounter)
}

func TestFederatedAuthIdpCredentialsFallback(t *testing.T) {
	expectedUser := "expectedUser"
	expectedPassword := "expectedPassword"
	props := map[string]string{
		property_util.DRIVER_PROTOCOL.Name: "postgresql",
		property_util.IDP_ENDPOINT.Name:    "idpEndpoint",
		property_util.IAM_ROLE_ARN.Name:    "iamRoleArn",
		property_util.IAM_IDP_ARN.Name:     "iamIdpArn",
		property_util.DB_USER.Name:         federatedAuthDbUser,
		property_util.USER.Name:            expectedUser,
		property_util.PASSWORD.Name:        expectedPassword,
	}

	var resultProps map[string]string
	connectFunc := func(props map[string]string) (driver.Conn, error) {
		resultProps = props
		return &MockConn{throwError: true}, nil
	}

	plugin := setup(props)
	key := "us-east-2:pg.testdb.us-east-2.rds.amazonaws.com:1234:iamUser"
	federated_auth.TokenCache.Put(key, federatedAuthTestToken, time.Millisecond*300000)
	_, _ = plugin.Connect(federatedAuthHostInfo1, props, true, connectFunc)

	assert.Equal(t, federatedAuthDbUser, resultProps[property_util.USER.Name])
	assert.Equal(t, federatedAuthTestToken, resultProps[property_util.PASSWORD.Name])
	assert.Equal(t, 0, mockIamTokenUtility.GenerateAuthenticationTokenCallCounter)
	assert.Equal(t, expectedUser, resultProps[property_util.IDP_USERNAME.Name])
	assert.Equal(t, expectedPassword, resultProps[property_util.IDP_PASSWORD.Name])
}

func TestFederatedAuthUsingIamHost(t *testing.T) {
	props := map[string]string{
		property_util.DRIVER_PROTOCOL.Name: "postgresql",
		property_util.IDP_USERNAME.Name:    "username",
		property_util.IDP_PASSWORD.Name:    "password",
		property_util.IDP_ENDPOINT.Name:    "idpEndpoint",
		property_util.IAM_ROLE_ARN.Name:    "iamRoleArn",
		property_util.IAM_IDP_ARN.Name:     "iamIdpArn",
		property_util.DB_USER.Name:         federatedAuthDbUser,
		property_util.IAM_HOST.Name:        federatedAuthIamHost,
	}

	var resultProps map[string]string
	connectFunc := func(props map[string]string) (driver.Conn, error) {
		resultProps = props
		return &MockConn{throwError: true}, nil
	}

	plugin := setup(props)
	_, _ = plugin.Connect(federatedAuthHostInfo1, props, true, connectFunc)

	assert.Equal(t, federatedAuthDbUser, resultProps[property_util.USER.Name])
	assert.Equal(t, federatedAuthTestToken, resultProps[property_util.PASSWORD.Name])
	assert.Equal(t, 1, mockIamTokenUtility.GenerateAuthenticationTokenCallCounter)
	assert.Equal(t, federatedAuthIamHost, mockIamTokenUtility.CapturedHost)
}

func TestFederatedAuthInvalidRegionWithoutHost(t *testing.T) {
	props := map[string]string{
		property_util.DRIVER_PROTOCOL.Name: "postgresql",
		property_util.IAM_REGION.Name:      "invalid-region",
		property_util.IDP_USERNAME.Name:    "username",
		property_util.IDP_PASSWORD.Name:    "password",
		property_util.IDP_ENDPOINT.Name:    "idpEndpoint",
		property_util.IAM_ROLE_ARN.Name:    "iamRoleArn",
		property_util.IAM_IDP_ARN.Name:     "iamIdpArn",
		property_util.DB_USER.Name:         federatedAuthDbUser,
	}

	connectFunc := func(props map[string]string) (driver.Conn, error) {
		return &MockConn{throwError: true}, nil
	}

	plugin := setup(props)
	conn, err := plugin.Connect(federatedAuthHostInfo2, props, true, connectFunc)
	assert.Nil(t, conn)
	assert.Equal(t, error_util.NewGenericAwsWrapperError(error_util.GetMessage("FederatedAuthPlugin.unableToDetermineRegion", property_util.IAM_REGION.Name)), err)

	props = map[string]string{
		property_util.DRIVER_PROTOCOL.Name: "postgresql",
		property_util.IAM_REGION.Name:      "",
		property_util.IDP_USERNAME.Name:    "username",
		property_util.IDP_PASSWORD.Name:    "password",
		property_util.IDP_ENDPOINT.Name:    "idpEndpoint",
		property_util.IAM_ROLE_ARN.Name:    "iamRoleArn",
		property_util.IAM_IDP_ARN.Name:     "iamIdpArn",
		property_util.DB_USER.Name:         federatedAuthDbUser,
	}

	plugin = setup(props)
	conn, err = plugin.Connect(federatedAuthHostInfo2, props, true, connectFunc)
	assert.Nil(t, conn)
	assert.Equal(t, error_util.NewGenericAwsWrapperError(error_util.GetMessage("FederatedAuthPlugin.unableToDetermineRegion", property_util.IAM_REGION.Name)), err)
}

func TestFederatedAuthGenerateTokenFailure(t *testing.T) {
	props := map[string]string{
		property_util.DRIVER_PROTOCOL.Name: "postgresql",
		property_util.IDP_USERNAME.Name:    "username",
		property_util.IDP_PASSWORD.Name:    "password",
		property_util.IDP_ENDPOINT.Name:    "idpEndpoint",
		property_util.IAM_ROLE_ARN.Name:    "iamRoleArn",
		property_util.IAM_IDP_ARN.Name:     "iamIdpArn",
		property_util.DB_USER.Name:         federatedAuthDbUser,
	}

	connectFunc := func(props map[string]string) (driver.Conn, error) {
		return &MockConn{throwError: true}, nil
	}

	plugin := setup(props)
	mockIamTokenUtility.GenerateTokenError = errors.New("GenerateTokenError")
	_, err := plugin.Connect(federatedAuthHostInfo1, props, true, connectFunc)

	assert.Equal(t, 1, mockIamTokenUtility.GenerateAuthenticationTokenCallCounter)
	assert.Equal(t, errors.New("GenerateTokenError"), err)
}

func TestFederatedAuthGetAwsCredentialsProviderError(t *testing.T) {
	props := map[string]string{
		property_util.DRIVER_PROTOCOL.Name: "postgresql",
		property_util.IDP_USERNAME.Name:    "username",
		property_util.IDP_PASSWORD.Name:    "password",
		property_util.IDP_ENDPOINT.Name:    "idpEndpoint",
		property_util.IAM_ROLE_ARN.Name:    "iamRoleArn",
		property_util.IAM_IDP_ARN.Name:     "iamIdpArn",
		property_util.DB_USER.Name:         federatedAuthDbUser,
	}

	connectFunc := func(props map[string]string) (driver.Conn, error) {
		return &MockConn{throwError: true}, nil
	}

	plugin := setup(props)
	credentialsProviderFactory.getAwsCredentialsProviderError = errors.New("getAwsCredentialsProviderError")
	_, err := plugin.Connect(federatedAuthHostInfo1, props, true, connectFunc)

	assert.Equal(t, 0, mockIamTokenUtility.GenerateAuthenticationTokenCallCounter)
	assert.Equal(t, errors.New("getAwsCredentialsProviderError"), err)
}

func TestFederatedMissingParams(t *testing.T) {
	missingParams := []string{property_util.IDP_USERNAME.Name, property_util.IDP_ENDPOINT.Name}
	props := map[string]string{
		property_util.DRIVER_PROTOCOL.Name: "postgresql",
		property_util.DB_USER.Name:         federatedAuthDbUser,
		property_util.IDP_PASSWORD.Name:    "password",
		property_util.IAM_ROLE_ARN.Name:    "iamRoleArn",
		property_util.IAM_IDP_ARN.Name:     "iamIdpArn",
	}

	connectFunc := func(props map[string]string) (driver.Conn, error) {
		return &MockConn{throwError: true}, nil
	}

	plugin := setup(props)
	key := "us-east-2:pg.testdb.us-east-2.rds.amazonaws.com:1234:iamUser"
	federated_auth.TokenCache.Put(key, federatedAuthTestToken, time.Millisecond*300000)
	_, err := plugin.Connect(federatedAuthHostInfo1, props, true, connectFunc)

	assert.Error(t, err)
	assert.Equal(t, error_util.GetMessage("AuthHelpers.missingRequiredParameters",
		"adfs", missingParams), err.Error())
}
