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
	"strconv"
	"testing"
	"time"

	auth_helpers "github.com/aws/aws-advanced-go-wrapper/auth-helpers"
	"github.com/aws/aws-advanced-go-wrapper/awssql/driver_infrastructure"
	"github.com/aws/aws-advanced-go-wrapper/awssql/error_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/host_info_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/plugin_helpers"
	"github.com/aws/aws-advanced-go-wrapper/awssql/property_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/region_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils/telemetry"
	"github.com/aws/aws-advanced-go-wrapper/okta"
	pgx_driver "github.com/aws/aws-advanced-go-wrapper/pgx-driver"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newOktaAuthPluginTest(
	props *utils.RWMap[string, string]) (
	*MockCredentialsProviderFactory,
	*MockIamTokenUtility,
	*okta.OktaAuthPlugin,
	driver_infrastructure.PluginService) {
	okta.OktaTokenCache.Clear()
	mockTargetDriver := &MockTargetDriver{}
	telemetryFactory, _ := telemetry.NewDefaultTelemetryFactory(props)
	mockPluginManager := plugin_helpers.NewPluginManagerImpl(mockTargetDriver, props, driver_infrastructure.ConnectionProviderManager{}, telemetryFactory)
	pluginServiceImpl, _ := plugin_helpers.NewPluginServiceImpl(mockPluginManager, pgx_driver.NewPgxDriverDialect(), props, pgTestDsn)
	mockPluginService := pluginServiceImpl
	iamTokenUtility := &MockIamTokenUtility{}
	mockCredProviderFactory := &MockCredentialsProviderFactory{}
	oktaPlugin, _ := okta.NewOktaAuthPlugin(mockPluginService, mockCredProviderFactory, iamTokenUtility)
	return mockCredProviderFactory, iamTokenUtility, oktaPlugin, pluginServiceImpl
}

func TestGetOktaAuthPlugin(t *testing.T) {
	props := MakeMapFromKeysAndVals(property_util.DRIVER_PROTOCOL.Name, "postgresql")
	_, _, _, pluginService := newOktaAuthPluginTest(props)
	pluginFactory := okta.NewOktaAuthPluginFactory()
	_, err := pluginFactory.GetInstance(pluginService, props)
	assert.NoError(t, err)
}

func TestOktaAuthPluginConnect(t *testing.T) {
	hostInfo, err := host_info_util.NewHostInfoBuilder().SetHost("database-test-name.cluster-XYZ.us-east-2.rds.amazonaws.com").SetPort(1234).Build()
	assert.NoError(t, err)
	var resultProps *utils.RWMap[string, string]
	mockConnFunc := func(props *utils.RWMap[string, string]) (driver.Conn, error) {
		resultProps = props
		return &MockConn{throwError: true}, nil
	}

	props := MakeMapFromKeysAndVals(
		property_util.DRIVER_PROTOCOL.Name, "postgresql",
		property_util.DB_USER.Name, "jane_doe",
		property_util.IDP_USERNAME.Name, "username",
		property_util.IDP_PASSWORD.Name, "password",
		property_util.IDP_ENDPOINT.Name, "idpEndpoint",
		property_util.IAM_ROLE_ARN.Name, "iamRoleArn",
		property_util.IAM_IDP_ARN.Name, "iamIdpArn",
		property_util.APP_ID.Name, "appId",
	)
	_, _, oktaAuthConnectionPlugin, _ := newOktaAuthPluginTest(props)

	_, err = oktaAuthConnectionPlugin.Connect(hostInfo, props, false, mockConnFunc)
	assert.NoError(t, err)
	assert.Equal(t, "someToken", property_util.PASSWORD.Get(resultProps))
	assert.Equal(t, "jane_doe", property_util.USER.Get(resultProps))
}

func TestOktaAuthPluginForceConnect(t *testing.T) {
	hostInfo, err := host_info_util.NewHostInfoBuilder().SetHost("database-test-name.cluster-XYZ.us-east-2.rds.amazonaws.com").SetPort(1234).Build()
	assert.NoError(t, err)
	var resultProps *utils.RWMap[string, string]
	mockConnFunc := func(props *utils.RWMap[string, string]) (driver.Conn, error) {
		resultProps = props
		return &MockConn{throwError: true}, nil
	}

	props := MakeMapFromKeysAndVals(
		property_util.DRIVER_PROTOCOL.Name, "postgresql",
		property_util.DB_USER.Name, "jane_doe",
		property_util.IDP_USERNAME.Name, "username",
		property_util.IDP_PASSWORD.Name, "password",
		property_util.IDP_ENDPOINT.Name, "idpEndpoint",
		property_util.IAM_ROLE_ARN.Name, "iamRoleArn",
		property_util.IAM_IDP_ARN.Name, "iamIdpArn",
		property_util.APP_ID.Name, "appId",
	)
	_, _, oktaAuthConnectionPlugin, _ := newOktaAuthPluginTest(props)

	_, err = oktaAuthConnectionPlugin.ForceConnect(hostInfo, props, false, mockConnFunc)
	assert.NoError(t, err)
	assert.Equal(t, "someToken", property_util.PASSWORD.Get(resultProps))
	assert.Equal(t, "jane_doe", property_util.USER.Get(resultProps))
}

func TestOktaAuthPluginInvalidRegion(t *testing.T) {
	hostInfo, err := host_info_util.NewHostInfoBuilder().SetHost("database-test-name.cluster-XYZ.invalid-region.rds.amazonaws.com").SetPort(1234).Build()
	assert.NoError(t, err)
	mockConnFunc := func(props *utils.RWMap[string, string]) (driver.Conn, error) { return &MockConn{throwError: true}, nil }

	props := MakeMapFromKeysAndVals(
		property_util.DRIVER_PROTOCOL.Name, "postgresql",
		property_util.DB_USER.Name, "jane_doe",
		property_util.IDP_USERNAME.Name, "username",
		property_util.IDP_PASSWORD.Name, "password",
		property_util.IDP_ENDPOINT.Name, "idpEndpoint",
		property_util.IAM_ROLE_ARN.Name, "iamRoleArn",
		property_util.IAM_IDP_ARN.Name, "iamIdpArn",
		property_util.APP_ID.Name, "appId",
	)
	_, _, oktaAuthConnectionPlugin, _ := newOktaAuthPluginTest(props)

	_, err = oktaAuthConnectionPlugin.Connect(hostInfo, props, false, mockConnFunc)
	assert.Equal(t, error_util.GetMessage("OktaAuthPlugin.unableToDetermineRegion", property_util.IAM_REGION.Name), err.Error())
}

func TestOktaAuthPluginValidRegionThroughIam(t *testing.T) {
	hostInfo, err := host_info_util.NewHostInfoBuilder().SetHost("database-test-name.cluster-XYZ.invalid-region.rds.amazonaws.com").SetPort(1234).Build()
	assert.NoError(t, err)
	var resultProps *utils.RWMap[string, string]
	mockConnFunc := func(props *utils.RWMap[string, string]) (driver.Conn, error) {
		resultProps = props
		return &MockConn{throwError: true}, nil
	}

	props := MakeMapFromKeysAndVals(
		property_util.DRIVER_PROTOCOL.Name, "postgresql",
		property_util.DB_USER.Name, "jane_doe",
		property_util.IDP_USERNAME.Name, "username",
		property_util.IDP_PASSWORD.Name, "password",
		property_util.IDP_ENDPOINT.Name, "idpEndpoint",
		property_util.IAM_ROLE_ARN.Name, "iamRoleArn",
		property_util.IAM_IDP_ARN.Name, "iamIdpArn",
		property_util.APP_ID.Name, "appId",
		property_util.IAM_REGION.Name, "us-west-2",
	)
	_, mockIamToken, oktaAuthConnectionPlugin, _ := newOktaAuthPluginTest(props)

	_, err = oktaAuthConnectionPlugin.Connect(hostInfo, props, false, mockConnFunc)
	assert.NoError(t, err)
	assert.Equal(t, mockIamToken.GetMockTokenValue(), property_util.PASSWORD.Get(resultProps))
	assert.Equal(t, "jane_doe", property_util.USER.Get(resultProps))
}

func TestOktaAuthPluginCachedToken(t *testing.T) {
	host := "database-test-name.cluster-XYZ.us-east-2.rds.amazonaws.com"
	port := 1234
	hostInfo, err := host_info_util.NewHostInfoBuilder().SetHost(host).SetPort(port).Build()
	assert.NoError(t, err)
	var resultProps *utils.RWMap[string, string]
	mockConnFunc := func(props *utils.RWMap[string, string]) (driver.Conn, error) {
		resultProps = props
		return &MockConn{throwError: true}, nil
	}

	props := MakeMapFromKeysAndVals(
		property_util.DRIVER_PROTOCOL.Name, "postgresql",
		property_util.DB_USER.Name, "jane_doe",
		property_util.IDP_USERNAME.Name, "username",
		property_util.IDP_PASSWORD.Name, "password",
		property_util.IDP_ENDPOINT.Name, "idpEndpoint",
		property_util.IAM_ROLE_ARN.Name, "iamRoleArn",
		property_util.IAM_IDP_ARN.Name, "iamIdpArn",
		property_util.APP_ID.Name, "appId",
	)
	_, _, oktaAuthConnectionPlugin, _ := newOktaAuthPluginTest(props)
	cacheKey := auth_helpers.GetCacheKey(
		property_util.GetVerifiedWrapperPropertyValue[string](props, property_util.DB_USER),
		host,
		port,
		"us-east-2")

	okta.OktaTokenCache.Put(cacheKey, "cachedPassword", time.Millisecond*3000)

	_, err = oktaAuthConnectionPlugin.Connect(hostInfo, props, false, mockConnFunc)
	assert.NoError(t, err)
	assert.Equal(t, "cachedPassword", property_util.PASSWORD.Get(resultProps))
	assert.Equal(t, "jane_doe", property_util.USER.Get(resultProps))
	assert.Equal(t, 1, okta.OktaTokenCache.Size())
}

func TestOktaAuthPluginConnectWithRetry(t *testing.T) {
	host := "database-test-name.cluster-XYZ.us-east-2.rds.amazonaws.com"
	port := 1234
	hostInfo, err := host_info_util.NewHostInfoBuilder().SetHost(host).SetPort(port).Build()
	assert.NoError(t, err)
	props := MakeMapFromKeysAndVals(
		property_util.DRIVER_PROTOCOL.Name, "postgresql",
		property_util.DB_USER.Name, "jane_doe",
		property_util.IDP_USERNAME.Name, "username",
		property_util.IDP_PASSWORD.Name, "password",
		property_util.IDP_ENDPOINT.Name, "idpEndpoint",
		property_util.IAM_ROLE_ARN.Name, "iamRoleArn",
		property_util.IAM_IDP_ARN.Name, "iamIdpArn",
		property_util.APP_ID.Name, "appId",
	)
	_, _, oktaAuthConnectionPlugin, _ := newOktaAuthPluginTest(props)

	cacheKey := auth_helpers.GetCacheKey(
		property_util.GetVerifiedWrapperPropertyValue[string](props, property_util.DB_USER),
		host,
		port,
		"us-east-2")
	okta.OktaTokenCache.Put(cacheKey, "cachedToken", time.Minute)
	connAttempts := 0
	var resultProps *utils.RWMap[string, string]
	mockConnFunc := func(props *utils.RWMap[string, string]) (driver.Conn, error) {
		connAttempts++
		cachedToken, ok := okta.OktaTokenCache.Get(cacheKey)
		if ok && cachedToken == "cachedToken" {
			// Fails using cached token.
			return nil, errors.New(pgx_driver.AccessErrors[0])
		}
		// Succeeds on retry attempt with a new token.
		resultProps = props
		return &MockConn{}, nil
	}
	_, err = oktaAuthConnectionPlugin.Connect(hostInfo, props, false, mockConnFunc)

	// Retry connection with a new token when the cached token fails with a login error.
	assert.NoError(t, err)
	assert.Equal(t, "someToken", property_util.PASSWORD.Get(resultProps))
	assert.Equal(t, "jane_doe", property_util.USER.Get(resultProps))
	assert.Equal(t, 1, okta.OktaTokenCache.Size())
	assert.Equal(t, 2, connAttempts)
}

func TestOktaAuthPluginDoesNotRetryConnect(t *testing.T) {
	host := "database-test-name.cluster-XYZ.us-east-2.rds.amazonaws.com"
	port := 1234
	hostInfo, err := host_info_util.NewHostInfoBuilder().SetHost(host).SetPort(port).Build()
	assert.NoError(t, err)
	props := MakeMapFromKeysAndVals(
		property_util.DRIVER_PROTOCOL.Name, "postgresql",
		property_util.DB_USER.Name, "jane_doe",
		property_util.IDP_USERNAME.Name, "username",
		property_util.IDP_PASSWORD.Name, "password",
		property_util.IDP_ENDPOINT.Name, "idpEndpoint",
		property_util.IAM_ROLE_ARN.Name, "iamRoleArn",
		property_util.IAM_IDP_ARN.Name, "iamIdpArn",
		property_util.APP_ID.Name, "appId",
	)
	_, _, oktaAuthConnectionPlugin, _ := newOktaAuthPluginTest(props)

	cacheKey := auth_helpers.GetCacheKey(
		property_util.GetVerifiedWrapperPropertyValue[string](props, property_util.DB_USER),
		host,
		port,
		"us-east-2")
	testErr := errors.New("test")
	connAttempts := 0
	var resultProps *utils.RWMap[string, string]
	mockConnFunc := func(props *utils.RWMap[string, string]) (driver.Conn, error) {
		resultProps = props
		connAttempts++
		cachedPassword, ok := okta.OktaTokenCache.Get(cacheKey)
		if ok && cachedPassword == "oldPassword" {
			// Fails with a non-login error when using cached password.
			return nil, errors.New(pgx_driver.NetworkErrors[0])
		}
		// Fails with a login error when using newly generated token.
		return nil, errors.New(pgx_driver.AccessErrors[0])
	}
	_, err = oktaAuthConnectionPlugin.Connect(hostInfo, props, false, mockConnFunc)

	// Should only generate a new token once, if it fails does not retry.
	require.Error(t, err)
	assert.Equal(t, pgx_driver.AccessErrors[0], err.Error())
	assert.Equal(t, "someToken", property_util.PASSWORD.Get(resultProps))
	assert.Equal(t, "jane_doe", property_util.USER.Get(resultProps))
	assert.Equal(t, 1, okta.OktaTokenCache.Size())
	assert.Equal(t, 1, connAttempts)

	okta.OktaTokenCache.Put(cacheKey, "oldPassword", time.Minute)
	connAttempts = 0
	_, err = oktaAuthConnectionPlugin.Connect(hostInfo, props, false, mockConnFunc)

	// Should not retry if the error is not a login error.
	assert.NotEqual(t, testErr, err)
	assert.Equal(t, "oldPassword", property_util.PASSWORD.Get(resultProps))
	assert.Equal(t, "jane_doe", property_util.USER.Get(resultProps))
	assert.Equal(t, 1, okta.OktaTokenCache.Size())
	assert.Equal(t, 1, connAttempts)
}

func TestOktaAuthPluginCachedIamHostToken(t *testing.T) {
	host := "database-test-name.cluster-XYZ.us-east-2.rds.amazonaws.com"
	port := 1234
	hostInfo, err := host_info_util.NewHostInfoBuilder().SetHost(host).SetPort(port).Build()
	assert.NoError(t, err)
	var resultProps *utils.RWMap[string, string]
	mockConnFunc := func(props *utils.RWMap[string, string]) (driver.Conn, error) {
		resultProps = props
		return &MockConn{throwError: true}, nil
	}
	iamHost := "iamHost"
	iamPort := 543
	iamRegion := "us-east-1"

	props := MakeMapFromKeysAndVals(
		property_util.DRIVER_PROTOCOL.Name, "postgresql",
		property_util.DB_USER.Name, "jane_doe",
		property_util.IDP_USERNAME.Name, "username",
		property_util.IDP_PASSWORD.Name, "password",
		property_util.IDP_ENDPOINT.Name, "idpEndpoint",
		property_util.IAM_ROLE_ARN.Name, "iamRoleArn",
		property_util.IAM_IDP_ARN.Name, "iamIdpArn",
		property_util.APP_ID.Name, "appId",
		property_util.IAM_HOST.Name, iamHost,
		property_util.IAM_REGION.Name, iamRegion,
		property_util.IAM_DEFAULT_PORT.Name, strconv.FormatInt(int64(iamPort), 10),
	)
	_, _, oktaAuthConnectionPlugin, _ := newOktaAuthPluginTest(props)
	cacheKey := auth_helpers.GetCacheKey(
		property_util.GetVerifiedWrapperPropertyValue[string](props, property_util.DB_USER),
		iamHost,
		iamPort,
		region_util.GetRegionFromRegionString(iamRegion))

	okta.OktaTokenCache.Put(cacheKey, "cachedPassword", time.Millisecond*3000)

	_, err = oktaAuthConnectionPlugin.Connect(hostInfo, props, false, mockConnFunc)
	assert.NoError(t, err)
	assert.Equal(t, "cachedPassword", property_util.PASSWORD.Get(resultProps))
	assert.Equal(t, "jane_doe", property_util.USER.Get(resultProps))
	assert.Equal(t, 1, okta.OktaTokenCache.Size())
}

func TestOktaAuthPluginExpiredTokenWithIamHost(t *testing.T) {
	host := "database-test-name.cluster-XYZ.us-east-2.rds.amazonaws.com"
	port := 1234
	hostInfo, err := host_info_util.NewHostInfoBuilder().SetHost(host).SetPort(port).Build()
	assert.NoError(t, err)
	var resultProps *utils.RWMap[string, string]
	mockConnFunc := func(props *utils.RWMap[string, string]) (driver.Conn, error) {
		resultProps = props
		return &MockConn{throwError: true}, nil
	}

	iamHost := "iamHost"
	iamRegion := "us-east-1"
	iamPort := 5435

	props := MakeMapFromKeysAndVals(
		property_util.DRIVER_PROTOCOL.Name, "postgresql",
		property_util.DB_USER.Name, "jane_doe",
		property_util.IDP_USERNAME.Name, "username",
		property_util.IDP_PASSWORD.Name, "password",
		property_util.IDP_ENDPOINT.Name, "idpEndpoint",
		property_util.IAM_ROLE_ARN.Name, "iamRoleArn",
		property_util.IAM_IDP_ARN.Name, "iamIdpArn",
		property_util.APP_ID.Name, "appId",
		property_util.IAM_HOST.Name, iamHost,
		property_util.IAM_REGION.Name, iamRegion,
		property_util.IAM_DEFAULT_PORT.Name, strconv.FormatInt(int64(iamPort), 10),
	)
	_, mockIamToken, oktaAuthConnectionPlugin, _ := newOktaAuthPluginTest(props)
	cacheKey := auth_helpers.GetCacheKey(
		property_util.GetVerifiedWrapperPropertyValue[string](props, property_util.DB_USER),
		iamHost,
		433,
		region_util.GetRegionFromRegionString(iamRegion))

	okta.OktaTokenCache.Put(cacheKey, "mypassword", time.Nanosecond)

	_, err = oktaAuthConnectionPlugin.Connect(hostInfo, props, false, mockConnFunc)
	assert.NoError(t, err)
	assert.Equal(t, mockIamToken.GetMockTokenValue(), property_util.PASSWORD.Get(resultProps))
	assert.Equal(t, "jane_doe", property_util.USER.Get(resultProps))
}

func TestOktaAuthGenerateTokenFailure(t *testing.T) {
	host := "database-test-name.cluster-XYZ.us-east-2.rds.amazonaws.com"
	port := 1234
	hostInfo, err := host_info_util.NewHostInfoBuilder().SetHost(host).SetPort(port).Build()
	assert.NoError(t, err)
	mockConnFunc := func(props *utils.RWMap[string, string]) (driver.Conn, error) { return &MockConn{throwError: true}, nil }

	props := MakeMapFromKeysAndVals(
		property_util.DRIVER_PROTOCOL.Name, "postgresql",
		property_util.DB_USER.Name, "jane_doe",
		property_util.IDP_USERNAME.Name, "username",
		property_util.IDP_PASSWORD.Name, "password",
		property_util.IDP_ENDPOINT.Name, "idpEndpoint",
		property_util.IAM_ROLE_ARN.Name, "iamRoleArn",
		property_util.IAM_IDP_ARN.Name, "iamIdpArn",
		property_util.APP_ID.Name, "appId",
	)
	_, mockIamToken, oktaAuthConnectionPlugin, _ := newOktaAuthPluginTest(props)

	errMsg := "generated token error"
	mockIamToken.GenerateTokenError = errors.New(errMsg)

	_, err = oktaAuthConnectionPlugin.Connect(hostInfo, props, false, mockConnFunc)
	assert.Equal(t, errMsg, err.Error())
}

func TestOktaAuthPluginGetAwsCredentialsProviderError(t *testing.T) {
	props := MakeMapFromKeysAndVals(
		property_util.DRIVER_PROTOCOL.Name, "postgresql",
		property_util.DB_USER.Name, "jane_doe",
		property_util.IDP_USERNAME.Name, "username",
		property_util.IDP_PASSWORD.Name, "password",
		property_util.IDP_ENDPOINT.Name, "idpEndpoint",
		property_util.IAM_ROLE_ARN.Name, "iamRoleArn",
		property_util.IAM_IDP_ARN.Name, "iamIdpArn",
		property_util.APP_ID.Name, "appId",
	)

	connectFunc := func(props *utils.RWMap[string, string]) (driver.Conn, error) {
		return &MockConn{throwError: true}, nil
	}

	mockCredFactory, _, plugin, _ := newOktaAuthPluginTest(props)
	mockCredFactory.getAwsCredentialsProviderError = errors.New("getAwsCredentialsProviderError")
	_, err := plugin.Connect(federatedAuthHostInfo1, props, true, connectFunc)

	assert.Equal(t, 0, mockIamTokenUtility.GenerateAuthenticationTokenCallCounter)
	assert.Equal(t, mockCredFactory.getAwsCredentialsProviderError.Error(), err.Error())
}

func TestOktaAuthPluginMissingParams(t *testing.T) {
	hostInfo, err := host_info_util.NewHostInfoBuilder().SetHost("database-test-name.cluster-XYZ.us-east-2.rds.amazonaws.com").SetPort(1234).Build()
	assert.NoError(t, err)
	mockConnFunc := func(props *utils.RWMap[string, string]) (driver.Conn, error) {
		return &MockConn{throwError: true}, nil
	}

	missingParams := []string{property_util.IDP_USERNAME.Name, property_util.IDP_ENDPOINT.Name, property_util.APP_ID.Name}
	props := MakeMapFromKeysAndVals(
		property_util.DRIVER_PROTOCOL.Name, "postgresql",
		property_util.DB_USER.Name, "jane_doe",
		property_util.IDP_PASSWORD.Name, "password",
		property_util.IAM_ROLE_ARN.Name, "iamRoleArn",
		property_util.IAM_IDP_ARN.Name, "iamIdpArn",
	)
	_, _, oktaAuthConnectionPlugin, _ := newOktaAuthPluginTest(props)

	_, err = oktaAuthConnectionPlugin.Connect(hostInfo, props, false, mockConnFunc)
	assert.Error(t, err)
	assert.Equal(t, error_util.GetMessage("AuthHelpers.missingRequiredParameters",
		"okta", missingParams), err.Error())
}
