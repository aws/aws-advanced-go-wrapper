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
	"awssql/plugins/iam"
	"awssql/property_util"
	"awssql/region_util"
	"awssql/utils/telemetry"
	"database/sql/driver"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newOktaAuthPluginTest(
	props map[string]string) (
	*MockCredentialsProviderFactory,
	*MockIamTokenUtility,
	*federated_auth.OktaAuthPlugin,
	driver_infrastructure.PluginService) {
	federated_auth.OktaTokenCache.Clear()
	mockTargetDriver := &MockTargetDriver{}
	telemetryFactory, _ := telemetry.NewDefaultTelemetryFactory(props)
	mockPluginManager := driver_infrastructure.PluginManager(
		plugin_helpers.NewPluginManagerImpl(mockTargetDriver, props, driver_infrastructure.ConnectionProviderManager{}, telemetryFactory))
	pluginServiceImpl, _ := plugin_helpers.NewPluginServiceImpl(mockPluginManager, driver_infrastructure.NewPgxDriverDialect(), props, pgTestDsn)
	mockPluginService := driver_infrastructure.PluginService(pluginServiceImpl)
	iamTokenUtility := &MockIamTokenUtility{}
	mockCredProviderFactory := &MockCredentialsProviderFactory{}
	oktaPlugin, _ := federated_auth.NewOktaAuthPlugin(mockPluginService, mockCredProviderFactory, iamTokenUtility)
	return mockCredProviderFactory, iamTokenUtility, oktaPlugin, pluginServiceImpl
}

func TestGetOktaAuthPlugin(t *testing.T) {
	props := map[string]string{property_util.DRIVER_PROTOCOL.Name: "postgresql"}
	_, _, _, pluginService := newOktaAuthPluginTest(props)
	pluginFactory := federated_auth.NewOktaAuthPluginFactory()
	_, err := pluginFactory.GetInstance(pluginService, props)
	assert.NoError(t, err)
}

func TestOktaAuthPluginConnect(t *testing.T) {
	hostInfo, err := host_info_util.NewHostInfoBuilder().SetHost("database-test-name.cluster-XYZ.us-east-2.rds.amazonaws.com").SetPort(1234).Build()
	assert.NoError(t, err)
	mockConnFunc := func() (driver.Conn, error) { return &MockConn{throwError: true}, nil }

	props := map[string]string{
		property_util.DRIVER_PROTOCOL.Name: "postgresql",
		property_util.DB_USER.Name:         "jane_doe",
	}
	_, _, oktaAuthConnectionPlugin, _ := newOktaAuthPluginTest(props)

	_, err = oktaAuthConnectionPlugin.Connect(hostInfo, props, false, mockConnFunc)
	assert.NoError(t, err)
	assert.Equal(t, "someToken", props[property_util.PASSWORD.Name])
	assert.Equal(t, "jane_doe", props[property_util.USER.Name])
}

func TestOktaAuthPluginForceConnect(t *testing.T) {
	hostInfo, err := host_info_util.NewHostInfoBuilder().SetHost("database-test-name.cluster-XYZ.us-east-2.rds.amazonaws.com").SetPort(1234).Build()
	assert.NoError(t, err)
	mockConnFunc := func() (driver.Conn, error) { return &MockConn{throwError: true}, nil }

	props := map[string]string{
		property_util.DRIVER_PROTOCOL.Name: "postgresql",
		property_util.DB_USER.Name:         "jane_doe",
	}
	_, _, oktaAuthConnectionPlugin, _ := newOktaAuthPluginTest(props)

	_, err = oktaAuthConnectionPlugin.ForceConnect(hostInfo, props, false, mockConnFunc)
	assert.NoError(t, err)
	assert.Equal(t, "someToken", props[property_util.PASSWORD.Name])
	assert.Equal(t, "jane_doe", props[property_util.USER.Name])
}

func TestOktaAuthPluginInvalidRegion(t *testing.T) {
	hostInfo, err := host_info_util.NewHostInfoBuilder().SetHost("database-test-name.cluster-XYZ.invalid-region.rds.amazonaws.com").SetPort(1234).Build()
	assert.NoError(t, err)
	mockConnFunc := func() (driver.Conn, error) { return &MockConn{throwError: true}, nil }

	props := map[string]string{
		property_util.DRIVER_PROTOCOL.Name: "postgresql",
		property_util.DB_USER.Name:         "jane_doe",
	}
	_, _, oktaAuthConnectionPlugin, _ := newOktaAuthPluginTest(props)

	_, err = oktaAuthConnectionPlugin.Connect(hostInfo, props, false, mockConnFunc)
	assert.Equal(t, error_util.GetMessage("OktaAuthPlugin.unableToDetermineRegion", property_util.IAM_REGION.Name), err.Error())
}

func TestOktaAuthPluginValidRegionThroughIam(t *testing.T) {
	hostInfo, err := host_info_util.NewHostInfoBuilder().SetHost("database-test-name.cluster-XYZ.invalid-region.rds.amazonaws.com").SetPort(1234).Build()
	assert.NoError(t, err)
	mockConnFunc := func() (driver.Conn, error) { return &MockConn{throwError: true}, nil }

	props := map[string]string{
		property_util.DRIVER_PROTOCOL.Name: "postgresql",
		property_util.DB_USER.Name:         "jane_doe",
		property_util.IAM_REGION.Name:      "us-west-2",
	}
	_, mockIamToken, oktaAuthConnectionPlugin, _ := newOktaAuthPluginTest(props)

	_, err = oktaAuthConnectionPlugin.Connect(hostInfo, props, false, mockConnFunc)
	assert.NoError(t, err)
	assert.Equal(t, mockIamToken.GetMockTokenValue(), props[property_util.PASSWORD.Name])
	assert.Equal(t, "jane_doe", props[property_util.USER.Name])
}

func TestOktaAuthPluginCachedToken(t *testing.T) {
	host := "database-test-name.cluster-XYZ.us-east-2.rds.amazonaws.com"
	port := 1234
	hostInfo, err := host_info_util.NewHostInfoBuilder().SetHost(host).SetPort(port).Build()
	assert.NoError(t, err)
	mockConnFunc := func() (driver.Conn, error) { return &MockConn{throwError: true}, nil }

	props := map[string]string{
		property_util.DRIVER_PROTOCOL.Name: "postgresql",
		property_util.DB_USER.Name:         "jane_doe",
	}
	_, _, oktaAuthConnectionPlugin, _ := newOktaAuthPluginTest(props)
	cacheKey := iam.GetCacheKey(
		property_util.GetVerifiedWrapperPropertyValue[string](props, property_util.DB_USER),
		host,
		port,
		"us-east-2")

	federated_auth.OktaTokenCache.Put(cacheKey, "cachedPassword", time.Millisecond*3000)

	_, err = oktaAuthConnectionPlugin.Connect(hostInfo, props, false, mockConnFunc)
	assert.NoError(t, err)
	assert.Equal(t, "cachedPassword", props[property_util.PASSWORD.Name])
	assert.Equal(t, "jane_doe", props[property_util.USER.Name])
	assert.Equal(t, 1, federated_auth.OktaTokenCache.Size())
}

func TestOktaAuthPluginConnectWithRetry(t *testing.T) {
	host := "database-test-name.cluster-XYZ.us-east-2.rds.amazonaws.com"
	port := 1234
	hostInfo, err := host_info_util.NewHostInfoBuilder().SetHost(host).SetPort(port).Build()
	assert.NoError(t, err)
	props := map[string]string{
		property_util.DRIVER_PROTOCOL.Name: "postgresql",
		property_util.DB_USER.Name:         "jane_doe",
	}
	_, _, oktaAuthConnectionPlugin, _ := newOktaAuthPluginTest(props)

	cacheKey := iam.GetCacheKey(
		property_util.GetVerifiedWrapperPropertyValue[string](props, property_util.DB_USER),
		host,
		port,
		"us-east-2")
	federated_auth.OktaTokenCache.Put(cacheKey, "cachedToken", time.Minute)
	connAttempts := 0
	mockConnFunc := func() (driver.Conn, error) {
		connAttempts++
		cachedToken, ok := federated_auth.OktaTokenCache.Get(cacheKey)
		if ok && cachedToken == "cachedToken" {
			// Fails using cached token.
			return nil, errors.New(driver_infrastructure.AccessErrors[0])
		}
		// Succeeds on retry attempt with a new token.
		return &MockConn{}, nil
	}
	_, err = oktaAuthConnectionPlugin.Connect(hostInfo, props, false, mockConnFunc)

	// Retry connection with a new token when the cached token fails with a login error.
	assert.NoError(t, err)
	assert.Equal(t, "someToken", props[property_util.PASSWORD.Name])
	assert.Equal(t, "jane_doe", props[property_util.USER.Name])
	assert.Equal(t, 1, federated_auth.OktaTokenCache.Size())
	assert.Equal(t, 2, connAttempts)
}

func TestOktaAuthPluginDoesNotRetryConnect(t *testing.T) {
	host := "database-test-name.cluster-XYZ.us-east-2.rds.amazonaws.com"
	port := 1234
	hostInfo, err := host_info_util.NewHostInfoBuilder().SetHost(host).SetPort(port).Build()
	assert.NoError(t, err)
	props := map[string]string{
		property_util.DRIVER_PROTOCOL.Name: "postgresql",
		property_util.DB_USER.Name:         "jane_doe",
	}
	_, _, oktaAuthConnectionPlugin, _ := newOktaAuthPluginTest(props)

	cacheKey := iam.GetCacheKey(
		property_util.GetVerifiedWrapperPropertyValue[string](props, property_util.DB_USER),
		host,
		port,
		"us-east-2")
	testErr := errors.New("test")
	connAttempts := 0
	mockConnFunc := func() (driver.Conn, error) {
		connAttempts++
		cachedPassword, ok := federated_auth.OktaTokenCache.Get(cacheKey)
		if ok && cachedPassword == "oldPassword" {
			// Fails with a non-login error when using cached password.
			return nil, errors.New(driver_infrastructure.NetworkErrors[0])
		}
		// Fails with a login error when using newly generated token.
		return nil, errors.New(driver_infrastructure.AccessErrors[0])
	}
	_, err = oktaAuthConnectionPlugin.Connect(hostInfo, props, false, mockConnFunc)

	// Should only generate a new token once, if it fails does not retry.
	require.Error(t, err)
	assert.Equal(t, driver_infrastructure.AccessErrors[0], err.Error())
	assert.Equal(t, "someToken", props[property_util.PASSWORD.Name])
	assert.Equal(t, "jane_doe", props[property_util.USER.Name])
	assert.Equal(t, 1, federated_auth.OktaTokenCache.Size())
	assert.Equal(t, 1, connAttempts)

	federated_auth.OktaTokenCache.Put(cacheKey, "oldPassword", time.Minute)
	connAttempts = 0
	_, err = oktaAuthConnectionPlugin.Connect(hostInfo, props, false, mockConnFunc)

	// Should not retry if the error is not a login error.
	assert.NotEqual(t, testErr, err)
	assert.Equal(t, "oldPassword", props[property_util.PASSWORD.Name])
	assert.Equal(t, "jane_doe", props[property_util.USER.Name])
	assert.Equal(t, 1, federated_auth.OktaTokenCache.Size())
	assert.Equal(t, 1, connAttempts)
}

func TestOktaAuthPluginCachedIamHostToken(t *testing.T) {
	host := "database-test-name.cluster-XYZ.us-east-2.rds.amazonaws.com"
	port := 1234
	hostInfo, err := host_info_util.NewHostInfoBuilder().SetHost(host).SetPort(port).Build()
	assert.NoError(t, err)
	mockConnFunc := func() (driver.Conn, error) { return &MockConn{throwError: true}, nil }
	iamHost := "iamHost"
	iamPort := 543
	iamRegion := "us-east-1"

	props := map[string]string{
		property_util.DRIVER_PROTOCOL.Name:  "postgresql",
		property_util.DB_USER.Name:          "jane_doe",
		property_util.IAM_HOST.Name:         iamHost,
		property_util.IAM_REGION.Name:       iamRegion,
		property_util.IAM_DEFAULT_PORT.Name: strconv.FormatInt(int64(iamPort), 10),
	}
	_, _, oktaAuthConnectionPlugin, _ := newOktaAuthPluginTest(props)
	cacheKey := iam.GetCacheKey(
		property_util.GetVerifiedWrapperPropertyValue[string](props, property_util.DB_USER),
		iamHost,
		iamPort,
		region_util.GetRegionFromRegionString(iamRegion))

	federated_auth.OktaTokenCache.Put(cacheKey, "cachedPassword", time.Millisecond*3000)

	_, err = oktaAuthConnectionPlugin.Connect(hostInfo, props, false, mockConnFunc)
	assert.NoError(t, err)
	assert.Equal(t, "cachedPassword", props[property_util.PASSWORD.Name])
	assert.Equal(t, "jane_doe", props[property_util.USER.Name])
	assert.Equal(t, 1, federated_auth.OktaTokenCache.Size())
}

func TestOktaAuthPluginExpiredTokenWithIamHost(t *testing.T) {
	host := "database-test-name.cluster-XYZ.us-east-2.rds.amazonaws.com"
	port := 1234
	hostInfo, err := host_info_util.NewHostInfoBuilder().SetHost(host).SetPort(port).Build()
	assert.NoError(t, err)
	mockConnFunc := func() (driver.Conn, error) { return &MockConn{throwError: true}, nil }

	iamHost := "iamHost"
	iamRegion := "us-east-1"
	iamPort := 5435

	props := map[string]string{
		property_util.DRIVER_PROTOCOL.Name:  "postgresql",
		property_util.DB_USER.Name:          "jane_doe",
		property_util.IAM_HOST.Name:         iamHost,
		property_util.IAM_REGION.Name:       iamRegion,
		property_util.IAM_DEFAULT_PORT.Name: strconv.FormatInt(int64(iamPort), 10),
	}
	_, mockIamToken, oktaAuthConnectionPlugin, _ := newOktaAuthPluginTest(props)
	cacheKey := iam.GetCacheKey(
		property_util.GetVerifiedWrapperPropertyValue[string](props, property_util.DB_USER),
		iamHost,
		433,
		region_util.GetRegionFromRegionString(iamRegion))

	federated_auth.OktaTokenCache.Put(cacheKey, "mypassword", time.Nanosecond)

	_, err = oktaAuthConnectionPlugin.Connect(hostInfo, props, false, mockConnFunc)
	assert.NoError(t, err)
	assert.Equal(t, mockIamToken.GetMockTokenValue(), props[property_util.PASSWORD.Name])
	assert.Equal(t, "jane_doe", props[property_util.USER.Name])
}

func TestOktaAuthGenerateTokenFailure(t *testing.T) {
	host := "database-test-name.cluster-XYZ.us-east-2.rds.amazonaws.com"
	port := 1234
	hostInfo, err := host_info_util.NewHostInfoBuilder().SetHost(host).SetPort(port).Build()
	assert.NoError(t, err)
	mockConnFunc := func() (driver.Conn, error) { return &MockConn{throwError: true}, nil }

	props := map[string]string{
		property_util.DRIVER_PROTOCOL.Name: "postgresql",
		property_util.DB_USER.Name:         "jane_doe",
	}
	_, mockIamToken, oktaAuthConnectionPlugin, _ := newOktaAuthPluginTest(props)

	errMsg := "Generated token error"
	mockIamToken.GenerateTokenError = errors.New(errMsg)

	_, err = oktaAuthConnectionPlugin.Connect(hostInfo, props, false, mockConnFunc)
	assert.Equal(t, errMsg, err.Error())
}

func TestOktaAuthPluginGetAwsCredentialsProviderError(t *testing.T) {
	props := map[string]string{
		property_util.DRIVER_PROTOCOL.Name: "postgresql",
		property_util.DB_USER.Name:         "jane_doe",
	}

	mockCredFactory, _, plugin, _ := newOktaAuthPluginTest(props)
	mockCredFactory.getAwsCredentialsProviderError = errors.New("getAwsCredentialsProviderError")
	_, err := plugin.Connect(federatedAuthHostInfo1, props, true, connectFunc)

	assert.Equal(t, 0, mockIamTokenUtility.GenerateAuthenticationTokenCallCounter)
	assert.Equal(t, mockCredFactory.getAwsCredentialsProviderError.Error(), err.Error())
}
