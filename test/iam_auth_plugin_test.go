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
	"awssql/plugins/iam"
	"awssql/property_util"
	"awssql/region_util"
	"github.com/go-sql-driver/mysql"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func beforeIamAuthPluginTests(props map[string]string) (driver_infrastructure.PluginService, iam.IamTokenUtility) {
	iam.TokenCache.Clear()
	mockTargetDriver := &MockTargetDriver{}
	mockPluginManager := driver_infrastructure.PluginManager(plugin_helpers.NewPluginManagerImpl(mockTargetDriver, props, driver_infrastructure.ConnectionProviderManager{}))
	pluginServiceImpl, _ := plugin_helpers.NewPluginServiceImpl(mockPluginManager, driver_infrastructure.NewMySQLDriverDialect(), props, mysqlTestDsn)

	mockPluginService := driver_infrastructure.PluginService(pluginServiceImpl)
	return mockPluginService, &MockIamTokenUtility{}
}

func TestIamAuthPluginConnect(t *testing.T) {
	hostInfo := host_info_util.NewHostInfoBuilder().SetHost("database-test-name.cluster-XYZ.us-east-2.rds.amazonaws.com").SetPort(1234).Build()
	mockConnFunc := func() (any, error) { return &MockConn{nil, nil, nil, nil, true}, nil }

	props := map[string]string{
		property_util.USER.Name:            "someUser",
		property_util.DRIVER_PROTOCOL.Name: "mysql",
	}
	mockPluginService, mockIamTokenUtility := beforeIamAuthPluginTests(props)

	iamAuthPlugin := iam.NewIamAuthPlugin(mockPluginService, mockIamTokenUtility, props)

	_, err := iamAuthPlugin.Connect(hostInfo, props, false, mockConnFunc)

	assert.Nil(t, err)
	assert.Equal(t,
		mockIamTokenUtility.(*MockIamTokenUtility).GetMockTokenValue(),
		property_util.GetVerifiedWrapperPropertyValue[string](props, property_util.PASSWORD))
	assert.Equal(t, "someUser", mockIamTokenUtility.(*MockIamTokenUtility).CapturedUsername)
	assert.Equal(t, "database-test-name.cluster-XYZ.us-east-2.rds.amazonaws.com", mockIamTokenUtility.(*MockIamTokenUtility).CapturedHost)
	assert.Equal(t, 1234, mockIamTokenUtility.(*MockIamTokenUtility).CapturedPort)
	assert.Equal(t, region_util.US_EAST_2, mockIamTokenUtility.(*MockIamTokenUtility).CapturedRegion)
}

func TestIamAuthPluginConnectWithIamProps(t *testing.T) {
	hostInfo := host_info_util.NewHostInfoBuilder().SetHost("database-test-name.cluster-XYZ.us-east-2.rds.amazonaws.com").SetPort(1234).Build()
	mockConnFunc := func() (any, error) { return &MockConn{nil, nil, nil, nil, true}, nil }

	props := map[string]string{
		property_util.USER.Name:             "someUser",
		property_util.DRIVER_PROTOCOL.Name:  "mysql",
		property_util.IAM_HOST.Name:         "someIamHost",
		property_util.IAM_REGION.Name:       string(region_util.US_EAST_1),
		property_util.IAM_DEFAULT_PORT.Name: "9999",
	}
	mockPluginService, mockIamTokenUtility := beforeIamAuthPluginTests(props)

	iamAuthPlugin := iam.NewIamAuthPlugin(mockPluginService, mockIamTokenUtility, props)

	_, err := iamAuthPlugin.Connect(hostInfo, props, false, mockConnFunc)

	assert.Nil(t, err)
	assert.Equal(t,
		mockIamTokenUtility.(*MockIamTokenUtility).GetMockTokenValue(),
		property_util.GetVerifiedWrapperPropertyValue[string](props, property_util.PASSWORD))
	assert.Equal(t, "someUser", mockIamTokenUtility.(*MockIamTokenUtility).CapturedUsername)
	assert.Equal(t, "someIamHost", mockIamTokenUtility.(*MockIamTokenUtility).CapturedHost)
	assert.Equal(t, 9999, mockIamTokenUtility.(*MockIamTokenUtility).CapturedPort)
	assert.Equal(t, region_util.US_EAST_1, mockIamTokenUtility.(*MockIamTokenUtility).CapturedRegion)
}

func TestIamAuthPluginConnectInvalidRegionError(t *testing.T) {
	hostInfo := host_info_util.NewHostInfoBuilder().SetHost("mydatabasewithnoregion.com").SetPort(1234).Build()
	mockConnFunc := func() (any, error) { return &MockConn{nil, nil, nil, nil, true}, nil }

	props := map[string]string{
		property_util.USER.Name:            "someUser",
		property_util.DRIVER_PROTOCOL.Name: "mysql",
		property_util.IAM_REGION.Name:      "someUnknownRegion",
	}
	mockPluginService, mockIamTokenUtility := beforeIamAuthPluginTests(props)

	iamAuthPlugin := iam.NewIamAuthPlugin(mockPluginService, mockIamTokenUtility, props)

	_, err := iamAuthPlugin.Connect(hostInfo, props, false, mockConnFunc)

	assert.NotNil(t, err)
	assert.Equal(t, error_util.GetMessage("IamAuthPlugin.unableToDetermineRegion", property_util.IAM_REGION.Name), err.Error())
}

func TestIamAuthPluginConnectPopulatesEmptyTokenCache(t *testing.T) {
	hostInfo := host_info_util.NewHostInfoBuilder().SetHost("database-test-name.cluster-XYZ.us-east-2.rds.amazonaws.com").SetPort(1234).Build()
	mockConnFunc := func() (any, error) { return &MockConn{nil, nil, nil, nil, true}, nil }

	props := map[string]string{
		property_util.USER.Name:            "someUser",
		property_util.DRIVER_PROTOCOL.Name: "mysql",
	}
	mockPluginService, mockIamTokenUtility := beforeIamAuthPluginTests(props)

	iamAuthPlugin := iam.NewIamAuthPlugin(mockPluginService, mockIamTokenUtility, props)

	assert.Equal(t, 0, iam.TokenCache.Size())

	_, err := iamAuthPlugin.Connect(hostInfo, props, false, mockConnFunc)

	assert.Equal(t, 1, iam.TokenCache.Size())
	assert.Nil(t, err)
}

func TestIamAuthPluginConnectUsesCachedToken(t *testing.T) {
	hostInfo := host_info_util.NewHostInfoBuilder().SetHost("database-test-name.cluster-XYZ.us-east-2.rds.amazonaws.com").SetPort(1234).Build()
	mockConnFunc := func() (any, error) { return &MockConn{nil, nil, nil, nil, true}, nil }

	props := map[string]string{
		property_util.USER.Name:             "someUser",
		property_util.DRIVER_PROTOCOL.Name:  "mysql",
		property_util.IAM_HOST.Name:         "someIamHost",
		property_util.IAM_REGION.Name:       string(region_util.US_EAST_1),
		property_util.IAM_DEFAULT_PORT.Name: "9999",
	}

	mockPluginService, mockIamTokenUtility := beforeIamAuthPluginTests(props)

	cacheExpirationDuration := time.Duration(180) * time.Second
	cachedToken := "someCachedToken"
	cacheKey := iam.GetCacheKey(
		property_util.GetVerifiedWrapperPropertyValue[string](props, property_util.USER),
		property_util.GetVerifiedWrapperPropertyValue[string](props, property_util.IAM_HOST),
		property_util.GetVerifiedWrapperPropertyValue[int](props, property_util.IAM_DEFAULT_PORT),
		region_util.US_EAST_1,
	)

	iam.TokenCache.Put(cacheKey, cachedToken, cacheExpirationDuration)

	iamAuthPlugin := iam.NewIamAuthPlugin(mockPluginService, mockIamTokenUtility, props)

	_, err := iamAuthPlugin.Connect(hostInfo, props, false, mockConnFunc)

	assert.Equal(t, 0, mockIamTokenUtility.(*MockIamTokenUtility).GenerateAuthenticationTokenCallCounter)
	assert.Nil(t, err)
}

func TestIamAuthPluginConnectCacheExpiredToken(t *testing.T) {
	hostInfo := host_info_util.NewHostInfoBuilder().SetHost("database-test-name.cluster-XYZ.us-east-2.rds.amazonaws.com").SetPort(1234).Build()
	mockConnFunc := func() (any, error) { return &MockConn{nil, nil, nil, nil, true}, nil }

	props := map[string]string{
		property_util.USER.Name:             "someUser",
		property_util.DRIVER_PROTOCOL.Name:  "mysql",
		property_util.IAM_HOST.Name:         "someIamHost",
		property_util.IAM_REGION.Name:       string(region_util.US_EAST_1),
		property_util.IAM_DEFAULT_PORT.Name: "9999",
	}

	mockPluginService, mockIamTokenUtility := beforeIamAuthPluginTests(props)

	expiredCacheExpirationDuration := -(time.Duration(180) * time.Second)
	cachedToken := "someCachedToken"
	cacheKey := iam.GetCacheKey(
		property_util.GetVerifiedWrapperPropertyValue[string](props, property_util.USER),
		property_util.GetVerifiedWrapperPropertyValue[string](props, property_util.IAM_HOST),
		property_util.GetVerifiedWrapperPropertyValue[int](props, property_util.IAM_DEFAULT_PORT),
		region_util.US_EAST_1,
	)

	iam.TokenCache.Put(cacheKey, cachedToken, expiredCacheExpirationDuration)

	iamAuthPlugin := iam.NewIamAuthPlugin(mockPluginService, mockIamTokenUtility, props)

	_, err := iamAuthPlugin.Connect(hostInfo, props, false, mockConnFunc)

	assert.Equal(t, 1, mockIamTokenUtility.(*MockIamTokenUtility).GenerateAuthenticationTokenCallCounter)
	assert.Nil(t, err)
	assert.Equal(t,
		mockIamTokenUtility.(*MockIamTokenUtility).GetMockTokenValue(),
		property_util.GetVerifiedWrapperPropertyValue[string](props, property_util.PASSWORD))
	assert.Equal(t, "someUser", mockIamTokenUtility.(*MockIamTokenUtility).CapturedUsername)
	assert.Equal(t, "someIamHost", mockIamTokenUtility.(*MockIamTokenUtility).CapturedHost)
	assert.Equal(t, 9999, mockIamTokenUtility.(*MockIamTokenUtility).CapturedPort)
	assert.Equal(t, region_util.US_EAST_1, mockIamTokenUtility.(*MockIamTokenUtility).CapturedRegion)
}

func TestIamAuthPluginConnectTtlExpiredCachedToken(t *testing.T) {
	hostInfo := host_info_util.NewHostInfoBuilder().SetHost("database-test-name.cluster-XYZ.us-east-2.rds.amazonaws.com").SetPort(1234).Build()
	mockLoginError := &mysql.MySQLError{SQLState: [5]byte(([]byte(driver_infrastructure.SqlStateAccessError))[:5])}
	mockConnFuncCallCounter := 0
	mockConnFunc := func() (any, error) {
		if mockConnFuncCallCounter == 0 {
			mockConnFuncCallCounter++
			return nil, mockLoginError
		} else {
			mockConnFuncCallCounter++
			return &MockConn{nil, nil, nil, nil, true}, nil
		}
	}

	props := map[string]string{
		property_util.USER.Name:             "someUser",
		property_util.DRIVER_PROTOCOL.Name:  "mysql",
		property_util.IAM_HOST.Name:         "someIamHost",
		property_util.IAM_REGION.Name:       string(region_util.US_EAST_1),
		property_util.IAM_DEFAULT_PORT.Name: "9999",
	}

	mockPluginService, mockIamTokenUtility := beforeIamAuthPluginTests(props)

	expiredCacheExpirationDuration := time.Duration(180) * time.Second
	cachedToken := "someCachedToken"
	cacheKey := iam.GetCacheKey(
		property_util.GetVerifiedWrapperPropertyValue[string](props, property_util.USER),
		property_util.GetVerifiedWrapperPropertyValue[string](props, property_util.IAM_HOST),
		property_util.GetVerifiedWrapperPropertyValue[int](props, property_util.IAM_DEFAULT_PORT),
		region_util.US_EAST_1,
	)

	iam.TokenCache.Put(cacheKey, cachedToken, expiredCacheExpirationDuration)

	iamAuthPlugin := iam.NewIamAuthPlugin(mockPluginService, mockIamTokenUtility, props)

	_, err := iamAuthPlugin.Connect(hostInfo, props, false, mockConnFunc)

	assert.Equal(t, 1, mockIamTokenUtility.(*MockIamTokenUtility).GenerateAuthenticationTokenCallCounter)
	assert.Nil(t, err)
	assert.Equal(t,
		mockIamTokenUtility.(*MockIamTokenUtility).GetMockTokenValue(),
		property_util.GetVerifiedWrapperPropertyValue[string](props, property_util.PASSWORD))
	assert.Equal(t, "someUser", mockIamTokenUtility.(*MockIamTokenUtility).CapturedUsername)
	assert.Equal(t, "someIamHost", mockIamTokenUtility.(*MockIamTokenUtility).CapturedHost)
	assert.Equal(t, 9999, mockIamTokenUtility.(*MockIamTokenUtility).CapturedPort)
	assert.Equal(t, region_util.US_EAST_1, mockIamTokenUtility.(*MockIamTokenUtility).CapturedRegion)
}

func TestIamAuthPluginConnectLoginError(t *testing.T) {
	hostInfo := host_info_util.NewHostInfoBuilder().SetHost("database-test-name.cluster-XYZ.us-east-2.rds.amazonaws.com").SetPort(1234).Build()
	mockLoginError := &mysql.MySQLError{SQLState: [5]byte(([]byte(driver_infrastructure.SqlStateAccessError))[:5])}
	mockConnFunc := func() (any, error) { return nil, mockLoginError }

	props := map[string]string{
		property_util.USER.Name:            "someUser",
		property_util.DRIVER_PROTOCOL.Name: "mysql",
	}
	mockPluginService, mockIamTokenUtility := beforeIamAuthPluginTests(props)

	iamAuthPlugin := iam.NewIamAuthPlugin(mockPluginService, mockIamTokenUtility, props)

	result, err := iamAuthPlugin.Connect(hostInfo, props, false, mockConnFunc)

	assert.Nil(t, result)
	assert.NotNil(t, err)
	assert.Equal(t, mockLoginError, err)
	assert.Equal(t,
		mockIamTokenUtility.(*MockIamTokenUtility).GetMockTokenValue(),
		property_util.GetVerifiedWrapperPropertyValue[string](props, property_util.PASSWORD))
	assert.Equal(t, "someUser", mockIamTokenUtility.(*MockIamTokenUtility).CapturedUsername)
	assert.Equal(t, "database-test-name.cluster-XYZ.us-east-2.rds.amazonaws.com", mockIamTokenUtility.(*MockIamTokenUtility).CapturedHost)
	assert.Equal(t, 1234, mockIamTokenUtility.(*MockIamTokenUtility).CapturedPort)
	assert.Equal(t, region_util.US_EAST_2, mockIamTokenUtility.(*MockIamTokenUtility).CapturedRegion)
	assert.Equal(t, 1, mockIamTokenUtility.(*MockIamTokenUtility).GenerateAuthenticationTokenCallCounter)
}
