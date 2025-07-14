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
	"errors"
	"reflect"
	"testing"

	mock_driver_infrastructure "github.com/aws/aws-advanced-go-wrapper/.test/test/mocks/awssql/driver_infrastructure"
	mock_database_sql_driver "github.com/aws/aws-advanced-go-wrapper/.test/test/mocks/database_sql_driver"
	"github.com/aws/aws-advanced-go-wrapper/awssql/driver_infrastructure"
	"github.com/aws/aws-advanced-go-wrapper/awssql/host_info_util"
	mysql_driver "github.com/aws/aws-advanced-go-wrapper/mysql-driver"
	"github.com/golang/mock/gomock"

	"github.com/stretchr/testify/assert"
)

func TestConnectionProviderManagerGetProvider(t *testing.T) {
	host0, _ := host_info_util.NewHostInfoBuilder().SetHost("host0").SetWeight(10).Build()
	host1, _ := host_info_util.NewHostInfoBuilder().SetHost("host1").SetWeight(9).Build()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	defaultConnProvider := driver_infrastructure.NewDriverConnectionProvider(nil)

	connectionProviderManager := driver_infrastructure.ConnectionProviderManager{DefaultProvider: defaultConnProvider}

	// When there is no other connection provider available returns the default.
	assert.Equal(t, defaultConnProvider, connectionProviderManager.GetConnectionProvider(*host0, nil))

	effectiveConnProvider := mock_driver_infrastructure.NewMockConnectionProvider(ctrl)
	effectiveConnProvider.EXPECT().AcceptsUrl(*host0, gomock.Any()).Return(true).AnyTimes()
	effectiveConnProvider.EXPECT().AcceptsUrl(*host1, gomock.Any()).Return(false).AnyTimes()

	connectionProviderManager = driver_infrastructure.ConnectionProviderManager{
		DefaultProvider: defaultConnProvider, EffectiveProvider: effectiveConnProvider}

	// If the EffectiveProvider accepts the host/props returns that as the provider.
	assert.Equal(t, effectiveConnProvider, connectionProviderManager.GetConnectionProvider(*host0, nil))
	assert.Equal(t, defaultConnProvider, connectionProviderManager.GetConnectionProvider(*host1, nil))

	customConnProvider := mock_driver_infrastructure.NewMockConnectionProvider(ctrl)
	customConnProvider.EXPECT().AcceptsUrl(*host1, gomock.Any()).Return(true).AnyTimes()
	customConnProvider.EXPECT().AcceptsUrl(*host0, gomock.Any()).Return(false).AnyTimes()

	driver_infrastructure.SetCustomConnectionProvider(customConnProvider)

	// If a Custom ConnectionProvider is set and accepts the host/props returns that as the provider.
	assert.Equal(t, customConnProvider, connectionProviderManager.GetConnectionProvider(*host1, nil))
	assert.Equal(t, effectiveConnProvider, connectionProviderManager.GetConnectionProvider(*host0, nil))
	assert.Equal(t, defaultConnProvider, connectionProviderManager.GetDefaultProvider())
	driver_infrastructure.ResetCustomConnectionProvider()
}

func TestConnectionProviderManagerAcceptsStrategy(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	defaultConnProvider := driver_infrastructure.NewDriverConnectionProvider(nil)

	connectionProviderManager := driver_infrastructure.ConnectionProviderManager{DefaultProvider: defaultConnProvider}
	notAcceptedStrategy := "unacceptable"

	// When there is no other connection provider available returns the default provider's result.
	assert.True(t, connectionProviderManager.AcceptsStrategy("random"))
	assert.False(t, connectionProviderManager.AcceptsStrategy(notAcceptedStrategy))

	effectiveConnProvider := mock_driver_infrastructure.NewMockConnectionProvider(ctrl)
	effectiveConnProvider.EXPECT().AcceptsStrategy("effective").Return(true).AnyTimes()
	effectiveConnProvider.EXPECT().AcceptsStrategy(notAcceptedStrategy).Return(false).AnyTimes()

	connectionProviderManager = driver_infrastructure.ConnectionProviderManager{
		DefaultProvider: defaultConnProvider, EffectiveProvider: effectiveConnProvider}

	// If the EffectiveProvider is present and accepts the strategy returns the effective provider's result.
	assert.True(t, connectionProviderManager.AcceptsStrategy("effective"))
	assert.False(t, connectionProviderManager.AcceptsStrategy(notAcceptedStrategy))

	customConnProvider := mock_driver_infrastructure.NewMockConnectionProvider(ctrl)
	customConnProvider.EXPECT().AcceptsStrategy("custom").Return(true).AnyTimes()
	customConnProvider.EXPECT().AcceptsStrategy("effective").Return(false).AnyTimes()
	customConnProvider.EXPECT().AcceptsStrategy(notAcceptedStrategy).Return(false).AnyTimes()

	driver_infrastructure.SetCustomConnectionProvider(customConnProvider)

	// If a Custom ConnectionProvider is present and accepts the strategy returns the effective provider's result.
	assert.True(t, connectionProviderManager.AcceptsStrategy("custom"))
	assert.True(t, connectionProviderManager.AcceptsStrategy("effective"))
	assert.False(t, connectionProviderManager.AcceptsStrategy(notAcceptedStrategy))
	driver_infrastructure.ResetCustomConnectionProvider()
}

func TestConnectionProviderManagerGetHostInfo(t *testing.T) {
	host0, _ := host_info_util.NewHostInfoBuilder().SetHost("host0").SetWeight(10).SetRole(host_info_util.WRITER).Build()
	host1, _ := host_info_util.NewHostInfoBuilder().SetHost("host1").SetWeight(9).SetRole(host_info_util.READER).Build()
	hosts := []*host_info_util.HostInfo{host0, host1}
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	defaultConnProvider := driver_infrastructure.NewDriverConnectionProvider(nil)

	connectionProviderManager := driver_infrastructure.ConnectionProviderManager{DefaultProvider: defaultConnProvider}
	notAcceptedStrategy := "unacceptable"

	// When there is no other connection provider available returns the default provider's result.
	host, err := connectionProviderManager.GetHostInfoByStrategy(hosts, host_info_util.WRITER, "random", nil)
	assert.Nil(t, err)
	assert.Equal(t, host, host0)
	_, err = connectionProviderManager.GetHostInfoByStrategy(hosts, host_info_util.WRITER, notAcceptedStrategy, nil)
	assert.NotNil(t, err)

	effectiveConnProvider := mock_driver_infrastructure.NewMockConnectionProvider(ctrl)
	effectiveConnProvider.EXPECT().AcceptsStrategy("effective").Return(true).AnyTimes()
	effectiveConnProvider.EXPECT().AcceptsStrategy(notAcceptedStrategy).Return(false).AnyTimes()
	effectiveConnProvider.EXPECT().GetHostInfoByStrategy(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(host1, nil).AnyTimes()

	connectionProviderManager = driver_infrastructure.ConnectionProviderManager{
		DefaultProvider: defaultConnProvider, EffectiveProvider: effectiveConnProvider}

	// If the EffectiveProvider is present and accepts the strategy returns the effective provider's result.
	host, _ = connectionProviderManager.GetHostInfoByStrategy(hosts, host_info_util.WRITER, "effective", nil)
	assert.Equal(t, host, host1)
	_, err = connectionProviderManager.GetHostInfoByStrategy(hosts, host_info_util.WRITER, notAcceptedStrategy, nil)
	assert.NotNil(t, err)

	customConnProvider := mock_driver_infrastructure.NewMockConnectionProvider(ctrl)
	customConnProvider.EXPECT().AcceptsStrategy("custom").Return(true).AnyTimes()
	customConnProvider.EXPECT().AcceptsStrategy("effective").Return(false).AnyTimes()
	customConnProvider.EXPECT().AcceptsStrategy(notAcceptedStrategy).Return(false).AnyTimes()
	customConnProvider.EXPECT().GetHostInfoByStrategy(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(host0, nil).AnyTimes()

	driver_infrastructure.SetCustomConnectionProvider(customConnProvider)

	// If a Custom ConnectionProvider is present and accepts the strategy returns the effective provider's result.
	host, _ = connectionProviderManager.GetHostInfoByStrategy(hosts, host_info_util.WRITER, "custom", nil)
	assert.Equal(t, host, host0)
	host, _ = connectionProviderManager.GetHostInfoByStrategy(hosts, host_info_util.WRITER, "effective", nil)
	assert.Equal(t, host, host1)
	_, err = connectionProviderManager.GetHostInfoByStrategy(hosts, host_info_util.WRITER, notAcceptedStrategy, nil)
	assert.NotNil(t, err)
	driver_infrastructure.ResetCustomConnectionProvider()
}

func TestConnectionProviderManagerGetHostSelector(t *testing.T) {
	host0, _ := host_info_util.NewHostInfoBuilder().SetHost("host0").SetWeight(10).SetRole(host_info_util.WRITER).Build()
	host1, _ := host_info_util.NewHostInfoBuilder().SetHost("host1").SetWeight(9).SetRole(host_info_util.READER).Build()
	hosts := []*host_info_util.HostInfo{host0, host1}
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	defaultConnProvider := driver_infrastructure.NewDriverConnectionProvider(nil)

	connectionProviderManager := driver_infrastructure.ConnectionProviderManager{DefaultProvider: defaultConnProvider}
	notAcceptedStrategy := "unacceptable"

	// When there is no other connection provider available returns the default provider's result.
	selector, err := connectionProviderManager.GetHostSelectorStrategy("random")
	assert.Nil(t, err)
	assert.Equal(t, "*driver_infrastructure.RandomHostSelector", reflect.TypeOf(selector).String())
	_, err = connectionProviderManager.GetHostSelectorStrategy(notAcceptedStrategy)
	assert.NotNil(t, err)

	effectiveConnProvider := mock_driver_infrastructure.NewMockConnectionProvider(ctrl)
	effectiveHostSelector := &driver_infrastructure.WeightedRandomHostSelector{}
	effectiveConnProvider.EXPECT().AcceptsStrategy("effective").Return(true).AnyTimes()
	effectiveConnProvider.EXPECT().AcceptsStrategy(notAcceptedStrategy).Return(false).AnyTimes()
	effectiveConnProvider.EXPECT().GetHostSelectorStrategy(gomock.Any()).Return(effectiveHostSelector, nil).AnyTimes()

	connectionProviderManager = driver_infrastructure.ConnectionProviderManager{
		DefaultProvider: defaultConnProvider, EffectiveProvider: effectiveConnProvider}

	// If the EffectiveProvider is present and accepts the strategy returns the effective provider's result.
	selector, _ = connectionProviderManager.GetHostSelectorStrategy("effective")
	assert.Equal(t, effectiveHostSelector, selector)
	_, err = connectionProviderManager.GetHostInfoByStrategy(hosts, host_info_util.WRITER, notAcceptedStrategy, nil)
	assert.NotNil(t, err)

	customConnProvider := mock_driver_infrastructure.NewMockConnectionProvider(ctrl)
	customHostSelector := &driver_infrastructure.RandomHostSelector{}
	customConnProvider.EXPECT().AcceptsStrategy("custom").Return(true).AnyTimes()
	customConnProvider.EXPECT().AcceptsStrategy("effective").Return(false).AnyTimes()
	customConnProvider.EXPECT().AcceptsStrategy(notAcceptedStrategy).Return(false).AnyTimes()
	customConnProvider.EXPECT().GetHostSelectorStrategy(gomock.Any()).Return(customHostSelector, nil).AnyTimes()

	driver_infrastructure.SetCustomConnectionProvider(customConnProvider)

	// If a Custom ConnectionProvider is present and accepts the strategy returns the effective provider's result.
	selector, _ = connectionProviderManager.GetHostSelectorStrategy("custom")
	assert.Equal(t, customHostSelector, selector)
	assert.NotEqual(t, effectiveHostSelector, selector)
	selector, _ = connectionProviderManager.GetHostSelectorStrategy("effective")
	assert.Equal(t, effectiveHostSelector, selector)
	_, err = connectionProviderManager.GetHostInfoByStrategy(hosts, host_info_util.WRITER, notAcceptedStrategy, nil)
	assert.NotNil(t, err)
	driver_infrastructure.ResetCustomConnectionProvider()
}

func TestDriverConnectionProvider(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	driverDialect := &mysql_driver.MySQLDriverDialect{}
	testError := errors.New("test")
	host0, _ := host_info_util.NewHostInfoBuilder().SetHost("host0").SetWeight(10).SetRole(host_info_util.WRITER).Build()
	mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
	mockDriver := mock_database_sql_driver.NewMockDriver(ctrl)

	mockPluginService.EXPECT().GetTargetDriverDialect().Return(driverDialect).AnyTimes()
	mockDriver.EXPECT().Open(driverDialect.PrepareDsn(nil, host0)).Return(nil, testError).AnyTimes()

	driverConnProvider := driver_infrastructure.NewDriverConnectionProvider(mockDriver)

	conn, err := driverConnProvider.Connect(host0, nil, mockPluginService)
	assert.Nil(t, conn)
	assert.NotNil(t, err)
	assert.Equal(t, testError, err)
	assert.True(t, driverConnProvider.AcceptsUrl(*host0, nil))
}
