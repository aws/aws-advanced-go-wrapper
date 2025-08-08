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
	"testing"
	"time"

	mock_driver_infrastructure "github.com/aws/aws-advanced-go-wrapper/.test/test/mocks/awssql/driver_infrastructure"
	mock_database_sql_driver "github.com/aws/aws-advanced-go-wrapper/.test/test/mocks/database_sql_driver"
	"github.com/aws/aws-advanced-go-wrapper/awssql/driver_infrastructure"
	"github.com/aws/aws-advanced-go-wrapper/awssql/host_info_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/internal_pool"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

func TestNewInternalPooledConnectionProvider(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockDriver := mock_database_sql_driver.NewMockDriver(ctrl)
	opts := internal_pool.NewInternalPoolOptions()
	poolKeyFunc := func(hostInfo *host_info_util.HostInfo, props map[string]string) string {
		return "test-key"
	}

	provider := internal_pool.NewInternalPooledConnectionProvider(mockDriver, opts, poolKeyFunc, time.Minute)

	assert.NotNil(t, provider)
}

func TestInternalPooledConnectionProvider_AcceptsUrl(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockDriver := mock_database_sql_driver.NewMockDriver(ctrl)
	opts := internal_pool.NewInternalPoolOptions()
	provider := internal_pool.NewInternalPooledConnectionProvider(mockDriver, opts, nil, time.Minute)

	// RDS URL should be accepted
	rdsHost := host_info_util.HostInfo{Host: "test.cluster-abc123.us-east-1.rds.amazonaws.com"}
	assert.True(t, provider.AcceptsUrl(rdsHost, nil))

	// Non-RDS URL should not be accepted
	nonRdsHost := host_info_util.HostInfo{Host: "localhost"}
	assert.False(t, provider.AcceptsUrl(nonRdsHost, nil))
}

func TestInternalPooledConnectionProvider_AcceptsStrategy(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockDriver := mock_database_sql_driver.NewMockDriver(ctrl)
	opts := internal_pool.NewInternalPoolOptions()
	provider := internal_pool.NewInternalPooledConnectionProvider(mockDriver, opts, nil, time.Minute)

	assert.True(t, provider.AcceptsStrategy(driver_infrastructure.SELECTOR_RANDOM))
	assert.True(t, provider.AcceptsStrategy(driver_infrastructure.SELECTOR_HIGHEST_WEIGHT))
	assert.True(t, provider.AcceptsStrategy(driver_infrastructure.SELECTOR_WEIGHTED_RANDOM))
	assert.False(t, provider.AcceptsStrategy("unsupported-strategy"))
}

func TestInternalPooledConnectionProvider_GetHostSelectorStrategy(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockDriver := mock_database_sql_driver.NewMockDriver(ctrl)
	opts := internal_pool.NewInternalPoolOptions()
	provider := internal_pool.NewInternalPooledConnectionProvider(mockDriver, opts, nil, time.Minute)

	selector, err := provider.GetHostSelectorStrategy(driver_infrastructure.SELECTOR_RANDOM)
	assert.NoError(t, err)
	assert.NotNil(t, selector)

	_, err = provider.GetHostSelectorStrategy("unsupported-strategy")
	assert.Error(t, err)
}

func TestInternalPooledConnectionProvider_Connect(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockDriver := mock_database_sql_driver.NewMockDriver(ctrl)
	mockConn := mock_database_sql_driver.NewMockConn(ctrl)
	mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
	mockDriverDialect := mock_driver_infrastructure.NewMockDriverDialect(ctrl)

	opts := internal_pool.NewInternalPoolOptions()
	provider := internal_pool.NewInternalPooledConnectionProvider(mockDriver, opts, nil, time.Minute)

	hostInfo := &host_info_util.HostInfo{Host: "test.cluster-abc123.us-east-1.rds.amazonaws.com"}
	props := map[string]string{"user": "testuser"}

	mockPluginService.EXPECT().GetTargetDriverDialect().Return(mockDriverDialect)
	mockDriverDialect.EXPECT().PrepareDsn(props, hostInfo).Return("test-dsn")
	mockDriver.EXPECT().Open("test-dsn").Return(mockConn, nil)

	conn, err := provider.Connect(hostInfo, props, mockPluginService)
	assert.NoError(t, err)
	assert.NotNil(t, conn)
}
