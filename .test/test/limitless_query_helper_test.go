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

	mock_driver_infrastructure "github.com/aws/aws-advanced-go-wrapper/.test/test/mocks/awssql/driver_infrastructure"
	mock_database_sql_driver "github.com/aws/aws-advanced-go-wrapper/.test/test/mocks/database_sql_driver"
	"github.com/aws/aws-advanced-go-wrapper/awssql/driver_infrastructure"
	"github.com/aws/aws-advanced-go-wrapper/awssql/plugins/limitless"
	"github.com/aws/aws-advanced-go-wrapper/awssql/property_util"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

// Composite mock that satisfies both driver.Conn and driver.QueryerContext.
type mockConnWithQueryer struct {
	*mock_database_sql_driver.MockConn
	*mock_database_sql_driver.MockQueryerContext
}

func TestQueryForLimitlessRouters_Success(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockConn := &mockConnWithQueryer{
		MockConn:           mock_database_sql_driver.NewMockConn(ctrl),
		MockQueryerContext: mock_database_sql_driver.NewMockQueryerContext(ctrl),
	}
	mockPlugin := mock_driver_infrastructure.NewMockPluginService(ctrl)
	mockRows := mock_database_sql_driver.NewMockRows(ctrl)
	dialect := &driver_infrastructure.AuroraPgDatabaseDialect{}
	query := dialect.GetLimitlessRouterEndpointQuery()

	props := map[string]string{
		property_util.LIMITLESS_ROUTER_QUERY_TIMEOUT_MS.Name: "100",
	}

	mockPlugin.EXPECT().GetDialect().Return(dialect)

	mockConn.MockQueryerContext.EXPECT().
		QueryContext(gomock.Any(), query, gomock.Nil()).
		Return(mockRows, nil)

	mockRows.EXPECT().Columns().Return([]string{"hostname", "load"})
	mockRows.EXPECT().Next(gomock.Any()).DoAndReturn(func(dest []driver.Value) error {
		dest[0] = "router1"
		dest[1] = 0.2
		return nil
	})
	mockRows.EXPECT().Next(gomock.Any()).Return(driver.ErrBadConn)
	mockRows.EXPECT().Close().Return(nil)

	helper := limitless.NewLimitlessQueryHelperImpl(mockPlugin)
	result, err := helper.QueryForLimitlessRouters(mockConn, 5432, props)

	assert.NoError(t, err)
	assert.Len(t, result, 1)
	assert.Equal(t, "router1", result[0].Host)
	assert.Equal(t, 8, result[0].Weight)
	assert.Equal(t, 5432, result[0].Port)
}

func TestQueryForLimitlessRouters_HighLoad_Success(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockConn := &mockConnWithQueryer{
		MockConn:           mock_database_sql_driver.NewMockConn(ctrl),
		MockQueryerContext: mock_database_sql_driver.NewMockQueryerContext(ctrl),
	}
	mockPlugin := mock_driver_infrastructure.NewMockPluginService(ctrl)
	mockRows := mock_database_sql_driver.NewMockRows(ctrl)
	dialect := &driver_infrastructure.AuroraPgDatabaseDialect{}
	query := dialect.GetLimitlessRouterEndpointQuery()

	props := map[string]string{
		property_util.LIMITLESS_ROUTER_QUERY_TIMEOUT_MS.Name: "100",
	}

	mockPlugin.EXPECT().GetDialect().Return(dialect)

	mockConn.MockQueryerContext.EXPECT().
		QueryContext(gomock.Any(), query, gomock.Nil()).
		Return(mockRows, nil)

	mockRows.EXPECT().Columns().Return([]string{"hostname", "load"})
	mockRows.EXPECT().Next(gomock.Any()).DoAndReturn(func(dest []driver.Value) error {
		dest[0] = "router1"
		dest[1] = 0.95
		return nil
	})
	mockRows.EXPECT().Next(gomock.Any()).Return(driver.ErrBadConn)
	mockRows.EXPECT().Close().Return(nil)

	helper := limitless.NewLimitlessQueryHelperImpl(mockPlugin)
	result, err := helper.QueryForLimitlessRouters(mockConn, 5432, props)

	assert.NoError(t, err)
	assert.Len(t, result, 1)
	assert.Equal(t, "router1", result[0].Host)
	assert.Equal(t, 1, result[0].Weight)
	assert.Equal(t, 5432, result[0].Port)
}

func TestQueryForLimitlessRouters_InvalidLoad_Defaults(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockConn := &mockConnWithQueryer{
		MockConn:           mock_database_sql_driver.NewMockConn(ctrl),
		MockQueryerContext: mock_database_sql_driver.NewMockQueryerContext(ctrl),
	}
	mockPlugin := mock_driver_infrastructure.NewMockPluginService(ctrl)
	mockRows := mock_database_sql_driver.NewMockRows(ctrl)
	dialect := &driver_infrastructure.AuroraPgDatabaseDialect{}
	query := dialect.GetLimitlessRouterEndpointQuery()

	props := map[string]string{
		property_util.LIMITLESS_ROUTER_QUERY_TIMEOUT_MS.Name: "100",
	}

	mockPlugin.EXPECT().GetDialect().Return(dialect)

	mockConn.MockQueryerContext.EXPECT().
		QueryContext(gomock.Any(), query, gomock.Nil()).
		Return(mockRows, nil)

	mockRows.EXPECT().Columns().Return([]string{"hostname", "load"})
	mockRows.EXPECT().Next(gomock.Any()).DoAndReturn(func(dest []driver.Value) error {
		dest[0] = "router1"
		dest[1] = 11.95
		return nil
	})
	mockRows.EXPECT().Next(gomock.Any()).Return(driver.ErrBadConn)
	mockRows.EXPECT().Close().Return(nil)

	helper := limitless.NewLimitlessQueryHelperImpl(mockPlugin)
	result, err := helper.QueryForLimitlessRouters(mockConn, 5432, props)

	assert.NoError(t, err)
	assert.Len(t, result, 1)
	assert.Equal(t, "router1", result[0].Host)
	assert.Equal(t, 1, result[0].Weight)
	assert.Equal(t, 5432, result[0].Port)
}

func TestQueryForLimitlessRouters_ConnDoesNotImplementQueryerContext(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockConn := mock_database_sql_driver.NewMockConn(ctrl)
	mockPlugin := mock_driver_infrastructure.NewMockPluginService(ctrl)
	dialect := &driver_infrastructure.AuroraPgDatabaseDialect{}

	mockPlugin.EXPECT().GetDialect().Return(dialect)

	props := map[string]string{
		property_util.LIMITLESS_ROUTER_QUERY_TIMEOUT_MS.Name: "100",
	}

	helper := limitless.NewLimitlessQueryHelperImpl(mockPlugin)

	result, err := helper.QueryForLimitlessRouters(mockConn, 1234, props)
	assert.Error(t, err)
	assert.Nil(t, result)
}

func TestQueryForLimitlessRouters_InvalidTimeoutProperty(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockConn := &mockConnWithQueryer{
		MockConn:           mock_database_sql_driver.NewMockConn(ctrl),
		MockQueryerContext: mock_database_sql_driver.NewMockQueryerContext(ctrl),
	}
	mockPlugin := mock_driver_infrastructure.NewMockPluginService(ctrl)
	dialect := &driver_infrastructure.AuroraPgDatabaseDialect{}

	mockPlugin.EXPECT().GetDialect().Return(dialect)
	mockConn.MockQueryerContext.
		EXPECT().
		QueryContext(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil, errors.New(""))
	props := map[string]string{
		property_util.LIMITLESS_ROUTER_QUERY_TIMEOUT_MS.Name: "not-a-number",
	}

	helper := limitless.NewLimitlessQueryHelperImpl(mockPlugin)
	result, err := helper.QueryForLimitlessRouters(mockConn, 1234, props)

	assert.Error(t, err)
	assert.Nil(t, result)
}

func TestQueryForLimitlessRouters_QueryFails(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockConn := &mockConnWithQueryer{
		MockConn:           mock_database_sql_driver.NewMockConn(ctrl),
		MockQueryerContext: mock_database_sql_driver.NewMockQueryerContext(ctrl),
	}
	mockPlugin := mock_driver_infrastructure.NewMockPluginService(ctrl)
	dialect := &driver_infrastructure.AuroraPgDatabaseDialect{}

	mockPlugin.EXPECT().GetDialect().Return(dialect)

	mockConn.MockQueryerContext.EXPECT().
		QueryContext(gomock.Any(), gomock.Any(), gomock.Nil()).
		Return(nil, errors.New("query error"))

	props := map[string]string{
		property_util.LIMITLESS_ROUTER_QUERY_TIMEOUT_MS.Name: "100",
	}

	helper := limitless.NewLimitlessQueryHelperImpl(mockPlugin)
	result, err := helper.QueryForLimitlessRouters(mockConn, 1234, props)

	assert.Error(t, err)
	assert.Nil(t, result)
}

func TestQueryForLimitlessRouters_EmptyHostNameOrBadType(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockConn := &mockConnWithQueryer{
		MockConn:           mock_database_sql_driver.NewMockConn(ctrl),
		MockQueryerContext: mock_database_sql_driver.NewMockQueryerContext(ctrl),
	}
	mockPlugin := mock_driver_infrastructure.NewMockPluginService(ctrl)
	dialect := &driver_infrastructure.AuroraPgDatabaseDialect{}
	mockRows := mock_database_sql_driver.NewMockRows(ctrl)

	mockPlugin.EXPECT().GetDialect().Return(dialect)
	mockConn.MockQueryerContext.EXPECT().
		QueryContext(gomock.Any(), gomock.Any(), gomock.Nil()).
		Return(mockRows, nil)

	mockRows.EXPECT().Columns().Return([]string{"host", "load"})
	mockRows.EXPECT().Next(gomock.Any()).DoAndReturn(func(dest []driver.Value) error {
		dest[0] = ""  // invalid hostname
		dest[1] = 0.3 // valid load
		return nil
	})
	mockRows.EXPECT().Next(gomock.Any()).Return(driver.ErrBadConn)
	mockRows.EXPECT().Close().Return(nil)

	props := map[string]string{
		property_util.LIMITLESS_ROUTER_QUERY_TIMEOUT_MS.Name: "100",
	}

	helper := limitless.NewLimitlessQueryHelperImpl(mockPlugin)
	result, err := helper.QueryForLimitlessRouters(mockConn, 9999, props)

	assert.NoError(t, err)
	assert.Len(t, result, 0)
}
