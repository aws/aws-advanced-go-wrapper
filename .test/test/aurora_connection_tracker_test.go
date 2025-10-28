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
	"testing"

	mock_driver_infrastructure "github.com/aws/aws-advanced-go-wrapper/.test/test/mocks/awssql/driver_infrastructure"
	mock_connection_tracker "github.com/aws/aws-advanced-go-wrapper/.test/test/mocks/awssql/plugins/connection_tracker"
	mock_database_sql_driver "github.com/aws/aws-advanced-go-wrapper/.test/test/mocks/database_sql_driver"
	"github.com/aws/aws-advanced-go-wrapper/awssql/error_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/host_info_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/plugins"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

func TestAuroraConnectionTracker_TrackNewInstanceConnections(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
	mockTracker := mock_connection_tracker.NewMockConnectionTracker(ctrl)
	mockDriverConn := mock_database_sql_driver.NewMockConn(ctrl)
	props := MakeMapFromKeysAndVals(
		"someKey", "someVal",
	)
	connectFunc := func(props *utils.RWMap[string, string]) (driver.Conn, error) {
		return mockDriverConn, nil
	}
	hostInfo := &host_info_util.HostInfo{Role: host_info_util.WRITER, Host: "writer1"}

	mockPluginService.EXPECT().FillAliases(mockDriverConn, hostInfo)
	mockTracker.EXPECT().PopulateOpenedConnectionQueue(hostInfo, mockDriverConn)

	auroraConnectionTracker := plugins.NewAuroraConnectionTrackerPlugin(mockPluginService, nil, mockTracker)
	conn, err := auroraConnectionTracker.Connect(hostInfo, props, true, connectFunc)
	assert.NoError(t, err)
	assert.Equal(t, conn, mockDriverConn)
	assert.Equal(t, 0, len(hostInfo.Aliases))
}

func TestAuroraConnectionTracker_InvalidateOpenedConnectionsWhenWriterHostNotChange(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
	mockTracker := mock_connection_tracker.NewMockConnectionTracker(ctrl)
	mockDriverConn := mock_database_sql_driver.NewMockConn(ctrl)

	failoverError := &error_util.AwsWrapperError{ErrorType: error_util.FailoverSuccessError.ErrorType}
	execFunc := func() (any, any, bool, error) {
		return "test1", "test2", true, failoverError
	}

	originalHost := &host_info_util.HostInfo{Role: host_info_util.WRITER, Host: "originalHost"}

	mockPluginService.EXPECT().GetHosts().Return([]*host_info_util.HostInfo{originalHost}).Times(2)

	auroraConnectionTracker := plugins.NewAuroraConnectionTrackerPlugin(mockPluginService, nil, mockTracker)
	_, _, _, err := auroraConnectionTracker.Execute(mockDriverConn, "", execFunc, "")
	assert.Error(t, err)
	assert.Equal(t, failoverError, err)
}

func TestAuroraConnectionTracker_InvalidateOpenedConnectionsWhenWriterHostChanged(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
	mockTracker := mock_connection_tracker.NewMockConnectionTracker(ctrl)
	mockDriverConn := mock_database_sql_driver.NewMockConn(ctrl)

	failoverError := &error_util.AwsWrapperError{ErrorType: error_util.FailoverSuccessError.ErrorType}
	execFunc := func() (any, any, bool, error) {
		return "test1", "test2", true, failoverError
	}

	originalHost := &host_info_util.HostInfo{Role: host_info_util.WRITER, Host: "originalHost"}
	newHost := &host_info_util.HostInfo{Role: host_info_util.WRITER, Host: "newHost"}

	mockPluginService.EXPECT().GetHosts().Return([]*host_info_util.HostInfo{originalHost})
	mockPluginService.EXPECT().GetHosts().Return([]*host_info_util.HostInfo{newHost})
	mockTracker.EXPECT().InvalidateAllConnections(originalHost)
	mockTracker.EXPECT().LogOpenedConnections()

	auroraConnectionTracker := plugins.NewAuroraConnectionTrackerPlugin(mockPluginService, nil, mockTracker)
	_, _, _, err := auroraConnectionTracker.Execute(mockDriverConn, "", execFunc, "")
	assert.Error(t, err)
	assert.Equal(t, failoverError, err)
}
