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

	mock_driver_infrastructure "github.com/aws/aws-advanced-go-wrapper/.test/test/mocks/awssql/driver_infrastructure"
	mock_database_sql_driver "github.com/aws/aws-advanced-go-wrapper/.test/test/mocks/database_sql_driver"
	"github.com/aws/aws-advanced-go-wrapper/awssql/host_info_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/plugins/connection_tracker"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOpenedConnectionTracker_PopulateOpenedConnectionQueue_RdsInstance(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
	mockConn := mock_database_sql_driver.NewMockConn(ctrl)

	tracker := connection_tracker.NewOpenedConnectionTracker(mockPluginService)
	tracker.ClearCache()

	hostInfo := &host_info_util.HostInfo{
		Host: "test-instance.xyz.us-east-1.rds.amazonaws.com",
		Port: 5432,
	}

	tracker.PopulateOpenedConnectionQueue(hostInfo, mockConn)

	connections := tracker.GetOpenedConnections()
	assert.Len(t, connections, 1)
	assert.Contains(t, connections, "test-instance.xyz.us-east-1.rds.amazonaws.com:5432")
}

func TestOpenedConnectionTracker_PopulateOpenedConnectionQueue_WithAliases(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
	mockConn := mock_database_sql_driver.NewMockConn(ctrl)

	tracker := connection_tracker.NewOpenedConnectionTracker(mockPluginService)
	tracker.ClearCache()

	hostInfo := &host_info_util.HostInfo{
		Host: "cluster-endpoint.cluster-xyz.us-east-1.rds.amazonaws.com",
		Port: 5432,
		AllAliases: map[string]bool{
			"test-instance-1.xyz.us-east-1.rds.amazonaws.com:5432": true,
		},
	}

	tracker.PopulateOpenedConnectionQueue(hostInfo, mockConn)

	connections := tracker.GetOpenedConnections()
	assert.Len(t, connections, 1)
	require.Contains(t, connections, "test-instance-1.xyz.us-east-1.rds.amazonaws.com:5432")

	queue := connections["test-instance-1.xyz.us-east-1.rds.amazonaws.com:5432"]
	assert.False(t, queue.IsEmpty())
}

func TestOpenedConnectionTracker_PopulateOpenedConnectionQueue_NoRdsInstance(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
	mockConn := mock_database_sql_driver.NewMockConn(ctrl)

	tracker := connection_tracker.NewOpenedConnectionTracker(mockPluginService)
	tracker.ClearCache()

	hostInfo := &host_info_util.HostInfo{
		Host: "localhost",
		Port: 5432,
		Aliases: map[string]bool{
			"127.0.0.1:5432": true,
		},
	}

	tracker.PopulateOpenedConnectionQueue(hostInfo, mockConn)

	connections := tracker.GetOpenedConnections()
	assert.Len(t, connections, 0)
}

func TestOpenedConnectionTracker_InvalidateAllConnections(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
	mockConn := mock_database_sql_driver.NewMockConn(ctrl)

	tracker := connection_tracker.NewOpenedConnectionTracker(mockPluginService)
	tracker.ClearCache()

	hostInfo := &host_info_util.HostInfo{
		Host: "test-instance-1.xyz.us-east-1.rds.amazonaws.com",
		Port: 5432,
		AllAliases: map[string]bool{
			"test-instance-1.xyz.us-east-1.rds.amazonaws.com:5432": true,
		},
	}

	tracker.PopulateOpenedConnectionQueue(hostInfo, mockConn)
	connections := tracker.GetOpenedConnections()
	assert.Len(t, connections, 1)
	require.Contains(t, connections, "test-instance-1.xyz.us-east-1.rds.amazonaws.com:5432")
	queue := connections["test-instance-1.xyz.us-east-1.rds.amazonaws.com:5432"]
	assert.NotNil(t, queue)

	// Invalidate the connection
	tracker.InvalidateAllConnections(hostInfo)
	connections = tracker.GetOpenedConnections()
	queue = connections["test-instance.cluster-xyz.us-east-1.rds.amazonaws.com:5432"]
	assert.Nil(t, queue)
}

func TestOpenedConnectionTracker_InvalidateAllConnectionsMultipleHosts(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
	mockConn := mock_database_sql_driver.NewMockConn(ctrl)

	tracker := connection_tracker.NewOpenedConnectionTracker(mockPluginService)
	tracker.ClearCache()

	hostInfo := &host_info_util.HostInfo{
		Host: "test-instance-1.xyz.us-east-1.rds.amazonaws.com",
		Port: 5432,
		AllAliases: map[string]bool{
			"test-instance-1.xyz.us-east-1.rds.amazonaws.com:5432": true,
		},
	}

	hostInfo2 := &host_info_util.HostInfo{
		Host: "test-instance-2.xyz.us-east-1.rds.amazonaws.com",
		Port: 5432,
		AllAliases: map[string]bool{
			"test-instance-2.xyz.us-east-1.rds.amazonaws.com:5432": true,
		},
	}

	tracker.PopulateOpenedConnectionQueue(hostInfo, mockConn)
	connections := tracker.GetOpenedConnections()
	assert.Len(t, connections, 1)
	require.Contains(t, connections, "test-instance-1.xyz.us-east-1.rds.amazonaws.com:5432")
	queue := connections["test-instance-1.xyz.us-east-1.rds.amazonaws.com:5432"]
	assert.NotNil(t, queue)

	tracker.PopulateOpenedConnectionQueue(hostInfo2, mockConn)
	connections = tracker.GetOpenedConnections()
	assert.Len(t, connections, 2)
	require.Contains(t, connections, "test-instance-2.xyz.us-east-1.rds.amazonaws.com:5432")
	queue = connections["test-instance-2.xyz.us-east-1.rds.amazonaws.com:5432"]
	assert.NotNil(t, queue)
	queue = connections["test-instance-1.xyz.us-east-1.rds.amazonaws.com:5432"]
	assert.NotNil(t, queue)

	tracker.InvalidateAllConnectionsMultipleHosts(
		"test-instance-1.xyz.us-east-1.rds.amazonaws.com:5432",
		"test-instance-2.xyz.us-east-1.rds.amazonaws.com:5432",
	)

	connections = tracker.GetOpenedConnections()
	queue = connections["test-instance.cluster-xyz.us-east-1.rds.amazonaws.com:5432"]
	assert.Nil(t, queue)
}

func TestOpenedConnectionTracker_InvalidateAllConnectionsMultipleHosts_NoRdsInstance(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)

	tracker := connection_tracker.NewOpenedConnectionTracker(mockPluginService)
	tracker.ClearCache()

	tracker.InvalidateAllConnectionsMultipleHosts("localhost:5432", "127.0.0.1:5432")

	connections := tracker.GetOpenedConnections()
	assert.Len(t, connections, 0)
}

func TestOpenedConnectionTracker_PruneNullConnections(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
	mockConn := mock_database_sql_driver.NewMockConn(ctrl)

	tracker := connection_tracker.NewOpenedConnectionTracker(mockPluginService)
	tracker.ClearCache()

	hostInfo := &host_info_util.HostInfo{
		Host: "test-instance-1.xyz.us-east-1.rds.amazonaws.com",
		Port: 5432,
	}

	tracker.PopulateOpenedConnectionQueue(hostInfo, mockConn)
	tracker.PruneNullConnections()

	connections := tracker.GetOpenedConnections()
	assert.Len(t, connections, 1)
}

func TestOpenedConnectionTracker_ClearCache(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
	mockConn := mock_database_sql_driver.NewMockConn(ctrl)

	tracker := connection_tracker.NewOpenedConnectionTracker(mockPluginService)
	tracker.ClearCache()

	hostInfo := &host_info_util.HostInfo{
		Host: "test-instance.cluster-xyz.us-east-1.rds.amazonaws.com",
		Port: 5432,
	}

	tracker.PopulateOpenedConnectionQueue(hostInfo, mockConn)
	tracker.ClearCache()

	connections := tracker.GetOpenedConnections()
	assert.Len(t, connections, 0)
}
