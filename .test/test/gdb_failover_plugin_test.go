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
	"context"
	"errors"
	"testing"

	mock_driver_infrastructure "github.com/aws/aws-advanced-go-wrapper/.test/test/mocks/awssql/driver_infrastructure"
	"github.com/aws/aws-advanced-go-wrapper/awssql/error_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/host_info_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/plugins"
	"github.com/aws/aws-advanced-go-wrapper/awssql/property_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils/telemetry"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

// Test hosts in the "home" region (us-east-2).
var gdbBuilder = host_info_util.NewHostInfoBuilder()
var gdbWriterHost, _ = gdbBuilder.SetHost("mydatabase-instance-1.xyz.us-east-2.rds.amazonaws.com").SetPort(3306).SetRole(host_info_util.WRITER).Build()
var gdbReaderHost, _ = gdbBuilder.SetHost("mydatabase-instance-2.xyz.us-east-2.rds.amazonaws.com").SetPort(3306).SetRole(host_info_util.READER).Build()
var gdbOutOfHomeReaderHost, _ = gdbBuilder.SetHost("mydatabase-instance-3.xyz.us-west-2.rds.amazonaws.com").SetPort(3306).SetRole(host_info_util.READER).Build()
var gdbMockConn = &MockConn{}

type gdbTestSetup struct {
	plugin *plugins.FailoverPlugin
	mockPS *mock_driver_infrastructure.MockPluginService
	mockSC *mock_driver_infrastructure.MockServicesContainer
}

func createGdbPlugin(
	t *testing.T,
	propsMap map[string]string,
	testHosts []*host_info_util.HostInfo,
	isInTransaction bool,
	hostRoleReturn host_info_util.HostRole,
	forceRefreshOk bool,
	connectErr error,
) gdbTestSetup {
	ctrl := gomock.NewController(t)

	props := utils.NewRWMapFromMap(propsMap)
	telemetryFactory, _ := telemetry.NewDefaultTelemetryFactory(props)

	mockSC := mock_driver_infrastructure.NewMockServicesContainer(ctrl)
	mockPS := mock_driver_infrastructure.NewMockPluginService(ctrl)

	mockSC.EXPECT().GetPluginService().Return(mockPS).AnyTimes()
	mockSC.EXPECT().GetTelemetryFactory().Return(telemetryFactory).AnyTimes()

	initialHost, _ := gdbBuilder.SetHost("mydatabase.cluster-xyz.us-east-2.rds.amazonaws.com").SetPort(3306).SetRole(host_info_util.WRITER).Build()
	mockPS.EXPECT().GetInitialConnectionHostInfo().Return(initialHost).AnyTimes()

	mockPS.EXPECT().GetTelemetryContext().Return(context.Background()).AnyTimes()
	mockPS.EXPECT().GetTelemetryFactory().Return(telemetryFactory).AnyTimes()
	mockPS.EXPECT().SetTelemetryContext(gomock.Any()).AnyTimes()

	mockPS.EXPECT().ForceRefreshHostListWithTimeout(gomock.Any(), gomock.Any()).Return(forceRefreshOk, nil).AnyTimes()

	mockPS.EXPECT().GetAllHosts().Return(testHosts).AnyTimes()
	mockPS.EXPECT().GetHosts().Return(testHosts).AnyTimes()

	mockPS.EXPECT().RefreshHostList(gomock.Any()).Return(nil).AnyTimes()

	mockPS.EXPECT().IsInTransaction().Return(isInTransaction).AnyTimes()

	mockPS.EXPECT().GetHostRole(gomock.Any()).Return(hostRoleReturn).AnyTimes()

	if connectErr != nil {
		mockPS.EXPECT().Connect(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, connectErr).AnyTimes()
	} else {
		mockPS.EXPECT().Connect(gomock.Any(), gomock.Any(), gomock.Any()).Return(gdbMockConn, nil).AnyTimes()
	}

	mockPS.EXPECT().SetCurrentConnection(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

	mockPS.EXPECT().SetAvailability(gomock.Any(), gomock.Any()).AnyTimes()

	mockPS.EXPECT().SetInTransaction(gomock.Any()).AnyTimes()

	mockPS.EXPECT().GetHostInfoByStrategy(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(role host_info_util.HostRole, strategy string, hosts []*host_info_util.HostInfo) (*host_info_util.HostInfo, error) {
			if len(hosts) == 0 {
				return nil, errors.New("no hosts")
			}
			return hosts[0], nil
		}).AnyTimes()

	mockPS.EXPECT().GetCurrentHostInfo().Return(gdbWriterHost, nil).AnyTimes()

	plugin, err := plugins.NewGlobalDbFailoverPlugin(mockSC, props)
	assert.NoError(t, err)

	return gdbTestSetup{
		plugin: plugin,
		mockPS: mockPS,
		mockSC: mockSC,
	}
}

func TestGdbInitFailoverModeStrictWriter(t *testing.T) {
	setup := createGdbPlugin(t, map[string]string{
		property_util.DRIVER_PROTOCOL.Name:               "mysql",
		property_util.GDB_FAILOVER_HOME_REGION.Name:      "us-east-2",
		property_util.GDB_FAILOVER_ACTIVE_HOME_MODE.Name: "strict-writer",
		property_util.ENABLE_CONNECT_FAILOVER.Name:       "false",
	}, []*host_info_util.HostInfo{gdbWriterHost, gdbReaderHost}, false, host_info_util.WRITER, true, nil)

	err := setup.plugin.InitFailoverMode()
	assert.NoError(t, err)
}

func TestGdbInitFailoverModeInvalidMode(t *testing.T) {
	setup := createGdbPlugin(t, map[string]string{
		property_util.DRIVER_PROTOCOL.Name:               "mysql",
		property_util.GDB_FAILOVER_HOME_REGION.Name:      "us-east-2",
		property_util.GDB_FAILOVER_ACTIVE_HOME_MODE.Name: "invalid-mode",
		property_util.ENABLE_CONNECT_FAILOVER.Name:       "false",
	}, []*host_info_util.HostInfo{gdbWriterHost}, false, host_info_util.WRITER, true, nil)

	err := setup.plugin.InitFailoverMode()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid GlobalDbFailoverMode")
}

func TestGdbInitFailoverModeAllValidModes(t *testing.T) {
	modes := []string{
		"strict-writer", "strict-home-reader", "strict-out-of-home-reader",
		"strict-any-reader", "home-reader-or-writer",
		"out-of-home-reader-or-writer", "any-reader-or-writer",
	}
	for _, mode := range modes {
		t.Run(mode, func(t *testing.T) {
			setup := createGdbPlugin(t, map[string]string{
				property_util.DRIVER_PROTOCOL.Name:                 "mysql",
				property_util.GDB_FAILOVER_HOME_REGION.Name:        "us-east-2",
				property_util.GDB_FAILOVER_ACTIVE_HOME_MODE.Name:   mode,
				property_util.GDB_FAILOVER_INACTIVE_HOME_MODE.Name: mode,
				property_util.ENABLE_CONNECT_FAILOVER.Name:         "false",
			}, []*host_info_util.HostInfo{gdbWriterHost, gdbReaderHost}, false, host_info_util.WRITER, true, nil)

			err := setup.plugin.InitFailoverMode()
			assert.NoError(t, err)
		})
	}
}

func TestGdbFailoverStrictWriter(t *testing.T) {
	setup := createGdbPlugin(t, map[string]string{
		property_util.DRIVER_PROTOCOL.Name:                 "mysql",
		property_util.GDB_FAILOVER_HOME_REGION.Name:        "us-east-2",
		property_util.GDB_FAILOVER_ACTIVE_HOME_MODE.Name:   "strict-writer",
		property_util.GDB_FAILOVER_INACTIVE_HOME_MODE.Name: "strict-writer",
		property_util.ENABLE_CONNECT_FAILOVER.Name:         "false",
	}, []*host_info_util.HostInfo{gdbWriterHost, gdbReaderHost}, false, host_info_util.WRITER, true, nil)
	setup.plugin.InitFailoverMode()

	err := setup.plugin.Failover()
	assert.Error(t, err)
	assert.True(t, errors.Is(err, error_util.FailoverSuccessError))
}

func TestGdbFailoverStrictWriterInTransaction(t *testing.T) {
	setup := createGdbPlugin(t, map[string]string{
		property_util.DRIVER_PROTOCOL.Name:                 "mysql",
		property_util.GDB_FAILOVER_HOME_REGION.Name:        "us-east-2",
		property_util.GDB_FAILOVER_ACTIVE_HOME_MODE.Name:   "strict-writer",
		property_util.GDB_FAILOVER_INACTIVE_HOME_MODE.Name: "strict-writer",
		property_util.ENABLE_CONNECT_FAILOVER.Name:         "false",
	}, []*host_info_util.HostInfo{gdbWriterHost, gdbReaderHost}, true, host_info_util.WRITER, true, nil)
	setup.plugin.InitFailoverMode()

	err := setup.plugin.Failover()
	assert.Error(t, err)
	assert.True(t, errors.Is(err, error_util.TransactionResolutionUnknownError))
}

func TestGdbFailoverStrictWriterConnectionFails(t *testing.T) {
	setup := createGdbPlugin(t, map[string]string{
		property_util.DRIVER_PROTOCOL.Name:                 "mysql",
		property_util.GDB_FAILOVER_HOME_REGION.Name:        "us-east-2",
		property_util.GDB_FAILOVER_ACTIVE_HOME_MODE.Name:   "strict-writer",
		property_util.GDB_FAILOVER_INACTIVE_HOME_MODE.Name: "strict-writer",
		property_util.ENABLE_CONNECT_FAILOVER.Name:         "false",
	}, []*host_info_util.HostInfo{gdbWriterHost, gdbReaderHost}, false, host_info_util.WRITER, true,
		errors.New("connection refused"))
	setup.plugin.InitFailoverMode()

	err := setup.plugin.Failover()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "connect")
}

func TestGdbFailoverStrictWriterIncorrectRole(t *testing.T) {
	setup := createGdbPlugin(t, map[string]string{
		property_util.DRIVER_PROTOCOL.Name:                 "mysql",
		property_util.GDB_FAILOVER_HOME_REGION.Name:        "us-east-2",
		property_util.GDB_FAILOVER_ACTIVE_HOME_MODE.Name:   "strict-writer",
		property_util.GDB_FAILOVER_INACTIVE_HOME_MODE.Name: "strict-writer",
		property_util.ENABLE_CONNECT_FAILOVER.Name:         "false",
	}, []*host_info_util.HostInfo{gdbWriterHost, gdbReaderHost}, false, host_info_util.READER, true, nil)
	setup.plugin.InitFailoverMode()

	err := setup.plugin.Failover()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "reader")
}

func TestGdbFailoverTopologyRefreshFails(t *testing.T) {
	setup := createGdbPlugin(t, map[string]string{
		property_util.DRIVER_PROTOCOL.Name:                 "mysql",
		property_util.GDB_FAILOVER_HOME_REGION.Name:        "us-east-2",
		property_util.GDB_FAILOVER_ACTIVE_HOME_MODE.Name:   "strict-writer",
		property_util.GDB_FAILOVER_INACTIVE_HOME_MODE.Name: "strict-writer",
		property_util.ENABLE_CONNECT_FAILOVER.Name:         "false",
	}, []*host_info_util.HostInfo{gdbWriterHost, gdbReaderHost}, false, host_info_util.WRITER, false, nil)
	setup.plugin.InitFailoverMode()

	err := setup.plugin.Failover()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "topology")
}

func TestGdbFailoverStrictHomeReader(t *testing.T) {
	setup := createGdbPlugin(t, map[string]string{
		property_util.DRIVER_PROTOCOL.Name:                 "mysql",
		property_util.GDB_FAILOVER_HOME_REGION.Name:        "us-east-2",
		property_util.GDB_FAILOVER_ACTIVE_HOME_MODE.Name:   "strict-home-reader",
		property_util.GDB_FAILOVER_INACTIVE_HOME_MODE.Name: "strict-home-reader",
		property_util.ENABLE_CONNECT_FAILOVER.Name:         "false",
	}, []*host_info_util.HostInfo{gdbWriterHost, gdbReaderHost, gdbOutOfHomeReaderHost}, false, host_info_util.READER, true, nil)
	setup.plugin.InitFailoverMode()

	err := setup.plugin.Failover()
	assert.Error(t, err)
	assert.True(t, errors.Is(err, error_util.FailoverSuccessError))
}

func TestGdbFailoverStrictOutOfHomeReader(t *testing.T) {
	setup := createGdbPlugin(t, map[string]string{
		property_util.DRIVER_PROTOCOL.Name:                 "mysql",
		property_util.GDB_FAILOVER_HOME_REGION.Name:        "us-east-2",
		property_util.GDB_FAILOVER_ACTIVE_HOME_MODE.Name:   "strict-out-of-home-reader",
		property_util.GDB_FAILOVER_INACTIVE_HOME_MODE.Name: "strict-out-of-home-reader",
		property_util.ENABLE_CONNECT_FAILOVER.Name:         "false",
	}, []*host_info_util.HostInfo{gdbWriterHost, gdbReaderHost, gdbOutOfHomeReaderHost}, false, host_info_util.READER, true, nil)
	setup.plugin.InitFailoverMode()

	err := setup.plugin.Failover()
	assert.Error(t, err)
	assert.True(t, errors.Is(err, error_util.FailoverSuccessError))
}

func TestGdbFailoverStrictAnyReader(t *testing.T) {
	setup := createGdbPlugin(t, map[string]string{
		property_util.DRIVER_PROTOCOL.Name:                 "mysql",
		property_util.GDB_FAILOVER_HOME_REGION.Name:        "us-east-2",
		property_util.GDB_FAILOVER_ACTIVE_HOME_MODE.Name:   "strict-any-reader",
		property_util.GDB_FAILOVER_INACTIVE_HOME_MODE.Name: "strict-any-reader",
		property_util.ENABLE_CONNECT_FAILOVER.Name:         "false",
	}, []*host_info_util.HostInfo{gdbWriterHost, gdbReaderHost, gdbOutOfHomeReaderHost}, false, host_info_util.READER, true, nil)
	setup.plugin.InitFailoverMode()

	err := setup.plugin.Failover()
	assert.Error(t, err)
	assert.True(t, errors.Is(err, error_util.FailoverSuccessError))
}

func TestGdbFailoverStrictReaderConnectionFails(t *testing.T) {
	setup := createGdbPlugin(t, map[string]string{
		property_util.DRIVER_PROTOCOL.Name:                 "mysql",
		property_util.GDB_FAILOVER_HOME_REGION.Name:        "us-east-2",
		property_util.GDB_FAILOVER_ACTIVE_HOME_MODE.Name:   "strict-home-reader",
		property_util.GDB_FAILOVER_INACTIVE_HOME_MODE.Name: "strict-home-reader",
		property_util.ENABLE_CONNECT_FAILOVER.Name:         "false",
		property_util.FAILOVER_TIMEOUT_MS.Name:             "2000",
	}, []*host_info_util.HostInfo{gdbWriterHost, gdbReaderHost}, false, host_info_util.READER, true,
		errors.New("connection refused"))
	setup.plugin.InitFailoverMode()

	err := setup.plugin.Failover()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "Unable to establish SQL connection to the reader")
}

func TestGdbFailoverHomeReaderOrWriter(t *testing.T) {
	setup := createGdbPlugin(t, map[string]string{
		property_util.DRIVER_PROTOCOL.Name:                 "mysql",
		property_util.GDB_FAILOVER_HOME_REGION.Name:        "us-east-2",
		property_util.GDB_FAILOVER_ACTIVE_HOME_MODE.Name:   "home-reader-or-writer",
		property_util.GDB_FAILOVER_INACTIVE_HOME_MODE.Name: "home-reader-or-writer",
		property_util.ENABLE_CONNECT_FAILOVER.Name:         "false",
	}, []*host_info_util.HostInfo{gdbWriterHost, gdbReaderHost, gdbOutOfHomeReaderHost}, false, host_info_util.READER, true, nil)
	setup.plugin.InitFailoverMode()

	err := setup.plugin.Failover()
	assert.Error(t, err)
	assert.True(t, errors.Is(err, error_util.FailoverSuccessError))
}

func TestGdbFailoverHomeReaderOrWriterWithWriterRole(t *testing.T) {
	setup := createGdbPlugin(t, map[string]string{
		property_util.DRIVER_PROTOCOL.Name:                 "mysql",
		property_util.GDB_FAILOVER_HOME_REGION.Name:        "us-east-2",
		property_util.GDB_FAILOVER_ACTIVE_HOME_MODE.Name:   "home-reader-or-writer",
		property_util.GDB_FAILOVER_INACTIVE_HOME_MODE.Name: "home-reader-or-writer",
		property_util.ENABLE_CONNECT_FAILOVER.Name:         "false",
	}, []*host_info_util.HostInfo{gdbWriterHost, gdbReaderHost}, false, host_info_util.WRITER, true, nil)
	setup.plugin.InitFailoverMode()

	err := setup.plugin.Failover()
	assert.Error(t, err)
	assert.True(t, errors.Is(err, error_util.FailoverSuccessError))
}

func TestGdbFailoverOutOfHomeReaderOrWriter(t *testing.T) {
	setup := createGdbPlugin(t, map[string]string{
		property_util.DRIVER_PROTOCOL.Name:                 "mysql",
		property_util.GDB_FAILOVER_HOME_REGION.Name:        "us-east-2",
		property_util.GDB_FAILOVER_ACTIVE_HOME_MODE.Name:   "out-of-home-reader-or-writer",
		property_util.GDB_FAILOVER_INACTIVE_HOME_MODE.Name: "out-of-home-reader-or-writer",
		property_util.ENABLE_CONNECT_FAILOVER.Name:         "false",
	}, []*host_info_util.HostInfo{gdbWriterHost, gdbReaderHost, gdbOutOfHomeReaderHost}, false, host_info_util.READER, true, nil)
	setup.plugin.InitFailoverMode()

	err := setup.plugin.Failover()
	assert.Error(t, err)
	assert.True(t, errors.Is(err, error_util.FailoverSuccessError))
}

func TestGdbFailoverAnyReaderOrWriter(t *testing.T) {
	setup := createGdbPlugin(t, map[string]string{
		property_util.DRIVER_PROTOCOL.Name:                 "mysql",
		property_util.GDB_FAILOVER_HOME_REGION.Name:        "us-east-2",
		property_util.GDB_FAILOVER_ACTIVE_HOME_MODE.Name:   "any-reader-or-writer",
		property_util.GDB_FAILOVER_INACTIVE_HOME_MODE.Name: "any-reader-or-writer",
		property_util.ENABLE_CONNECT_FAILOVER.Name:         "false",
	}, []*host_info_util.HostInfo{gdbWriterHost, gdbReaderHost, gdbOutOfHomeReaderHost}, false, host_info_util.WRITER, true, nil)
	setup.plugin.InitFailoverMode()

	err := setup.plugin.Failover()
	assert.Error(t, err)
	assert.True(t, errors.Is(err, error_util.FailoverSuccessError))
}

func TestGdbFailoverReaderOrWriterConnectionFails(t *testing.T) {
	setup := createGdbPlugin(t, map[string]string{
		property_util.DRIVER_PROTOCOL.Name:                 "mysql",
		property_util.GDB_FAILOVER_HOME_REGION.Name:        "us-east-2",
		property_util.GDB_FAILOVER_ACTIVE_HOME_MODE.Name:   "any-reader-or-writer",
		property_util.GDB_FAILOVER_INACTIVE_HOME_MODE.Name: "any-reader-or-writer",
		property_util.ENABLE_CONNECT_FAILOVER.Name:         "false",
		property_util.FAILOVER_TIMEOUT_MS.Name:             "2000",
	}, []*host_info_util.HostInfo{gdbWriterHost, gdbReaderHost}, false, host_info_util.WRITER, true,
		errors.New("connection refused"))
	setup.plugin.InitFailoverMode()

	err := setup.plugin.Failover()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "Unable to establish SQL connection to the reader")
}

func TestGdbFailoverInactiveHomeRegion(t *testing.T) {
	writerInWest, _ := host_info_util.NewHostInfoBuilder().
		SetHost("mydatabase-instance-1.xyz.us-west-2.rds.amazonaws.com").
		SetPort(3306).SetRole(host_info_util.WRITER).Build()
	readerInEast, _ := host_info_util.NewHostInfoBuilder().
		SetHost("mydatabase-instance-2.xyz.us-east-2.rds.amazonaws.com").
		SetPort(3306).SetRole(host_info_util.READER).Build()

	setup := createGdbPlugin(t, map[string]string{
		property_util.DRIVER_PROTOCOL.Name:                 "mysql",
		property_util.GDB_FAILOVER_HOME_REGION.Name:        "us-east-2",
		property_util.GDB_FAILOVER_ACTIVE_HOME_MODE.Name:   "strict-home-reader",
		property_util.GDB_FAILOVER_INACTIVE_HOME_MODE.Name: "strict-writer",
		property_util.ENABLE_CONNECT_FAILOVER.Name:         "false",
	}, []*host_info_util.HostInfo{writerInWest, readerInEast}, false, host_info_util.WRITER, true, nil)
	setup.plugin.InitFailoverMode()

	err := setup.plugin.Failover()
	assert.Error(t, err)
	assert.True(t, errors.Is(err, error_util.FailoverSuccessError))
}

func TestGdbFailoverInactiveHomeRegionReaderMode(t *testing.T) {
	writerInWest, _ := host_info_util.NewHostInfoBuilder().
		SetHost("mydatabase-instance-1.xyz.us-west-2.rds.amazonaws.com").
		SetPort(3306).SetRole(host_info_util.WRITER).Build()
	readerInEast, _ := host_info_util.NewHostInfoBuilder().
		SetHost("mydatabase-instance-2.xyz.us-east-2.rds.amazonaws.com").
		SetPort(3306).SetRole(host_info_util.READER).Build()

	setup := createGdbPlugin(t, map[string]string{
		property_util.DRIVER_PROTOCOL.Name:                 "mysql",
		property_util.GDB_FAILOVER_HOME_REGION.Name:        "us-east-2",
		property_util.GDB_FAILOVER_ACTIVE_HOME_MODE.Name:   "strict-writer",
		property_util.GDB_FAILOVER_INACTIVE_HOME_MODE.Name: "strict-any-reader",
		property_util.ENABLE_CONNECT_FAILOVER.Name:         "false",
	}, []*host_info_util.HostInfo{writerInWest, readerInEast}, false, host_info_util.READER, true, nil)
	setup.plugin.InitFailoverMode()

	err := setup.plugin.Failover()
	assert.Error(t, err)
	assert.True(t, errors.Is(err, error_util.FailoverSuccessError))
}
