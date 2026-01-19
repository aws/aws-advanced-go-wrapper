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
	"sync/atomic"
	"testing"

	mock_driver_infrastructure "github.com/aws/aws-advanced-go-wrapper/.test/test/mocks/awssql/driver_infrastructure"
	mock_database_sql_driver "github.com/aws/aws-advanced-go-wrapper/.test/test/mocks/database_sql_driver"
	"github.com/aws/aws-advanced-go-wrapper/awssql/driver_infrastructure"
	"github.com/aws/aws-advanced-go-wrapper/awssql/host_info_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/plugin_helpers"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils/telemetry"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

func preparePartialPluginService(t *testing.T) (*plugin_helpers.PartialPluginService, *MockPluginManager, *host_info_util.HostInfoBuilder) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	someProps := MakeMapFromKeysAndVals(
		"someKey", "someVal",
	)
	mockTargetDriver := &MockTargetDriver{}
	telemetryFactory, _ := telemetry.NewDefaultTelemetryFactory(someProps)
	mockPluginManager := &MockPluginManager{
		plugin_helpers.NewPluginManagerImpl(mockTargetDriver, someProps, driver_infrastructure.ConnectionProviderManager{}, telemetryFactory), nil, nil}
	var mockCurrentConn driver.Conn = mock_database_sql_driver.NewMockConn(ctrl)
	mockHostListProvider := mock_driver_infrastructure.NewMockHostListProvider(ctrl)
	mockDialect := mock_driver_infrastructure.NewMockDatabaseDialect(ctrl)
	mockDriverDialect := mock_driver_infrastructure.NewMockDriverDialect(ctrl)
	someAllHosts := []*host_info_util.HostInfo{}
	mockAllowedAndBlockedHosts := new(atomic.Pointer[driver_infrastructure.AllowedAndBlockedHosts])
	partialPluginService := plugin_helpers.NewPartialPluginService(
		mockPluginManager,
		someProps,
		&mockCurrentConn,
		mockHostListProvider,
		mockDialect,
		mockDriverDialect,
		someAllHosts,
		mockAllowedAndBlockedHosts)
	return partialPluginService.(*plugin_helpers.PartialPluginService), mockPluginManager, host_info_util.NewHostInfoBuilder()
}

func TestForceConnect_PartialPluginService(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockPluginManager := mock_driver_infrastructure.NewMockPluginManager(ctrl)
	someProps := MakeMapFromKeysAndVals(
		"someKey", "someVal",
	)
	var mockCurrentConn driver.Conn = mock_database_sql_driver.NewMockConn(ctrl)

	someHost := &host_info_util.HostInfo{}
	expectedConn := mock_database_sql_driver.NewMockConn(ctrl)

	partialPluginService := plugin_helpers.NewPartialPluginService(
		mockPluginManager,
		someProps,
		&mockCurrentConn,
		mock_driver_infrastructure.NewMockHostListProvider(ctrl),
		mock_driver_infrastructure.NewMockDatabaseDialect(ctrl),
		mock_driver_infrastructure.NewMockDriverDialect(ctrl),
		[]*host_info_util.HostInfo{},
		new(atomic.Pointer[driver_infrastructure.AllowedAndBlockedHosts]))

	mockPluginManager.EXPECT().ForceConnect(someHost, someProps, false).Return(expectedConn, nil)
	actualConn, err := partialPluginService.ForceConnect(someHost, someProps)

	assert.Equal(t, expectedConn, actualConn)
	assert.NoError(t, err)
}

func TestGetHostSelectorStrategy_PartialPluginService(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockPluginManager := mock_driver_infrastructure.NewMockPluginManager(ctrl)
	someProps := MakeMapFromKeysAndVals(
		"someKey", "someVal",
	)
	var mockCurrentConn driver.Conn = mock_database_sql_driver.NewMockConn(ctrl)

	someStrategy := "someStrategy"
	expectedHostSelector := &driver_infrastructure.HighestWeightHostSelector{}

	partialPluginService := plugin_helpers.NewPartialPluginService(
		mockPluginManager,
		someProps,
		&mockCurrentConn,
		mock_driver_infrastructure.NewMockHostListProvider(ctrl),
		mock_driver_infrastructure.NewMockDatabaseDialect(ctrl),
		mock_driver_infrastructure.NewMockDriverDialect(ctrl),
		[]*host_info_util.HostInfo{},
		new(atomic.Pointer[driver_infrastructure.AllowedAndBlockedHosts]))

	mockPluginManager.EXPECT().GetHostSelectorStrategy(someStrategy).Return(expectedHostSelector, nil)
	actualHostSelector, err := partialPluginService.GetHostSelectorStrategy(someStrategy)

	assert.Equal(t, expectedHostSelector, actualHostSelector)
	assert.NoError(t, err)
}

func TestCreateHostListProvider_PartialPluginService(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	someProps := MakeMapFromKeysAndVals(
		"someKey", "someVal",
	)
	var mockCurrentConn driver.Conn = mock_database_sql_driver.NewMockConn(ctrl)

	mockDatabaseDialect := mock_driver_infrastructure.NewMockDatabaseDialect(ctrl)

	partialPluginService := plugin_helpers.NewPartialPluginService(
		mock_driver_infrastructure.NewMockPluginManager(ctrl),
		someProps, &mockCurrentConn,
		mock_driver_infrastructure.NewMockHostListProvider(ctrl),
		mockDatabaseDialect, mock_driver_infrastructure.NewMockDriverDialect(ctrl),
		[]*host_info_util.HostInfo{},
		new(atomic.Pointer[driver_infrastructure.AllowedAndBlockedHosts]))

	expectedHostListProvider := mock_driver_infrastructure.NewMockHostListProvider(ctrl)
	mockDatabaseDialect.EXPECT().GetHostListProvider(
		someProps,
		driver_infrastructure.HostListProviderService(partialPluginService),
		partialPluginService).Return(expectedHostListProvider)
	actualHostListProvider := partialPluginService.CreateHostListProvider(someProps)

	assert.Equal(t, expectedHostListProvider, actualHostListProvider)
}

func TestHostAvailability_PartialPluginService_WhenHostAvailabilityWentUp(t *testing.T) {
	target, mockPluginManager, hostInfoBuilder := preparePartialPluginService(t)

	hostA, err := hostInfoBuilder.SetHost("hostA").SetRole(host_info_util.WRITER).SetAvailability(host_info_util.UNAVAILABLE).Build()
	assert.Nil(t, err)
	target.AllHosts = []*host_info_util.HostInfo{hostA}
	target.SetAvailability(map[string]bool{"hostA": true}, host_info_util.AVAILABLE)

	assert.Equal(t, 1, len(target.GetHosts()))
	assert.Equal(t, host_info_util.AVAILABLE, target.GetHosts()[0].Availability)

	assert.Equal(t, 1, len(mockPluginManager.Changes))
	hostAChangeMap, hostAInChangeMap := mockPluginManager.Changes["hostA"]
	assert.True(t, hostAInChangeMap)
	assert.Equal(t, 2, len(hostAChangeMap))
	_, changedHostA := hostAChangeMap[driver_infrastructure.HOST_CHANGED]
	_, wentUpHostA := hostAChangeMap[driver_infrastructure.WENT_UP]
	assert.True(t, changedHostA)
	assert.True(t, wentUpHostA)
}

func TestHostAvailability_PartialPluginService_WhenHostAvailabilityWentDown(t *testing.T) {
	target, mockPluginManager, hostInfoBuilder := preparePartialPluginService(t)

	hostA, err := hostInfoBuilder.SetHost("hostA").SetRole(host_info_util.WRITER).SetAvailability(host_info_util.AVAILABLE).Build()
	assert.Nil(t, err)
	target.AllHosts = []*host_info_util.HostInfo{hostA}
	target.SetAvailability(map[string]bool{"hostA": true}, host_info_util.UNAVAILABLE)

	assert.Equal(t, 1, len(target.GetHosts()))
	assert.Equal(t, host_info_util.UNAVAILABLE, target.GetHosts()[0].Availability)

	assert.Equal(t, 1, len(mockPluginManager.Changes))
	hostAChangeMap, hostAInChangeMap := mockPluginManager.Changes["hostA"]
	assert.True(t, hostAInChangeMap)
	assert.Equal(t, 2, len(hostAChangeMap))
	_, changedHostA := hostAChangeMap[driver_infrastructure.HOST_CHANGED]
	_, wentDownHostA := hostAChangeMap[driver_infrastructure.WENT_DOWN]
	assert.True(t, changedHostA)
	assert.True(t, wentDownHostA)
}

func TestHostAvailability_PartialPluginService_WhenHostAvailabilityWentUpByAlias(t *testing.T) {
	target, mockPluginManager, hostInfoBuilder := preparePartialPluginService(t)

	hostA, err := hostInfoBuilder.SetHost("hostA").SetRole(host_info_util.READER).SetAvailability(host_info_util.UNAVAILABLE).Build()
	assert.Nil(t, err)
	hostA.AddAlias("ip-10-10-10-10")
	hostA.AddAlias("hostA.custom.domain.com")
	hostB, err := hostInfoBuilder.SetHost("hostB").SetRole(host_info_util.READER).SetAvailability(host_info_util.UNAVAILABLE).Build()
	assert.Nil(t, err)
	hostB.AddAlias("ip-10-10-10-10")
	hostB.AddAlias("hostB.custom.domain.com")

	target.AllHosts = []*host_info_util.HostInfo{hostA, hostB}
	target.SetAvailability(map[string]bool{"hostA.custom.domain.com": true}, host_info_util.AVAILABLE)

	assert.Equal(t, 2, len(target.GetHosts()))
	assert.Equal(t, host_info_util.AVAILABLE, target.GetHosts()[0].Availability)
	assert.Equal(t, host_info_util.UNAVAILABLE, target.GetHosts()[1].Availability)

	hostAChangeMap, hostAInChangeMap := mockPluginManager.Changes["hostA"]
	assert.True(t, hostAInChangeMap)
	assert.Equal(t, 2, len(hostAChangeMap))
	_, changedHostA := hostAChangeMap[driver_infrastructure.HOST_CHANGED]
	_, wentUpHostA := hostAChangeMap[driver_infrastructure.WENT_UP]
	assert.True(t, changedHostA)
	assert.True(t, wentUpHostA)

	_, hostBInChangeMap := mockPluginManager.Changes["hostB"]
	assert.False(t, hostBInChangeMap)
}

func TestHostAvailability_PartialPluginService_WhenHostAvailabilityWentUpMultipleHostsByAlias(t *testing.T) {
	target, mockPluginManager, hostInfoBuilder := preparePartialPluginService(t)

	hostA, err := hostInfoBuilder.SetHost("hostA").SetRole(host_info_util.READER).SetAvailability(host_info_util.UNAVAILABLE).Build()
	assert.Nil(t, err)
	hostA.AddAlias("ip-10-10-10-10")
	hostA.AddAlias("hostA.custom.domain.com")
	hostB, err := hostInfoBuilder.SetHost("hostB").SetRole(host_info_util.READER).SetAvailability(host_info_util.UNAVAILABLE).Build()
	assert.Nil(t, err)
	hostB.AddAlias("ip-10-10-10-10")
	hostB.AddAlias("hostB.custom.domain.com")

	target.AllHosts = []*host_info_util.HostInfo{hostA, hostB}
	target.SetAvailability(map[string]bool{"ip-10-10-10-10": true}, host_info_util.AVAILABLE)

	assert.Equal(t, 2, len(target.GetHosts()))
	assert.Equal(t, host_info_util.AVAILABLE, target.GetHosts()[0].Availability)
	assert.Equal(t, host_info_util.AVAILABLE, target.GetHosts()[1].Availability)

	hostAChangeMap, hostAInChangeMap := mockPluginManager.Changes["hostA"]
	assert.True(t, hostAInChangeMap)
	assert.Equal(t, 2, len(hostAChangeMap))
	_, changedHostA := hostAChangeMap[driver_infrastructure.HOST_CHANGED]
	_, wentUpHostA := hostAChangeMap[driver_infrastructure.WENT_UP]
	assert.True(t, changedHostA)
	assert.True(t, wentUpHostA)

	hostBChangeMap, hostBInChangeMap := mockPluginManager.Changes["hostB"]
	assert.True(t, hostBInChangeMap)
	assert.Equal(t, 2, len(hostBChangeMap))
	_, changedHostB := hostBChangeMap[driver_infrastructure.HOST_CHANGED]
	_, wentUpHostB := hostAChangeMap[driver_infrastructure.WENT_UP]
	assert.True(t, changedHostB)
	assert.True(t, wentUpHostB)
}

func TestGetCurrentConnection_PartialPluginService_Panics(t *testing.T) {
	target, _, _ := preparePartialPluginService(t)
	assert.Panics(t, func() { target.GetCurrentConnection() })
}

func TestGetCurrentConnectionRef_PartialPluginService_Panics(t *testing.T) {
	target, _, _ := preparePartialPluginService(t)
	assert.Panics(t, func() { target.GetCurrentConnectionRef() })
}

func TestSetCurrentConnection_PartialPluginService_Panics(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	target, _, _ := preparePartialPluginService(t)

	inputConn := mock_database_sql_driver.NewMockConn(ctrl)
	inputHostInfo := &host_info_util.HostInfo{}

	assert.Panics(t, func() { _ = target.SetCurrentConnection(inputConn, inputHostInfo, nil) })
}

func TestGetInitialConnectionHostInfo_PartialPluginService_Panics(t *testing.T) {
	target, _, _ := preparePartialPluginService(t)
	assert.Panics(t, func() { target.GetInitialConnectionHostInfo() })
}

func TestGetCurrentHostInfo_PartialPluginService_Panics(t *testing.T) {
	target, _, _ := preparePartialPluginService(t)
	assert.Panics(t, func() { _, _ = target.GetCurrentHostInfo() })
}

func TestAcceptsStrategy_PartialPluginService_Panics(t *testing.T) {
	target, _, _ := preparePartialPluginService(t)
	assert.Panics(t, func() { target.AcceptsStrategy("someString") })
}

func TestGetHostInfoByStrategy_PartialPluginService_Panics(t *testing.T) {
	target, _, _ := preparePartialPluginService(t)
	assert.Panics(t, func() {
		_, _ = target.GetHostInfoByStrategy(host_info_util.READER, "someStrategy", []*host_info_util.HostInfo{})
	})
}

func TestGetHostRole_PartialPluginService_Panics(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	inputConn := mock_database_sql_driver.NewMockConn(ctrl)
	target, _, _ := preparePartialPluginService(t)
	assert.Panics(t, func() { target.GetHostRole(inputConn) })
}

func TestIsInTransaction_PartialPluginService_Panics(t *testing.T) {
	target, _, _ := preparePartialPluginService(t)
	assert.Panics(t, func() { target.IsInTransaction() })
}

func TestSetInTransaction_PartialPluginService_Panics(t *testing.T) {
	target, _, _ := preparePartialPluginService(t)
	assert.Panics(t, func() { target.SetInTransaction(true) })
}

func TestGetCurrentTx_PartialPluginService_Panics(t *testing.T) {
	target, _, _ := preparePartialPluginService(t)
	assert.Panics(t, func() { target.GetCurrentTx() })
}

func TestSetCurrentTx_PartialPluginService_Panics(t *testing.T) {
	target, _, _ := preparePartialPluginService(t)
	assert.Panics(t, func() { target.SetCurrentTx(*new(driver.Tx)) })
}

func TestSetHostListProvider_PartialPluginService_Panics(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	target, _, _ := preparePartialPluginService(t)
	assert.Panics(t, func() { target.SetHostListProvider(mock_driver_infrastructure.NewMockHostListProvider(ctrl)) })
}

func TestSetInitialConnectionHostInfo_PartialPluginService_Panics(t *testing.T) {
	target, _, _ := preparePartialPluginService(t)
	assert.Panics(t, func() { target.SetInitialConnectionHostInfo(&host_info_util.HostInfo{}) })
}

func TestIsStaticHostListProvider_PartialPluginService_Panics(t *testing.T) {
	target, _, _ := preparePartialPluginService(t)
	assert.Panics(t, func() { target.IsStaticHostListProvider() })
}

func TestGetHostListProvider_PartialPluginService_Panics(t *testing.T) {
	target, _, _ := preparePartialPluginService(t)
	assert.Panics(t, func() { target.GetHostListProvider() })
}

func TestRefreshHostList_PartialPluginService_Panics(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	target, _, _ := preparePartialPluginService(t)
	assert.Panics(t, func() { _ = target.RefreshHostList(mock_database_sql_driver.NewMockConn(ctrl)) })
}

func TestForceRefreshHostList_PartialPluginService_Panics(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	target, _, _ := preparePartialPluginService(t)
	assert.Panics(t, func() { _ = target.ForceRefreshHostList(mock_database_sql_driver.NewMockConn(ctrl)) })
}

func TestForceRefreshHostListWithTimeout_PartialPluginService_Panics(t *testing.T) {
	target, _, _ := preparePartialPluginService(t)
	assert.Panics(t, func() { _, _ = target.ForceRefreshHostListWithTimeout(true, 555) })
}

func TestGetUpdatedHostListWithTimeout_PartialPluginService_Panics(t *testing.T) {
	target, _, _ := preparePartialPluginService(t)
	assert.Panics(t, func() { _, _ = target.GetUpdatedHostListWithTimeout(true, 555) })
}

func TestConnect_PartialPluginService_Panics(t *testing.T) {
	target, _, _ := preparePartialPluginService(t)
	assert.Panics(t, func() {
		_, _ = target.Connect(&host_info_util.HostInfo{}, MakeMapFromKeysAndVals("someKey", "someValue"), nil)
	})
}

func TestSetDialect_PartialPluginService_Panics(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	target, _, _ := preparePartialPluginService(t)
	assert.Panics(t, func() { target.SetDialect(mock_driver_infrastructure.NewMockDatabaseDialect(ctrl)) })
}

func TestUpdateDialect_PartialPluginService_Panics(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	target, _, _ := preparePartialPluginService(t)
	assert.Panics(t, func() { target.UpdateDialect(mock_database_sql_driver.NewMockConn(ctrl)) })
}

func TestIdentifyConnection_PartialPluginService_Panics(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	target, _, _ := preparePartialPluginService(t)
	assert.Panics(t, func() { _, _ = target.IdentifyConnection(mock_database_sql_driver.NewMockConn(ctrl)) })
}

func TestFillAliases_PartialPluginService_Panics(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	target, _, _ := preparePartialPluginService(t)
	assert.Panics(t, func() { target.FillAliases(mock_database_sql_driver.NewMockConn(ctrl), &host_info_util.HostInfo{}) })
}

func TestGetConnectionProvider_PartialPluginService_Panics(t *testing.T) {
	target, _, _ := preparePartialPluginService(t)
	assert.Panics(t, func() { target.GetConnectionProvider() })
}

func TestGetProperties_PartialPluginService_Panics(t *testing.T) {
	target, _, _ := preparePartialPluginService(t)
	assert.Panics(t, func() { target.GetProperties() })
}

func TestIsNetworkError_PartialPluginService_Panics(t *testing.T) {
	target, _, _ := preparePartialPluginService(t)
	assert.Panics(t, func() { target.IsNetworkError(errors.New("someError")) })
}

func TestIsLoginError_PartialPluginService_Panics(t *testing.T) {
	target, _, _ := preparePartialPluginService(t)
	assert.Panics(t, func() { target.IsLoginError(errors.New("someError")) })
}

func TestGetTelemetryFactory_PartialPluginService_Panics(t *testing.T) {
	target, _, _ := preparePartialPluginService(t)
	assert.Panics(t, func() { target.GetTelemetryFactory() })
}

func TestUpdateState_PartialPluginService_Panics(t *testing.T) {
	target, _, _ := preparePartialPluginService(t)
	assert.Panics(t, func() { target.UpdateState("someSql", "someMethodArgs") })
}

func TestGetBgStatus_PartialPluginService_Panics(t *testing.T) {
	target, _, _ := preparePartialPluginService(t)
	assert.Panics(t, func() { target.GetBgStatus("someId") })
}

func TestSetBgStatus_PartialPluginService_Panics(t *testing.T) {
	target, _, _ := preparePartialPluginService(t)
	assert.Panics(t, func() { target.SetBgStatus(driver_infrastructure.BlueGreenStatus{}, "someId") })
}

func TestIsPluginInUse_PartialPluginService_Panics(t *testing.T) {
	target, _, _ := preparePartialPluginService(t)
	assert.Panics(t, func() { target.IsPluginInUse("somePluginName") })
}

func TestResetSession_PartialPluginService_Panics(t *testing.T) {
	target, _, _ := preparePartialPluginService(t)
	assert.Panics(t, func() { target.ResetSession() })
}
