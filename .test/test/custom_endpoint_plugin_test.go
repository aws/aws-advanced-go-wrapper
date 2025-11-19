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
	"time"

	mock_driver_infrastructure "github.com/aws/aws-advanced-go-wrapper/.test/test/mocks/awssql/driver_infrastructure"
	mock_telemetry "github.com/aws/aws-advanced-go-wrapper/.test/test/mocks/awssql/util/telemetry"
	mock_custom_endpoint "github.com/aws/aws-advanced-go-wrapper/.test/test/mocks/custom-endpoint"
	"github.com/aws/aws-advanced-go-wrapper/awssql/error_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/host_info_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/property_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils"
	custom_endpoint "github.com/aws/aws-advanced-go-wrapper/custom-endpoint"
	"github.com/aws/aws-sdk-go-v2/service/rds"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

func createMocks(ctrl *gomock.Controller) (
	mockPluginService *mock_driver_infrastructure.MockPluginService,
	mockTelemetryFactory *mock_telemetry.MockTelemetryFactory,
	mockTelemetryCounter *mock_telemetry.MockTelemetryCounter,
	mockMonitor *mock_custom_endpoint.MockCustomEndpointMonitor) {
	mockPluginService = mock_driver_infrastructure.NewMockPluginService(ctrl)
	mockTelemetryFactory = mock_telemetry.NewMockTelemetryFactory(ctrl)
	mockTelemetryCounter = mock_telemetry.NewMockTelemetryCounter(ctrl)
	mockMonitor = mock_custom_endpoint.NewMockCustomEndpointMonitor(ctrl)
	return
}

func TestCustomEndpointPluginConnect_InvalidUrl(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockPluginService, mockTelemetryFactory, mockTelemetryCounter, _ := createMocks(ctrl)

	mockPluginService.EXPECT().GetTelemetryFactory().Return(mockTelemetryFactory)
	mockTelemetryFactory.EXPECT().CreateCounter(custom_endpoint.TELEMETRY_WAIT_FOR_INFO_COUNTER).Return(mockTelemetryCounter, nil)

	props := utils.NewRWMap[string, string]()
	rdsClientFunc := func(*host_info_util.HostInfo, *utils.RWMap[string, string]) (*rds.Client, error) { return nil, nil }

	plugin, err := custom_endpoint.NewCustomEndpointPlugin(mockPluginService, rdsClientFunc, props)
	assert.NoError(t, err)

	hostInfo, err := host_info_util.NewHostInfoBuilder().SetHost("database-test-name.invalid-XYZ.us-east-2.rds.amazonaws.com").SetPort(1234).Build()
	assert.NoError(t, err)

	expectedConn := &MockConn{}
	mockConnFunc := func(props *utils.RWMap[string, string]) (driver.Conn, error) {
		return expectedConn, nil
	}

	actualConn, connErr := plugin.Connect(hostInfo, props, true, mockConnFunc)
	assert.Nil(t, connErr)
	assert.Equal(t, expectedConn, actualConn)
}

func TestCustomEndpointPluginConnect_InvalidRegion(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockPluginService, mockTelemetryFactory, mockTelemetryCounter, _ := createMocks(ctrl)

	mockPluginService.EXPECT().GetTelemetryFactory().Return(mockTelemetryFactory)
	mockTelemetryFactory.EXPECT().CreateCounter(custom_endpoint.TELEMETRY_WAIT_FOR_INFO_COUNTER).Return(mockTelemetryCounter, nil)

	props := utils.NewRWMap[string, string]()
	props.Put(property_util.CUSTOM_ENDPOINT_REGION_PROPERTY.Name, "invalid-region")
	rdsClientFunc := func(*host_info_util.HostInfo, *utils.RWMap[string, string]) (*rds.Client, error) { return nil, nil }

	plugin, err := custom_endpoint.NewCustomEndpointPlugin(mockPluginService, rdsClientFunc, props)
	assert.NoError(t, err)

	hostInfo, err := host_info_util.NewHostInfoBuilder().SetHost("database-test-name.cluster-custom-XYZ.invalid-region.rds.amazonaws.com").SetPort(1234).Build()
	assert.NoError(t, err)

	mockConnFunc := func(props *utils.RWMap[string, string]) (driver.Conn, error) {
		return &MockConn{}, nil
	}

	_, connErr := plugin.Connect(hostInfo, props, true, mockConnFunc)
	assert.NotNil(t, connErr)
	assert.Equal(t,
		error_util.GetMessage("CustomEndpointPlugin.unableToDetermineRegion", property_util.CUSTOM_ENDPOINT_REGION_PROPERTY.Name),
		connErr.Error())
}

func TestCustomEndpointPluginConnect_DontWaitForCustomEndpointInfo(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockPluginService, mockTelemetryFactory, mockTelemetryCounter, mockMonitor := createMocks(ctrl)

	mockPluginService.EXPECT().GetTelemetryFactory().Return(mockTelemetryFactory)
	mockTelemetryFactory.EXPECT().CreateCounter(custom_endpoint.TELEMETRY_WAIT_FOR_INFO_COUNTER).Return(mockTelemetryCounter, nil)

	mockMonitor.EXPECT().HasCustomEndpointInfo().Return(true).Times(0)
	mockMonitor.EXPECT().Close()

	props := utils.NewRWMap[string, string]()
	props.Put(property_util.WAIT_FOR_CUSTOM_ENDPOINT_INFO.Name, "false")
	rdsClientFunc := func(*host_info_util.HostInfo, *utils.RWMap[string, string]) (*rds.Client, error) { return nil, nil }

	plugin, err := custom_endpoint.NewCustomEndpointPlugin(mockPluginService, rdsClientFunc, props)
	assert.NoError(t, err)
	defer custom_endpoint.CustomEndpointPluginFactory{}.ClearCaches()

	hostInfo, err := host_info_util.NewHostInfoBuilder().SetHost("database-test-name.cluster-custom-XYZ.us-east-2.rds.amazonaws.com").SetPort(1234).Build()
	assert.NoError(t, err)

	custom_endpoint.CUSTOM_ENDPOINT_MONITORS.Put(hostInfo.Host, mockMonitor, time.Minute*1)

	expectedConn := &MockConn{}
	mockConnFunc := func(props *utils.RWMap[string, string]) (driver.Conn, error) {
		return expectedConn, nil
	}

	actualConn, actualConnErr := plugin.Connect(hostInfo, props, true, mockConnFunc)

	assert.Equal(t, expectedConn, actualConn)
	assert.Nil(t, actualConnErr)
}

func TestCustomEndpointPluginConnect_WaitForCustomEndpointInfo(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockPluginService, mockTelemetryFactory, mockTelemetryCounter, mockMonitor := createMocks(ctrl)

	mockPluginService.EXPECT().GetTelemetryFactory().Return(mockTelemetryFactory)
	mockPluginService.EXPECT().GetTelemetryContext().Return(nil)
	mockTelemetryFactory.EXPECT().CreateCounter(custom_endpoint.TELEMETRY_WAIT_FOR_INFO_COUNTER).Return(mockTelemetryCounter, nil)
	mockTelemetryCounter.EXPECT().Inc(gomock.Any())

	mockMonitor.EXPECT().Close()
	mockedHasCustomEndpointInfoCalls0 := mockMonitor.EXPECT().HasCustomEndpointInfo().Return(false).Times(5)
	mockedHasCustomEndpointInfoCalls1 := mockMonitor.EXPECT().HasCustomEndpointInfo().Return(true).Times(1)
	gomock.InOrder(mockedHasCustomEndpointInfoCalls0, mockedHasCustomEndpointInfoCalls1)

	props := utils.NewRWMap[string, string]()
	props.Put(property_util.WAIT_FOR_CUSTOM_ENDPOINT_INFO.Name, "true")
	rdsClientFunc := func(*host_info_util.HostInfo, *utils.RWMap[string, string]) (*rds.Client, error) { return nil, nil }

	plugin, err := custom_endpoint.NewCustomEndpointPlugin(mockPluginService, rdsClientFunc, props)
	assert.NoError(t, err)
	defer custom_endpoint.CustomEndpointPluginFactory{}.ClearCaches()

	hostInfo, err := host_info_util.NewHostInfoBuilder().SetHost("database-test-name.cluster-custom-XYZ.us-east-2.rds.amazonaws.com").SetPort(1234).Build()
	assert.NoError(t, err)

	custom_endpoint.CUSTOM_ENDPOINT_MONITORS.Put(hostInfo.Host, mockMonitor, time.Minute*1)

	expectedConn := &MockConn{}
	mockConnFunc := func(props *utils.RWMap[string, string]) (driver.Conn, error) {
		return expectedConn, nil
	}

	actualConn, actualConnErr := plugin.Connect(hostInfo, props, true, mockConnFunc)

	assert.Equal(t, expectedConn, actualConn)
	assert.Nil(t, actualConnErr)
}

func TestCustomEndpointPluginExecute_CustomEndpointHostNotSet(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockPluginService, mockTelemetryFactory, mockTelemetryCounter, _ := createMocks(ctrl)

	mockPluginService.EXPECT().GetTelemetryFactory().Return(mockTelemetryFactory)
	mockTelemetryFactory.EXPECT().CreateCounter(custom_endpoint.TELEMETRY_WAIT_FOR_INFO_COUNTER).Return(mockTelemetryCounter, nil)

	props := utils.NewRWMap[string, string]()
	props.Put(property_util.CUSTOM_ENDPOINT_REGION_PROPERTY.Name, "invalid-region")
	rdsClientFunc := func(*host_info_util.HostInfo, *utils.RWMap[string, string]) (*rds.Client, error) { return nil, nil }

	plugin, err := custom_endpoint.NewCustomEndpointPlugin(mockPluginService, rdsClientFunc, props)
	assert.NoError(t, err)

	expectedResult0 := "result0"
	expectedResult1 := "result1"
	expectedBool := true
	expectedErr := errors.New("expectedError")
	mockExecuteFunc := func() (any, any, bool, error) {
		return expectedResult0, expectedResult1, expectedBool, expectedErr
	}
	actualResult0, actualResult1, actualBool, actualErr := plugin.Execute(nil, "", mockExecuteFunc)

	assert.Equal(t, expectedResult0, actualResult0)
	assert.Equal(t, expectedResult1, actualResult1)
	assert.Equal(t, expectedBool, actualBool)
	assert.Equal(t, expectedErr, actualErr)
}

func TestCustomEndpointPluginExecute_DontWaitForCustomEndpointInfo(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockPluginService, mockTelemetryFactory, mockTelemetryCounter, mockMonitor := createMocks(ctrl)

	mockPluginService.EXPECT().GetTelemetryFactory().Return(mockTelemetryFactory)
	mockTelemetryFactory.EXPECT().CreateCounter(custom_endpoint.TELEMETRY_WAIT_FOR_INFO_COUNTER).Return(mockTelemetryCounter, nil)

	mockMonitor.EXPECT().Close()
	mockMonitor.EXPECT().HasCustomEndpointInfo().Return(true).Times(0)

	props := utils.NewRWMap[string, string]()
	props.Put(property_util.WAIT_FOR_CUSTOM_ENDPOINT_INFO.Name, "false")
	rdsClientFunc := func(*host_info_util.HostInfo, *utils.RWMap[string, string]) (*rds.Client, error) { return nil, nil }

	plugin, err := custom_endpoint.NewCustomEndpointPlugin(mockPluginService, rdsClientFunc, props)
	assert.NoError(t, err)
	defer custom_endpoint.CustomEndpointPluginFactory{}.ClearCaches()

	hostInfo, err := host_info_util.NewHostInfoBuilder().SetHost("database-test-name.cluster-custom-XYZ.us-east-2.rds.amazonaws.com").SetPort(1234).Build()
	assert.NoError(t, err)

	custom_endpoint.CUSTOM_ENDPOINT_MONITORS.Put(hostInfo.Host, mockMonitor, time.Minute*1)

	expectedResult0 := "result0"
	expectedResult1 := "result1"
	expectedBool := true
	expectedErr := errors.New("expectedError")
	mockExecuteFunc := func() (any, any, bool, error) {
		return expectedResult0, expectedResult1, expectedBool, expectedErr
	}
	actualResult0, actualResult1, actualBool, actualErr := plugin.Execute(nil, "", mockExecuteFunc)

	assert.Equal(t, expectedResult0, actualResult0)
	assert.Equal(t, expectedResult1, actualResult1)
	assert.Equal(t, expectedBool, actualBool)
	assert.Equal(t, expectedErr, actualErr)
}

func TestCustomEndpointPluginExecute_WaitForCustomEndpointInfo(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockPluginService, mockTelemetryFactory, mockTelemetryCounter, mockMonitor := createMocks(ctrl)

	mockPluginService.EXPECT().GetTelemetryFactory().Return(mockTelemetryFactory)
	mockPluginService.EXPECT().GetTelemetryContext().Return(nil)
	mockTelemetryFactory.EXPECT().CreateCounter(custom_endpoint.TELEMETRY_WAIT_FOR_INFO_COUNTER).Return(mockTelemetryCounter, nil)
	mockTelemetryCounter.EXPECT().Inc(gomock.Any())

	mockMonitor.EXPECT().Close()
	mockedHasCustomEndpointInfoCalls0 := mockMonitor.EXPECT().HasCustomEndpointInfo().Return(false).Times(5)
	mockedHasCustomEndpointInfoCalls1 := mockMonitor.EXPECT().HasCustomEndpointInfo().Return(true).Times(1)
	gomock.InOrder(mockedHasCustomEndpointInfoCalls0, mockedHasCustomEndpointInfoCalls1)

	props := utils.NewRWMap[string, string]()
	props.Put(property_util.WAIT_FOR_CUSTOM_ENDPOINT_INFO.Name, "true")
	rdsClientFunc := func(*host_info_util.HostInfo, *utils.RWMap[string, string]) (*rds.Client, error) { return nil, nil }

	hostInfo, err := host_info_util.NewHostInfoBuilder().SetHost("database-test-name.cluster-custom-XYZ.us-east-2.rds.amazonaws.com").SetPort(1234).Build()
	assert.NoError(t, err)

	plugin, err := custom_endpoint.NewCustomEndpointPluginWithHostInfo(mockPluginService, rdsClientFunc, props, hostInfo)
	assert.NoError(t, err)
	defer custom_endpoint.CustomEndpointPluginFactory{}.ClearCaches()

	custom_endpoint.CUSTOM_ENDPOINT_MONITORS.Put(hostInfo.Host, mockMonitor, time.Minute*1)

	expectedResult0 := "result0"
	expectedResult1 := "result1"
	expectedBool := true
	expectedErr := errors.New("expectedError")
	mockExecuteFunc := func() (any, any, bool, error) {
		return expectedResult0, expectedResult1, expectedBool, expectedErr
	}
	actualResult0, actualResult1, actualBool, actualErr := plugin.Execute(nil, "", mockExecuteFunc)

	assert.Equal(t, expectedResult0, actualResult0)
	assert.Equal(t, expectedResult1, actualResult1)
	assert.Equal(t, expectedBool, actualBool)
	assert.Equal(t, expectedErr, actualErr)
}
