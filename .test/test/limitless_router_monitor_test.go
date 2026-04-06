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
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/driver_infrastructure"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/host_info_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/plugins/limitless"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/property_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/services"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/utils/telemetry"
	"github.com/golang/mock/gomock"
)

func TestMonitor_RunAndClose_SuccessfulCycle(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
	mockSelector := mock_driver_infrastructure.NewMockWeightedHostSelector(ctrl)

	host := &host_info_util.HostInfo{Host: "h1", Port: 5432, Weight: 10}

	// Set up a mock connection that returns router data from QueryContext
	routerRows := newMultiRowMockRows(
		[]string{"router_endpoint", "load"},
		[][]driver.Value{
			{"r1", 0.8},
		},
	)
	fakeConn := &MockConn{queryResult: routerRows}

	props := MakeMapFromKeysAndVals(
		property_util.LIMITLESS_ROUTER_CACHE_EXPIRATION_TIME_MS.Name, "100",
	)
	telemetryFactory, _ := telemetry.NewDefaultTelemetryFactory(props)

	storage := services.NewExpiringStorage(5*time.Minute, nil)
	driver_infrastructure.RegisterDefaultStorageTypes(storage)
	limitless.LimitlessRoutersStorageType.Register(storage)

	mockMonitorService := mock_driver_infrastructure.NewMockMonitorService(ctrl)
	mockMonitorService.EXPECT().RegisterMonitorType(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()

	container := &services.FullServicesContainer{
		Storage:       storage,
		Monitor:       mockMonitorService,
		Telemetry:     telemetryFactory,
		PluginService: mockPluginService,
	}

	mockPluginService.EXPECT().ForceConnect(gomock.Any(), gomock.Any()).Return(fakeConn, nil).AnyTimes()
	mockPluginService.EXPECT().GetDialect().Return(driver_infrastructure.DatabaseDialect(&driver_infrastructure.AuroraPgDatabaseDialect{})).AnyTimes()
	mockPluginService.EXPECT().GetHostSelectorStrategy(driver_infrastructure.SELECTOR_WEIGHTED_RANDOM).
		Return(driver_infrastructure.HostSelector(mockSelector), nil).AnyTimes()
	mockSelector.EXPECT().SetHostWeights(gomock.Any()).AnyTimes()

	monitor := limitless.NewLimitlessRouterMonitorImpl(
		container,
		host,
		"key",
		props,
		10,
	)

	monitor.Start()
	time.Sleep(50 * time.Millisecond)
	monitor.Stop()

	// Verify routers were stored in StorageService
	routers, found := limitless.LimitlessRoutersStorageType.Get(storage, "key")
	if !found || routers == nil {
		t.Error("expected router cache to contain key after successful fetch")
	}
}

func TestOpenConnection_ErrorClosesConn(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)

	host := &host_info_util.HostInfo{Host: "hX", Port: 1234, Weight: 1}

	telemetryFactory, _ := telemetry.NewDefaultTelemetryFactory(emptyProps)

	storage := services.NewExpiringStorage(5*time.Minute, nil)
	driver_infrastructure.RegisterDefaultStorageTypes(storage)

	mockMonitorService := mock_driver_infrastructure.NewMockMonitorService(ctrl)
	mockMonitorService.EXPECT().RegisterMonitorType(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()

	container := &services.FullServicesContainer{
		Storage:       storage,
		Monitor:       mockMonitorService,
		Telemetry:     telemetryFactory,
		PluginService: mockPluginService,
	}

	fakeConn := &MockConn{}
	mockPluginService.EXPECT().ForceConnect(gomock.Any(), gomock.Any()).Return(fakeConn, errors.New("fail")).AnyTimes()

	monitor := limitless.NewLimitlessRouterMonitorImpl(
		container,
		host,
		"any",
		emptyProps,
		10,
	)

	monitor.Start()
	time.Sleep(50 * time.Millisecond)
	monitor.Stop()
}
