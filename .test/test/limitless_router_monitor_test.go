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
	"testing"
	"time"

	mock_driver_infrastructure "github.com/aws/aws-advanced-go-wrapper/.test/test/mocks/awssql/driver_infrastructure"
	mock_limitless "github.com/aws/aws-advanced-go-wrapper/.test/test/mocks/awssql/limitless"
	mock_database_sql_driver "github.com/aws/aws-advanced-go-wrapper/.test/test/mocks/database_sql_driver"

	"github.com/aws/aws-advanced-go-wrapper/awssql/driver_infrastructure"
	"github.com/aws/aws-advanced-go-wrapper/awssql/host_info_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/plugins/limitless"
	"github.com/aws/aws-advanced-go-wrapper/awssql/property_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils"
	"github.com/golang/mock/gomock"
)

func TestMonitor_RunAndClose_SuccessfulCycle(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockPlugin := mock_driver_infrastructure.NewMockPluginService(ctrl)
	mockSelector := mock_driver_infrastructure.NewMockWeightedHostSelector(ctrl)
	mockQuery := mock_limitless.NewMockLimitlessQueryHelper(ctrl)
	fakeConn := mock_database_sql_driver.NewMockConn(ctrl)

	host := &host_info_util.HostInfo{Host: "h1", Port: 5432, Weight: 10}
	testRouters := []*host_info_util.HostInfo{
		{Host: "r1", Port: 5432, Weight: 2},
	}

	cache := utils.NewSlidingExpirationCache[[]*host_info_util.HostInfo]("test")

	mockPlugin.
		EXPECT().
		ForceConnect(gomock.Any(), gomock.Any()).
		Return(fakeConn, nil).
		AnyTimes()

	// Ensure conn.Close() is expected
	fakeConn.
		EXPECT().
		Close().
		Return(nil).
		AnyTimes()

	mockQuery.
		EXPECT().
		QueryForLimitlessRouters(fakeConn, host.Port, gomock.Any()).
		Return(testRouters, nil)

	mockPlugin.
		EXPECT().
		GetHostSelectorStrategy(driver_infrastructure.SELECTOR_WEIGHTED_RANDOM).
		Return(driver_infrastructure.HostSelector(mockSelector), nil)

	mockSelector.
		EXPECT().
		SetHostWeights(map[string]int{"r1": 2})

	mockQuery.
		EXPECT().
		QueryForLimitlessRouters(fakeConn, host.Port, gomock.Any()).
		Return([]*host_info_util.HostInfo{}, nil)

	props := map[string]string{
		property_util.LIMITLESS_ROUTER_CACHE_EXPIRATION_TIME_MS.Name: "100",
	}

	monitor := limitless.NewLimitlessRouterMonitorImpl(
		mockQuery,
		mockPlugin,
		host,
		cache,
		"key",
		10, // ms
		props,
	)

	time.Sleep(50 * time.Millisecond)

	monitor.Close()
	time.Sleep(20 * time.Millisecond)

	if _, found := cache.Get("key", 100*time.Millisecond); !found {
		t.Error("expected router cache to contain key after successful fetch")
	}
}

func TestOpenConnection_ErrorClosesConn(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockPlugin := mock_driver_infrastructure.NewMockPluginService(ctrl)
	mockQuery := mock_limitless.NewMockLimitlessQueryHelper(ctrl)
	fakeConn := mock_database_sql_driver.NewMockConn(ctrl)

	host := &host_info_util.HostInfo{Host: "hX", Port: 1234, Weight: 1}
	cache := utils.NewSlidingExpirationCache[[]*host_info_util.HostInfo]("test")

	mockPlugin.
		EXPECT().
		ForceConnect(gomock.Any(), gomock.Any()).
		Return(fakeConn, errors.New("fail")).
		AnyTimes()

	// Make sure conn.Close() is expected
	fakeConn.
		EXPECT().
		Close().
		Return(nil).
		AnyTimes()

	monitor := limitless.NewLimitlessRouterMonitorImpl(
		mockQuery,
		mockPlugin,
		host,
		cache,
		"any",
		10,
		map[string]string{},
	)

	time.Sleep(20 * time.Millisecond)

	monitor.Close()
}
