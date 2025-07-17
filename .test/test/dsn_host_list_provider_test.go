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
	"github.com/aws/aws-advanced-go-wrapper/awssql/driver_infrastructure"
	"github.com/aws/aws-advanced-go-wrapper/awssql/host_info_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/property_util"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

func TestDsnHostListProvider_Refresh_Success(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockHostListService := mock_driver_infrastructure.NewMockHostListProviderService(ctrl)
	// Construct DSN string with host
	dsn := "postgresql://127.0.0.1:5432/db"

	props := map[string]string{
		property_util.HOST.Name: "127.0.0.1",
	}

	provider := driver_infrastructure.NewDsnHostListProvider(props, dsn, mockHostListService)

	// `init()` should call SetInitialConnectionHostInfo with parsed host
	mockHostListService.EXPECT().SetInitialConnectionHostInfo(gomock.Any()).Times(1)

	hosts, err := provider.Refresh(nil)
	assert.NoError(t, err)
	assert.Len(t, hosts, 1)
	assert.Equal(t, "127.0.0.1", hosts[0].Host)
	assert.Equal(t, 5432, hosts[0].Port)
}

func TestDsnHostListProvider_ForceRefresh_UsesInit(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockHostListService := mock_driver_infrastructure.NewMockHostListProviderService(ctrl)

	dsn := "postgresql://127.0.0.1:5432/db"
	props := map[string]string{
		property_util.HOST.Name: "127.0.0.1",
	}

	provider := driver_infrastructure.NewDsnHostListProvider(props, dsn, mockHostListService)

	mockHostListService.EXPECT().SetInitialConnectionHostInfo(gomock.Any()).Times(1)

	hosts, err := provider.ForceRefresh(nil)
	assert.NoError(t, err)
	assert.Len(t, hosts, 1)
	assert.Equal(t, "127.0.0.1", hosts[0].Host)
}

func TestDsnHostListProvider_CreateHost_BuildsCorrectly(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockHostListService := mock_driver_infrastructure.NewMockHostListProviderService(ctrl)
	mockDialect := mock_driver_infrastructure.NewMockDatabaseDialect(ctrl)

	mockHostListService.EXPECT().GetDialect().Return(mockDialect).Times(1)
	mockDialect.EXPECT().GetDefaultPort().Return(3306).Times(1)

	props := map[string]string{
		property_util.HOST.Name: "127.0.0.1",
	}

	provider := driver_infrastructure.NewDsnHostListProvider(props, "postgresql://127.0.0.1:5432/db", mockHostListService)

	now := time.Now()
	result := provider.CreateHost("some-host", host_info_util.READER, 1.0, 0.5, now)

	assert.Equal(t, "127.0.0.1", result.Host)
	assert.Equal(t, 3306, result.Port)
	assert.Equal(t, host_info_util.READER, result.Role)
	assert.Equal(t, host_info_util.AVAILABLE, result.Availability)
	assert.Equal(t, 101, result.Weight) // 1.0 lag * 100 + 0.5 = 100.5 → rounds to 100
	assert.Equal(t, now, result.LastUpdateTime)
}

func TestDsnHostListProvider_GetHostRole_ReturnsUnknown(t *testing.T) {
	provider := driver_infrastructure.NewDsnHostListProvider(nil, "dsn", nil)
	role := provider.GetHostRole(nil)
	assert.Equal(t, host_info_util.UNKNOWN, role)
}

func TestDsnHostListProvider_IdentifyConnection_Unsupported(t *testing.T) {
	provider := driver_infrastructure.NewDsnHostListProvider(nil, "dsn", nil)
	host, err := provider.IdentifyConnection(nil)
	assert.Nil(t, host)
	assert.Error(t, err)
}

func TestDsnHostListProvider_GetClusterId_Unsupported(t *testing.T) {
	provider := driver_infrastructure.NewDsnHostListProvider(nil, "dsn", nil)
	id, err := provider.GetClusterId()
	assert.Empty(t, id)
	assert.Error(t, err)
}

func TestDsnHostListProvider_IsStaticHostListProvider(t *testing.T) {
	provider := driver_infrastructure.NewDsnHostListProvider(nil, "dsn", nil)
	assert.True(t, provider.IsStaticHostListProvider())
}
