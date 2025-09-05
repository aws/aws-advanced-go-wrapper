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
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

func TestDsnHostListProvider_Refresh_Success(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockHostListService := mock_driver_infrastructure.NewMockHostListProviderService(ctrl)
	// Construct DSN string with host
	dsn := "postgresql://127.0.0.1:5432/db"
	props, _ := utils.ParseDsn(dsn)

	provider := driver_infrastructure.NewDsnHostListProvider(props, mockHostListService)

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
	props, _ := utils.ParseDsn(dsn)

	provider := driver_infrastructure.NewDsnHostListProvider(props, mockHostListService)

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

	mockHostListService.EXPECT().GetDialect().Return(driver_infrastructure.DatabaseDialect(&driver_infrastructure.PgDatabaseDialect{})).Times(2)

	props, _ := utils.ParseDsn("postgresql://127.0.0.1:5432/db")

	provider := driver_infrastructure.NewDsnHostListProvider(props, mockHostListService)

	now := time.Now()
	result := provider.CreateHost("", host_info_util.READER, 1.0, 0.5, now)

	assert.Equal(t, "127.0.0.1", result.Host)
	assert.Equal(t, 5432, result.Port)
	assert.Equal(t, host_info_util.READER, result.Role)
	assert.Equal(t, host_info_util.AVAILABLE, result.Availability)
	assert.Equal(t, 101, result.Weight) // 1 lag * 100 + 0.5 = 100.5 → rounds to 101
	assert.Equal(t, now, result.LastUpdateTime)

	result = provider.CreateHost("some-host", host_info_util.WRITER, 2.1, 0.3, now)
	assert.Equal(t, "some-host", result.Host)
	assert.Equal(t, 5432, result.Port)
	assert.Equal(t, host_info_util.WRITER, result.Role)
	assert.Equal(t, host_info_util.AVAILABLE, result.Availability)
	assert.Equal(t, 200, result.Weight) // 2 lag * 100 + 0.3 = 200.3 → rounds to 210
	assert.Equal(t, now, result.LastUpdateTime)
}

func TestDsnHostListProvider_GetHostRole_ReturnsUnknown(t *testing.T) {
	provider := driver_infrastructure.NewDsnHostListProvider(nil, nil)
	role := provider.GetHostRole(nil)
	assert.Equal(t, host_info_util.UNKNOWN, role)
}

func TestDsnHostListProvider_IdentifyConnection_Unsupported(t *testing.T) {
	provider := driver_infrastructure.NewDsnHostListProvider(nil, nil)
	host, err := provider.IdentifyConnection(nil)
	assert.Nil(t, host)
	assert.Error(t, err)
}

func TestDsnHostListProvider_GetClusterId_Unsupported(t *testing.T) {
	provider := driver_infrastructure.NewDsnHostListProvider(nil, nil)
	id, err := provider.GetClusterId()
	assert.Empty(t, id)
	assert.Error(t, err)
}

func TestDsnHostListProvider_IsStaticHostListProvider(t *testing.T) {
	provider := driver_infrastructure.NewDsnHostListProvider(nil, nil)
	assert.True(t, provider.IsStaticHostListProvider())
}
