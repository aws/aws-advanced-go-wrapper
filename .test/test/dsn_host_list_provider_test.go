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
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/driver_infrastructure"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/host_info_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/property_util"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

func TestDsnHostListProvider_Refresh_Success(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockHostListService := mock_driver_infrastructure.NewMockHostListProviderService(ctrl)
	dsn := "postgresql://127.0.0.1:5432/db"
	props, _ := property_util.ParseDsn(dsn)

	provider := driver_infrastructure.NewDsnHostListProvider(props, mockHostListService)

	mockHostListService.EXPECT().SetInitialConnectionHostInfo(gomock.Any()).Times(1)

	hosts, err := provider.Refresh()
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
	props, _ := property_util.ParseDsn(dsn)

	provider := driver_infrastructure.NewDsnHostListProvider(props, mockHostListService)

	mockHostListService.EXPECT().SetInitialConnectionHostInfo(gomock.Any()).Times(1)

	hosts, err := provider.ForceRefresh()
	assert.NoError(t, err)
	assert.Len(t, hosts, 1)
	assert.Equal(t, "127.0.0.1", hosts[0].Host)
}

// CreateHost was removed from DsnHostListProvider as it was dead code.

func TestDsnHostListProvider_GetHostRole_ReturnsUnknown(t *testing.T) {
	provider := driver_infrastructure.NewDsnHostListProvider(nil, nil)
	assert.Equal(t, host_info_util.UNKNOWN, provider.GetHostRole(nil))
}

func TestDsnHostListProvider_IdentifyConnection_ReturnsNil(t *testing.T) {
	provider := driver_infrastructure.NewDsnHostListProvider(nil, nil)
	host, err := provider.IdentifyConnection(nil)
	assert.Nil(t, host)
	assert.NoError(t, err)
}

func TestDsnHostListProvider_GetClusterId_ReturnsNone(t *testing.T) {
	provider := driver_infrastructure.NewDsnHostListProvider(nil, nil)
	id, err := provider.GetClusterId()
	assert.Equal(t, "<none>", id)
	assert.NoError(t, err)
}

func TestDsnHostListProvider_IsStaticHostListProvider(t *testing.T) {
	provider := driver_infrastructure.NewDsnHostListProvider(nil, nil)
	assert.True(t, provider.IsStaticHostListProvider())
}
