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
	"sync"
	"testing"
	"time"

	mock_driver_infrastructure "github.com/aws/aws-advanced-go-wrapper/.test/test/mocks/awssql/driver_infrastructure"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/driver_infrastructure"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/host_info_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/plugin_helpers"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/services"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/utils"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPartialPluginService_GetHostsAppliesPermissions is the guard for two defects that sat on one
// line and had no coverage: GetHosts looked the permissions up with the *HostInfo pointer while
// every writer uses the GetUrl() string, and the constructor accepted initialHostInfo and then
// never assigned it. Either alone makes the lookup permanently miss, so the function was identical
// to GetAllHosts. Both compiled cleanly.
//
// Both were latent rather than live: nothing in production calls this method today. The blue/green
// status provider holds the only PartialPluginService and never asks it for a host list.
func TestPartialPluginService_GetHostsAppliesPermissions(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	initialHost, err := host_info_util.NewHostInfoBuilder().
		SetHost("my-endpoint.cluster-custom-xyz.us-east-2.rds.amazonaws.com").
		SetPort(5432).SetHostId("my-endpoint").Build()
	require.NoError(t, err)

	writer := hostWithRole(t, "writer-1", host_info_util.WRITER)
	reader1 := hostWithRole(t, "reader-1", host_info_util.READER)
	reader2 := hostWithRole(t, "reader-2", host_info_util.READER)
	allHosts := []*host_info_util.HostInfo{writer, reader1, reader2}

	// A real storage service: Get/Set live on the RawStorageAccess interface, which
	// MockStorageService does not implement, so a mock would silently swallow the Set below and the
	// lookup would miss for the wrong reason.
	storage := services.NewExpiringStorage(time.Minute, nil)
	t.Cleanup(storage.Stop)
	driver_infrastructure.AllowedAndBlockedHostsStorageType.Register(storage)

	mockContainer := mock_driver_infrastructure.NewMockServicesContainer(ctrl)
	mockContainer.EXPECT().GetStorageService().Return(storage).AnyTimes()

	partial := plugin_helpers.NewPartialPluginService(
		mockContainer, utils.NewRWMap[string, string](), "dsn", nil, nil,
		allHosts, &sync.RWMutex{}, initialHost)

	// With nothing published, every host is returned.
	assert.Equal(t, []string{"writer-1", "reader-1", "reader-2"}, hostIds(partial.GetHosts()))

	// Published exactly as the custom endpoint monitor does for an exclusion-list endpoint: keyed on
	// the endpoint URL, and excluding the writer as well as one reader.
	driver_infrastructure.AllowedAndBlockedHostsStorageType.Set(storage, initialHost.GetUrl(),
		driver_infrastructure.NewAllowedAndBlockedHostsWithRole(
			nil, map[string]bool{"writer-1": true, "reader-2": true}, host_info_util.UNKNOWN))

	assert.Equal(t, []string{"reader-1"}, hostIds(partial.GetHosts()),
		"the permissions published under the custom endpoint URL were not applied")
	assert.Equal(t, []string{"writer-1", "reader-1", "reader-2"}, hostIds(partial.GetAllHosts()),
		"GetAllHosts must stay unfiltered")
}
