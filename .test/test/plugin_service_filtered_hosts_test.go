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
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/driver_infrastructure"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/host_info_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/services"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPluginService_GetUpdatedHostListWithTimeoutAppliesPermissions guards the routing half of the
// custom endpoint feature at the seam where it was missing.
//
// Reader failover seeds its candidates from GetHosts, which applies the endpoint's allowed/blocked
// host permissions, and then re-seeds from GetUpdatedHostListWithTimeout when a round finds nothing
// to try. That second path returned the host list provider's raw topology, so instances excluded
// from the custom endpoint came back as candidates - and with them the writer, which a role
// requirement is supposed to remove, turning it into a reader failover target.
//
// ForceRefreshHostListWithTimeout must keep storing the unfiltered list: AllHosts is the cluster
// topology, and filtering it there would hide excluded instances from topology tracking too.
func TestPluginService_GetUpdatedHostListWithTimeoutAppliesPermissions(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	target, _, _, container, err := beforePluginServiceTests()
	require.NoError(t, err)

	initialHost, err := host_info_util.NewHostInfoBuilder().
		SetHost("my-endpoint.cluster-custom-xyz.us-east-2.rds.amazonaws.com").
		SetPort(5432).SetHostId("my-endpoint").Build()
	require.NoError(t, err)
	target.SetInitialConnectionHostInfo(initialHost)

	writer := hostWithRole(t, "writer-1", host_info_util.WRITER)
	reader1 := hostWithRole(t, "reader-1", host_info_util.READER)
	reader2 := hostWithRole(t, "reader-2", host_info_util.READER)
	topology := []*host_info_util.HostInfo{writer, reader1, reader2}

	// A real storage service: Get/Set live on the RawStorageAccess interface, which
	// MockStorageService does not implement, so a mock would swallow the Set below.
	storage := services.NewExpiringStorage(time.Minute, nil)
	t.Cleanup(storage.Stop)
	driver_infrastructure.AllowedAndBlockedHostsStorageType.Register(storage)
	container.Storage = storage

	hostListProvider := mock_driver_infrastructure.NewMockHostListProvider(ctrl)
	hostListProvider.EXPECT().ForceRefreshHostListWithTimeout(gomock.Any(), gomock.Any()).
		Return(topology, nil).AnyTimes()
	target.SetHostListProvider(hostListProvider)

	// With nothing published, the refreshed list passes through untouched.
	refreshed, err := target.GetUpdatedHostListWithTimeout(false, 1000)
	require.NoError(t, err)
	assert.Equal(t, []string{"writer-1", "reader-1", "reader-2"}, hostIds(refreshed))

	// Published exactly as the custom endpoint monitor does for an exclusion-list endpoint: keyed on
	// the endpoint URL, and excluding the writer as well as one reader.
	driver_infrastructure.AllowedAndBlockedHostsStorageType.Set(storage, initialHost.GetUrl(),
		driver_infrastructure.NewAllowedAndBlockedHostsWithRole(
			nil, map[string]bool{"writer-1": true, "reader-2": true}, host_info_util.UNKNOWN))

	refreshed, err = target.GetUpdatedHostListWithTimeout(false, 1000)
	require.NoError(t, err)
	assert.Equal(t, []string{"reader-1"}, hostIds(refreshed),
		"a refreshed topology handed to host selection was not filtered by the custom endpoint permissions")

	ok, err := target.ForceRefreshHostListWithTimeout(false, 1000)
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, []string{"writer-1", "reader-1", "reader-2"}, hostIds(target.GetAllHosts()),
		"the stored topology must stay unfiltered")
	assert.Equal(t, []string{"reader-1"}, hostIds(target.GetHosts()),
		"GetHosts must filter the stored topology")
}
