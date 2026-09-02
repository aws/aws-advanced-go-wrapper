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
	"sync/atomic"
	"testing"
	"time"

	mock_driver_infrastructure "github.com/aws/aws-advanced-go-wrapper/.test/test/mocks/awssql/driver_infrastructure"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/driver_infrastructure"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/host_info_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/plugins"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/property_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/services"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/utils/telemetry"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestFailoverReader_RetryRoundsArePacedAndReDerived is the regression guard for reader failover
// burning a core for the whole failover timeout.
//
// More than one round shape reaches the end of getReaderFailoverConnection's body having performed no
// I/O: an empty candidate list skips the inner reader loop, and the writer fallback is skipped both
// when no writer is known and - in STRICT_READER mode - once the writer has been confirmed as still
// being the writer. Any of those without a delay spins at 100% of one core until failoverTimeoutMs
// (300s by default), and without re-deriving candidates each round, a reader appearing in the topology
// mid-failover is never seen, so the spin cannot end in success.
//
// The configuration here is the second shape, which is the one an earlier revision's special-case
// guard missed: STRICT_READER, a host list containing only a writer, and a writer that verifies as
// still being the writer. It is reachable on any cluster whose topology shows a writer and no readers.
//
// Both bounds are load-bearing and they fail in opposite directions:
//   - the lower bound fails if candidates are not re-derived per round (the pre-fix code derived them
//     once, so GetHosts is called exactly once no matter how many times the loop spins);
//   - the upper bound fails if the rounds are not paced (unpaced re-derivation calls GetHosts
//     thousands of times in the same window).
func TestFailoverReader_RetryRoundsArePacedAndReDerived(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// A topology with a writer and no readers. GetWriter finds the writer, so originalWriter is not
	// nil, so the empty-candidates-and-no-writer guard does not fire here.
	writerOnly := []*host_info_util.HostInfo{hostWithRole(t, "writer-1", host_info_util.WRITER)}

	const failoverTimeout = 400 * time.Millisecond
	props := MakeMapFromKeysAndVals(
		property_util.FAILOVER_TIMEOUT_MS.Name, "400",
		property_util.DRIVER_PROTOCOL.Name, "mysql",
		property_util.ENABLE_CONNECT_FAILOVER.Name, "false",
	)

	telemetryFactory, err := telemetry.NewDefaultTelemetryFactory(props)
	require.NoError(t, err)

	var getHostsCalls atomic.Int32
	mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
	mockPluginService.EXPECT().GetHosts().DoAndReturn(func() []*host_info_util.HostInfo {
		getHostsCalls.Add(1)
		return writerOnly
	}).AnyTimes()
	mockPluginService.EXPECT().GetUpdatedHostListWithTimeout(gomock.Any(), gomock.Any()).
		Return(writerOnly, nil).AnyTimes()
	mockPluginService.EXPECT().ForceRefreshHostListWithTimeout(gomock.Any(), gomock.Any()).
		Return(true, nil).AnyTimes()
	mockPluginService.EXPECT().Connect(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&MockConn{}, nil).AnyTimes()
	// The candidate verifies as still being the writer, which in STRICT_READER mode makes it an invalid
	// reader connection and latches the branch that skips the writer fallback on every later round.
	mockPluginService.EXPECT().GetHostRole(gomock.Any()).Return(host_info_util.WRITER).AnyTimes()
	mockPluginService.EXPECT().GetTelemetryContext().Return(nil).AnyTimes()
	mockPluginService.EXPECT().SetTelemetryContext(gomock.Any()).AnyTimes()
	mockPluginService.EXPECT().GetTelemetryFactory().Return(telemetryFactory).AnyTimes()

	container := &services.FullServicesContainer{
		Telemetry:     telemetryFactory,
		PluginService: mockPluginService,
	}

	plugin, err := plugins.NewFailoverPlugin(container, props, driver_infrastructure.FAILOVER_PLUGIN_CODE,
		func(p *plugins.FailoverPlugin) plugins.FailoverHandler { return plugins.NewRdsFailoverHandler(p) })
	require.NoError(t, err)
	plugin.FailoverMode = plugins.MODE_STRICT_READER

	start := time.Now()
	failoverErr := plugin.FailoverReader()
	elapsed := time.Since(start)

	require.Error(t, failoverErr, "reader failover cannot succeed when no reader exists")
	assert.GreaterOrEqual(t, elapsed, failoverTimeout,
		"the loop exited before the failover timeout, so it is not the retry path being measured")

	calls := getHostsCalls.Load()
	assert.GreaterOrEqual(t, calls, int32(2),
		"candidates were derived %d time(s) across a %v retry window, so a reader appearing in the "+
			"topology mid-failover would never be picked up", calls, failoverTimeout)
	assert.LessOrEqual(t, calls, int32(12),
		"%d retry rounds in %v means the rounds are not paced - the loop is spinning", calls, failoverTimeout)
}
