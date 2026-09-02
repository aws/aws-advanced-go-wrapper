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
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	mock_driver_infrastructure "github.com/aws/aws-advanced-go-wrapper/.test/test/mocks/awssql/driver_infrastructure"
	mock_telemetry "github.com/aws/aws-advanced-go-wrapper/.test/test/mocks/awssql/util/telemetry"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/driver_infrastructure"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/host_info_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/region_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/services"
	custom_endpoint "github.com/aws/aws-advanced-go-wrapper/custom-endpoint"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/rds"
	"github.com/aws/aws-sdk-go-v2/service/rds/types"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func createCustomEndpointMonitorMocks(t *testing.T, ctrl *gomock.Controller) (
	*mock_driver_infrastructure.MockServicesContainer,
	*mock_driver_infrastructure.MockPluginService,
	*mock_telemetry.MockTelemetryCounter,
	driver_infrastructure.StorageService,
) {
	mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
	mockContainer := mock_driver_infrastructure.NewMockServicesContainer(ctrl)
	mockContainer.EXPECT().GetPluginService().Return(mockPluginService).AnyTimes()
	// Reached on the successful-fetch path, where the info-changed counter is incremented.
	mockPluginService.EXPECT().GetTelemetryContext().Return(nil).AnyTimes()
	mockTelemetryCounter := mock_telemetry.NewMockTelemetryCounter(ctrl)
	// A real storage service, not MockStorageService. Get/Set live on the separate
	// RawStorageAccess interface, which the generated mock does not implement, so the descriptor's
	// type assertion fails and every Set silently becomes a no-op — a test using the mock would
	// assert nothing about publication.
	storage := services.NewExpiringStorage(time.Minute, nil)
	t.Cleanup(storage.Stop)
	driver_infrastructure.AllowedAndBlockedHostsStorageType.Register(storage)
	mockContainer.EXPECT().GetStorageService().Return(storage).AnyTimes()
	return mockContainer, mockPluginService, mockTelemetryCounter, storage
}

// newTestMonitor builds a monitor so each test states only the values it cares about.
func newTestMonitor(
	t *testing.T,
	container driver_infrastructure.ServicesContainer,
	host string,
	refreshRate time.Duration,
	counter *mock_telemetry.MockTelemetryCounter,
	api rds.DescribeDBClusterEndpointsAPIClient,
) *custom_endpoint.CustomEndpointMonitorImpl {
	t.Helper()
	hostInfo, err := host_info_util.NewHostInfoBuilder().
		SetHost(host + ".cluster-custom-xyz.us-east-2.rds.amazonaws.com").SetPort(5432).Build()
	require.NoError(t, err)
	return custom_endpoint.NewCustomEndpointMonitorImpl(
		container, hostInfo, host, region_util.Region("us-east-2"),
		refreshRate, counter, api)
}

// stubRdsApi counts calls and returns a fixed error, so a test can measure how hard the monitor
// loop hits the RDS API.
type stubRdsApi struct {
	calls atomic.Int32
	err   error
	out   *rds.DescribeDBClusterEndpointsOutput
}

func (s *stubRdsApi) DescribeDBClusterEndpoints(
	_ context.Context,
	_ *rds.DescribeDBClusterEndpointsInput,
	_ ...func(*rds.Options),
) (*rds.DescribeDBClusterEndpointsOutput, error) {
	s.calls.Add(1)
	return s.out, s.err
}

func TestCustomEndpointMonitorImpl_NewMonitor(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockContainer, _, mockCounter, _ := createCustomEndpointMonitorMocks(t, ctrl)
	monitor := newTestMonitor(t, mockContainer, "my-endpoint", 5*time.Second, mockCounter, nil)

	assert.NotNil(t, monitor)
	assert.True(t, monitor.CanDispose())
}

func TestCustomEndpointMonitorImpl_GetState_BeforeStart(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockContainer, _, mockCounter, _ := createCustomEndpointMonitorMocks(t, ctrl)
	monitor := newTestMonitor(t, mockContainer, "my-endpoint", 5*time.Second, mockCounter, nil)

	// Before Start(), state should be Stopped (default)
	assert.Equal(t, driver_infrastructure.MonitorStateStopped, monitor.GetState())
}

func TestCustomEndpointMonitorImpl_HasCustomEndpointInfo_NoCache(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockContainer, _, mockCounter, _ := createCustomEndpointMonitorMocks(t, ctrl)
	monitor := newTestMonitor(t, mockContainer, "test-no-cache", 5*time.Second, mockCounter, nil)

	// No cache entry exists, should return false
	assert.False(t, monitor.HasCustomEndpointInfo())
}

func TestCustomEndpointMonitorImpl_RequestCustomEndpointInfoUpdate(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockContainer, _, mockCounter, _ := createCustomEndpointMonitorMocks(t, ctrl)
	monitor := newTestMonitor(t, mockContainer, "test-update", 5*time.Second, mockCounter, nil)

	// Should not panic when called
	monitor.RequestCustomEndpointInfoUpdate()
}

func TestCustomEndpointMonitorImpl_Close(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockContainer, _, mockCounter, _ := createCustomEndpointMonitorMocks(t, ctrl)
	monitor := newTestMonitor(t, mockContainer, "test-close", 5*time.Second, mockCounter, nil)

	// Close should not panic
	monitor.Close()
}

func TestCustomEndpointMonitorImpl_GetLastActivityTimestampNanos(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockContainer, _, mockCounter, _ := createCustomEndpointMonitorMocks(t, ctrl)
	monitor := newTestMonitor(t, mockContainer, "test-timestamp", 5*time.Second, mockCounter, nil)

	// Before Start(), timestamp should be 0
	assert.Equal(t, int64(0), monitor.GetLastActivityTimestampNanos())
}

func TestClearCache(t *testing.T) {
	// Should not panic
	custom_endpoint.ClearCache()
}

// --- Phase 2: the monitor loop must not spin on the RDS API -------------------------------------

// TestCustomEndpointMonitor_DoesNotSpinOnError is the regression guard for the critical defect.
// The interruptible sleep returns immediately while refreshRequired is set, and that flag used to
// be cleared only on the info-changed branch — which no error path reaches. A failing RDS call
// therefore re-issued DescribeDBClusterEndpoints as fast as the API could answer, forever.
//
// A connection asks for a refresh first, exactly as waitForCustomEndpointInfo does, so the flag is
// set when the first failure lands.
func TestCustomEndpointMonitor_DoesNotSpinOnError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockContainer, _, mockCounter, _ := createCustomEndpointMonitorMocks(t, ctrl)
	api := &stubRdsApi{err: errors.New("AccessDenied: not authorized")}

	// 50ms refresh rate keeps the test quick while still being long enough that a correct
	// implementation cannot make many calls in the window.
	monitor := newTestMonitor(t, mockContainer, "spin-guard", 50*time.Millisecond, mockCounter, api)

	monitor.RequestCustomEndpointInfoUpdate()
	monitor.Start()
	time.Sleep(400 * time.Millisecond)
	monitor.Stop()

	calls := api.calls.Load()
	assert.Greater(t, calls, int32(0), "the monitor should have attempted at least one fetch")
	// 400ms at a 50ms floor is at most ~8 attempts, and backoff should make it fewer. Before the
	// fix this loop was bounded only by stub latency and reached thousands.
	assert.LessOrEqual(t, calls, int32(12),
		"the monitor is spinning on the RDS API: %d calls in 400ms", calls)
}

func TestCustomEndpointMonitor_PublishesHostPermissions(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockContainer, _, mockCounter, storage := createCustomEndpointMonitorMocks(t, ctrl)
	mockCounter.EXPECT().Inc(gomock.Any()).AnyTimes()
	endpointId := "zero-factor"
	api := &stubRdsApi{out: &rds.DescribeDBClusterEndpointsOutput{
		DBClusterEndpoints: []types.DBClusterEndpoint{{
			DBClusterEndpointIdentifier: &endpointId,
			DBClusterIdentifier:         &endpointId,
			Endpoint:                    &endpointId,
			CustomEndpointType:          aws.String("READER"),
			StaticMembers:               []string{"instance-1"},
		}},
	}}

	monitor := newTestMonitor(t, mockContainer, endpointId, 20*time.Millisecond, mockCounter, api)

	hostInfo, err := host_info_util.NewHostInfoBuilder().
		SetHost(endpointId + ".cluster-custom-xyz.us-east-2.rds.amazonaws.com").SetPort(5432).Build()
	require.NoError(t, err)

	assert.NotPanics(t, func() {
		monitor.Start()
		time.Sleep(120 * time.Millisecond)
	})
	assert.Greater(t, api.calls.Load(), int32(0))

	// The point of the whole feature: a successful fetch publishes the host permissions that
	// GetHosts reads to exclude instances outside the custom endpoint. Asserted while the monitor is
	// still running, so that the entry's presence is attributable to publication rather than to it
	// having survived a stop - which the assertion after Stop below covers separately.
	permissions, found := driver_infrastructure.AllowedAndBlockedHostsStorageType.Get(storage, hostInfo.GetUrl())
	require.True(t, found, "the monitor did not publish an allowed/blocked host list")
	assert.Equal(t, map[string]bool{"instance-1": true}, permissions.GetAllowedHostIds())

	assert.NotPanics(t, monitor.Stop)

	// A forward-looking guard rather than a fixed defect: no code path has ever deleted the
	// permissions entry, and none may start. GetHosts fails open on a missing entry and returns every
	// host in the cluster, and Close is reached on every monitor recreate, not only at shutdown, so a
	// delete here would open that window routinely. Letting the entry lapse on its own TTL serves
	// stale permissions across a handover instead, which can only misroute within the last known
	// member set.
	permissions, found = driver_infrastructure.AllowedAndBlockedHostsStorageType.Get(storage, hostInfo.GetUrl())
	require.True(t, found, "the host permissions were deleted on stop, so GetHosts now fails open")
	assert.Equal(t, map[string]bool{"instance-1": true}, permissions.GetAllowedHostIds())
}

// TestCustomEndpointMonitor_NonPositiveRefreshRateIsFloored covers the second, independent spin
// trigger: GetRefreshRateValue logs a non-positive value but still returns it, and sleep(0) returns
// immediately, so customEndpointInfoRefreshRateMs=0 span the loop with no error required.
func TestCustomEndpointMonitor_NonPositiveRefreshRateIsFloored(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockContainer, _, mockCounter, _ := createCustomEndpointMonitorMocks(t, ctrl)
	api := &stubRdsApi{err: errors.New("boom")}

	monitor := newTestMonitor(t, mockContainer, "zero-rate", 0, mockCounter, api)

	monitor.RequestCustomEndpointInfoUpdate()
	monitor.Start()
	time.Sleep(200 * time.Millisecond)
	monitor.Stop()

	// Floored to the 30s default, so only the very first attempt fits in the window.
	assert.Greater(t, api.calls.Load(), int32(0), "the monitor should have attempted at least one fetch")
	assert.LessOrEqual(t, api.calls.Load(), int32(2),
		"a non-positive refresh rate was not floored: %d calls in 200ms", api.calls.Load())
}

// TestCustomEndpointMonitor_NilCustomEndpointTypeDoesNotPanic guards the unchecked pointer
// dereference in NewCustomEndpointInfo. Its three sibling fields were checked; this one was not,
// so a response missing it panicked on the monitor goroutine and aborted the host process.
func TestCustomEndpointMonitor_NilCustomEndpointTypeDoesNotPanic(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockContainer, _, mockCounter, _ := createCustomEndpointMonitorMocks(t, ctrl)
	endpointId := "nil-type"
	api := &stubRdsApi{out: &rds.DescribeDBClusterEndpointsOutput{
		DBClusterEndpoints: []types.DBClusterEndpoint{{
			DBClusterEndpointIdentifier: &endpointId,
			DBClusterIdentifier:         &endpointId,
			Endpoint:                    &endpointId,
			CustomEndpointType:          nil,
			StaticMembers:               []string{"instance-1"},
		}},
	}}

	monitor := newTestMonitor(t, mockContainer, endpointId, 20*time.Millisecond, mockCounter, api)

	assert.NotPanics(t, func() {
		monitor.Start()
		time.Sleep(120 * time.Millisecond)
		monitor.Stop()
	})
}

// TestCustomEndpointMonitor_StopIsPromptAndIdempotent covers the shutdown path. Stop() does an
// unbounded wg.Wait(), and it is called from the shared monitor-cleanup goroutine, so a monitor
// that does not exit promptly stalls expiry for every monitor type in the process.
func TestCustomEndpointMonitor_StopIsPromptAndIdempotent(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockContainer, _, mockCounter, _ := createCustomEndpointMonitorMocks(t, ctrl)
	api := &stubRdsApi{err: errors.New("boom")}

	// A 10s interval means the monitor is deep inside a sleep when Stop lands.
	monitor := newTestMonitor(t, mockContainer, "stop-guard", 10*time.Second, mockCounter, api)

	monitor.Start()
	time.Sleep(50 * time.Millisecond)

	done := make(chan struct{})
	go func() {
		monitor.Stop()
		monitor.Stop() // second call must not panic or block
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Stop() did not return promptly while the monitor was sleeping")
	}
	assert.Equal(t, driver_infrastructure.MonitorStateStopped, monitor.GetState())
}

// countingStorage counts Set calls so a test can tell "published once" from "republished every
// poll". It embeds the real storage because Get/Set live on the separate RawStorageAccess
// interface that StorageTypeDescriptor type-asserts; MockStorageService does not implement it, so
// a mock would silently swallow every Set.
type countingStorage struct {
	*services.ExpiringStorage
	sets atomic.Int32
}

func (c *countingStorage) Set(typeKey string, key any, value any) {
	c.sets.Add(1)
	c.ExpiringStorage.Set(typeKey, key, value)
}

// TestCustomEndpointMonitor_RepublishesUnchangedPermissions guards the fix for the recurring window
// where filtering lapsed. The permissions entry and the info cache entry share a 5 minute
// non-renewing TTL, so writing the permissions only when the endpoint info changed let both expire
// together, after which GetHosts found nothing and returned every host unfiltered.
func TestCustomEndpointMonitor_RepublishesUnchangedPermissions(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
	mockPluginService.EXPECT().GetTelemetryContext().Return(nil).AnyTimes()
	mockContainer := mock_driver_infrastructure.NewMockServicesContainer(ctrl)
	mockContainer.EXPECT().GetPluginService().Return(mockPluginService).AnyTimes()
	mockCounter := mock_telemetry.NewMockTelemetryCounter(ctrl)
	mockCounter.EXPECT().Inc(gomock.Any()).AnyTimes()

	storage := &countingStorage{ExpiringStorage: services.NewExpiringStorage(time.Minute, nil)}
	t.Cleanup(storage.Stop)
	driver_infrastructure.AllowedAndBlockedHostsStorageType.Register(storage)
	mockContainer.EXPECT().GetStorageService().Return(storage).AnyTimes()

	endpointId := "republish"
	api := &stubRdsApi{out: &rds.DescribeDBClusterEndpointsOutput{
		DBClusterEndpoints: []types.DBClusterEndpoint{{
			DBClusterEndpointIdentifier: &endpointId,
			DBClusterIdentifier:         &endpointId,
			Endpoint:                    &endpointId,
			CustomEndpointType:          aws.String("READER"),
			StaticMembers:               []string{"instance-1"},
		}},
	}}

	// The stub always returns identical info, so every poll after the first is "unchanged".
	monitor := newTestMonitor(t, mockContainer, endpointId, 20*time.Millisecond, mockCounter, api)
	monitor.Start()
	time.Sleep(200 * time.Millisecond)
	monitor.Stop()

	require.Greater(t, api.calls.Load(), int32(2),
		"need several polls to distinguish publish-once from republish-always")
	assert.Greater(t, storage.sets.Load(), int32(1),
		"host permissions were written %d time(s) across %d polls of unchanged endpoint info; "+
			"they must be republished so the entry cannot expire between changes",
		storage.sets.Load(), api.calls.Load())
}
