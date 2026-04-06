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
	"testing"
	"time"
	"weak"

	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/driver_infrastructure"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/plugins/efm"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/property_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/services"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/utils/telemetry"
	"github.com/stretchr/testify/assert"
)

func newTestHostMonitor(failureTimeMs, failureIntervalMs, failureCount int) *efm.HostMonitorImpl {
	props := MakeMapFromKeysAndVals(property_util.DRIVER_PROTOCOL.Name, "postgresql")
	container, _ := efmTestContainer(props)
	monitor := efm.NewHostMonitorImpl(container, mockHostInfo, emptyProps, failureTimeMs, failureIntervalMs, failureCount, telemetry.NilTelemetryCounter{})
	return monitor
}

func TestHostMonitor_StartAndStop(t *testing.T) {
	monitor := newTestHostMonitor(30000, 5000, 3)

	assert.Equal(t, driver_infrastructure.MonitorStateStopped, monitor.GetState())

	monitor.Start()
	assert.Equal(t, driver_infrastructure.MonitorStateRunning, monitor.GetState())
	assert.True(t, monitor.GetLastActivityTimestampNanos() > 0)

	monitor.Stopped.Store(true)
	monitor.Stop()
	assert.Equal(t, driver_infrastructure.MonitorStateStopped, monitor.GetState())
}

func TestHostMonitor_UpdateHostHealthStatus_HostBecomesUnhealthy(t *testing.T) {
	monitor := newTestHostMonitor(0, 10, 1)
	monitor.Close()

	assert.False(t, monitor.HostUnhealthy.Load())
	assert.Zero(t, monitor.FailureCount.Load())

	startTime := time.Now()
	endTime := startTime.Add(20 * time.Millisecond)

	// First failure with count=1 should immediately mark unhealthy
	monitor.UpdateHostHealthStatus(false, startTime, endTime)
	assert.Equal(t, int32(1), monitor.FailureCount.Load())
	assert.True(t, monitor.HostUnhealthy.Load())
}

func TestHostMonitor_UpdateHostHealthStatus_HostRecovers(t *testing.T) {
	monitor := newTestHostMonitor(0, 10, 3)
	monitor.Close()

	// Simulate a failure
	startTime := time.Now()
	monitor.UpdateHostHealthStatus(false, startTime, startTime.Add(5*time.Millisecond))
	assert.Equal(t, int32(1), monitor.FailureCount.Load())
	assert.False(t, monitor.HostUnhealthy.Load())

	// Host recovers
	monitor.UpdateHostHealthStatus(true, time.Now(), time.Now().Add(5*time.Millisecond))
	assert.Zero(t, monitor.FailureCount.Load())
	assert.True(t, monitor.InvalidHostStartTime.IsZero())
	assert.False(t, monitor.HostUnhealthy.Load())
}

func TestHostMonitor_UpdateHostHealthStatus_MultipleFailuresBeforeUnhealthy(t *testing.T) {
	// failureDetectionCount=3, failureDetectionIntervalNanos=100ms
	monitor := newTestHostMonitor(0, 100, 3)
	monitor.Close()

	startTime := time.Now()

	// First failure - not yet unhealthy
	monitor.UpdateHostHealthStatus(false, startTime, startTime.Add(1*time.Millisecond))
	assert.Equal(t, int32(1), monitor.FailureCount.Load())
	assert.False(t, monitor.HostUnhealthy.Load())

	// Second failure - still not unhealthy (need duration >= interval * (count-1))
	monitor.UpdateHostHealthStatus(false, startTime, startTime.Add(50*time.Millisecond))
	assert.Equal(t, int32(2), monitor.FailureCount.Load())
	assert.False(t, monitor.HostUnhealthy.Load())

	// Third failure - now duration exceeds threshold (200ms >= 100ms * 2)
	monitor.UpdateHostHealthStatus(false, startTime, startTime.Add(250*time.Millisecond))
	assert.Equal(t, int32(3), monitor.FailureCount.Load())
	assert.True(t, monitor.HostUnhealthy.Load())
}

func TestHostMonitor_CanDispose_EmptyStates(t *testing.T) {
	monitor := newTestHostMonitor(0, 10, 0)
	monitor.Close()

	assert.True(t, monitor.CanDispose())
}

func TestHostMonitor_CanDispose_WithNewStates(t *testing.T) {
	monitor := newTestHostMonitor(0, 10, 0)
	monitor.Close()

	monitor.NewStates.Put(time.Now(), nil)
	assert.False(t, monitor.CanDispose())
}

func TestHostMonitor_CanDispose_WithActiveStates(t *testing.T) {
	monitor := newTestHostMonitor(0, 10, 0)
	monitor.Close()

	var conn driver.Conn = &MockDriverConn{}
	state := efm.NewMonitorConnectionState(&conn)
	monitor.ActiveStates.Enqueue(weak.Make(state))
	assert.False(t, monitor.CanDispose())
}

func TestHostMonitor_StartMonitoring_AddsNewState(t *testing.T) {
	monitor := newTestHostMonitor(100, 10, 3)
	monitor.Close()

	var conn driver.Conn = &MockDriverConn{}
	state := efm.NewMonitorConnectionState(&conn)

	monitor.StartMonitoring(state)
	assert.Equal(t, 1, monitor.NewStates.Size())
}

func TestHostMonitor_ProcessEvent_MonitorResetEvent(t *testing.T) {
	monitor := newTestHostMonitor(0, 10, 3)
	mockConn := &MockDriverConnection{}
	monitor.MonitoringConn = mockConn
	monitor.FailureCount.Store(5)
	monitor.HostUnhealthy.Store(true)
	monitor.InvalidHostStartTime = time.Now()

	endpoints := map[string]struct{}{
		mockHostInfo.Host: {},
	}
	event := services.MonitorResetEvent{
		Endpoints: endpoints,
	}
	monitor.ProcessEvent(event)

	// After reset, failure tracking should be cleared
	assert.Zero(t, monitor.FailureCount.Load())
	assert.False(t, monitor.HostUnhealthy.Load())
	assert.True(t, monitor.InvalidHostStartTime.IsZero())
	assert.Nil(t, monitor.MonitoringConn)
	assert.True(t, mockConn.IsClosed)
}

func TestHostMonitor_ProcessEvent_MonitorResetEvent_DifferentHost(t *testing.T) {
	monitor := newTestHostMonitor(0, 10, 3)
	monitor.Close()
	monitor.FailureCount.Store(5)
	monitor.HostUnhealthy.Store(true)

	// Event for a different host - should not reset
	endpoints := map[string]struct{}{
		"some-other-host": {},
	}
	event := services.MonitorResetEvent{
		Endpoints: endpoints,
	}
	monitor.ProcessEvent(event)

	// Should remain unchanged
	assert.Equal(t, int32(5), monitor.FailureCount.Load())
	assert.True(t, monitor.HostUnhealthy.Load())
}

func TestHostMonitor_ProcessEvent_NonResetEvent(t *testing.T) {
	monitor := newTestHostMonitor(0, 10, 3)
	monitor.Close()
	monitor.FailureCount.Store(5)

	// A non-reset event should not affect the monitor
	event := services.DataAccessEvent{
		TypeKey: "test",
		Key:     "test-key",
	}
	monitor.ProcessEvent(event)

	// Should remain unchanged
	assert.Equal(t, int32(5), monitor.FailureCount.Load())
}

func TestHostMonitor_Close_ClosesMonitoringConnection(t *testing.T) {
	monitor := newTestHostMonitor(0, 10, 3)
	mockConn := &MockDriverConnection{}
	monitor.MonitoringConn = mockConn

	assert.False(t, mockConn.IsClosed)
	monitor.Close()
	assert.True(t, mockConn.IsClosed)
}

func TestHostMonitor_Close_NilMonitoringConnection(t *testing.T) {
	monitor := newTestHostMonitor(0, 10, 3)
	monitor.MonitoringConn = nil

	// Should not panic
	monitor.Close()
}

func TestHostMonitor_GetState_DefaultIsStopped(t *testing.T) {
	monitor := newTestHostMonitor(0, 10, 3)
	monitor.Close()

	assert.Equal(t, driver_infrastructure.MonitorStateStopped, monitor.GetState())
}

func TestHostMonitor_GetLastActivityTimestampNanos_DefaultIsZero(t *testing.T) {
	monitor := newTestHostMonitor(0, 10, 3)
	monitor.Close()

	assert.Equal(t, int64(0), monitor.GetLastActivityTimestampNanos())
}
