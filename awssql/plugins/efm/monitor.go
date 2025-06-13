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

package efm

import (
	"context"
	"database/sql/driver"
	"log/slog"
	"math"
	"strings"
	"sync"
	"time"
	"weak"

	"github.com/aws/aws-advanced-go-wrapper/awssql/driver_infrastructure"
	"github.com/aws/aws-advanced-go-wrapper/awssql/error_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/host_info_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/property_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils/telemetry"
)

var EFM_ROUTINE_SLEEP_DURATION = 100 * time.Millisecond

type Monitor interface {
	StartMonitoring(state *MonitorConnectionState)
	CanDispose() bool
	Close()
}

func NewMonitorImpl(pluginService driver_infrastructure.PluginService, hostInfo *host_info_util.HostInfo, props map[string]string,
	failureDetectionTimeMillis int, failureDetectionIntervalMillis int, failureDetectionCount int) *MonitorImpl {
	monitoringConnectionProps := props
	for propKey, propValue := range props {
		if strings.HasPrefix(propKey, property_util.MONITORING_PROPERTY_PREFIX) {
			monitoringConnectionProps[strings.TrimPrefix(propKey, property_util.MONITORING_PROPERTY_PREFIX)] = propValue
			delete(monitoringConnectionProps, propKey)
		}
	}
	monitor := &MonitorImpl{
		hostInfo:                      hostInfo,
		monitoringProps:               monitoringConnectionProps,
		pluginService:                 pluginService,
		failureDetectionTimeNanos:     time.Millisecond * time.Duration(failureDetectionTimeMillis),
		failureDetectionIntervalNanos: time.Millisecond * time.Duration(failureDetectionIntervalMillis),
		failureDetectionCount:         failureDetectionCount,
		NewStates:                     map[time.Time][]weak.Pointer[MonitorConnectionState]{},
	}

	monitor.wg.Add(2)
	go monitor.newStateRun()
	go monitor.run()

	return monitor
}

type MonitorImpl struct {
	hostInfo                      *host_info_util.HostInfo
	MonitoringConn                driver.Conn
	pluginService                 driver_infrastructure.PluginService
	monitoringProps               map[string]string
	failureDetectionTimeNanos     time.Duration
	failureDetectionIntervalNanos time.Duration
	FailureCount                  int
	failureDetectionCount         int
	InvalidHostStartTime          time.Time
	ActiveStates                  []weak.Pointer[MonitorConnectionState]
	NewStates                     map[time.Time][]weak.Pointer[MonitorConnectionState]
	Stopped                       bool
	HostUnhealthy                 bool
	lock                          sync.RWMutex
	wg                            sync.WaitGroup
}

func (m *MonitorImpl) CanDispose() bool {
	return len(m.ActiveStates) == 0 && len(m.NewStates) == 0
}

func (m *MonitorImpl) Close() {
	m.lock.Lock()
	m.Stopped = true
	m.lock.Unlock()

	m.wg.Wait()

	slog.Debug(error_util.GetMessage("MonitorImpl.stopped", m.hostInfo.Host))
}

func (m *MonitorImpl) StartMonitoring(state *MonitorConnectionState) {
	if m.isStopped() {
		slog.Warn(error_util.GetMessage("MonitorImpl.monitorIsStopped", m.hostInfo.Host))
	}

	startMonitoringTimeNano := time.Now().Add(m.failureDetectionTimeNanos)
	m.lock.Lock()
	m.NewStates[startMonitoringTimeNano] = []weak.Pointer[MonitorConnectionState]{weak.Make(state)}
	m.lock.Unlock()
}

func (m *MonitorImpl) newStateRun() {
	slog.Debug(error_util.GetMessage("MonitorImpl.startMonitoringRoutineNewState", m.hostInfo.Host))
	defer m.wg.Done()

	for !m.isStopped() {
		currentTime := time.Now()
		for startMonitoringTime, queuedStates := range m.NewStates {
			// Get entries with a starting time less than or equal to the current time.
			if startMonitoringTime.Before(currentTime) {
				// The value of an entry is the queued monitoring states awaiting active monitoring.
				// Add the states to an active monitoring states queue. Ignore disposed states.
				for _, stateWeakRef := range queuedStates {
					state := stateWeakRef.Value()
					if state != nil && state.IsActive() {
						m.ActiveStates = append(m.ActiveStates, stateWeakRef)
					}
				}
				// Remove the processed entry from new states.
				m.lock.Lock()
				delete(m.NewStates, startMonitoringTime)
				m.lock.Unlock()
			}
		}
		time.Sleep(time.Second)
	}

	slog.Debug(error_util.GetMessage("MonitorImpl.stopMonitoringRoutineNewState", m.hostInfo.Host))
}

func (m *MonitorImpl) run() {
	slog.Debug(error_util.GetMessage("MonitorImpl.startMonitoringRoutine", m.hostInfo.Host))
	defer m.wg.Done()

	for !m.isStopped() {
		if len(m.ActiveStates) == 0 && !m.HostUnhealthy {
			time.Sleep(EFM_ROUTINE_SLEEP_DURATION)
			continue
		}

		statusCheckStartTime := time.Now()
		connIsValid := m.CheckConnectionStatus()
		statusCheckEndTime := time.Now()
		m.UpdateHostHealthStatus(connIsValid, statusCheckStartTime, statusCheckEndTime)

		if m.HostUnhealthy {
			m.pluginService.SetAvailability(m.hostInfo.AllAliases, host_info_util.UNAVAILABLE)
		}

		tmpActiveStates := []weak.Pointer[MonitorConnectionState]{}
		for _, monitorStateWeakRef := range m.ActiveStates {
			if m.isStopped() {
				break
			}

			monitorState := monitorStateWeakRef.Value()
			if monitorState == nil {
				continue
			}

			if m.HostUnhealthy {
				// Kill connection.
				monitorState.SetHostUnhealthy(true)
				connToAbort := monitorState.GetConn()
				monitorState.SetInactive()
				if connToAbort != nil {
					(*connToAbort).Close()
				}
			} else if monitorState.IsActive() {
				tmpActiveStates = append(tmpActiveStates, monitorStateWeakRef)
			}
		}
		// Update activeStates to those that are still active.
		slog.Debug(error_util.GetMessage("MonitorImpl.updatingActiveStates", m.hostInfo.Host, len(m.ActiveStates), len(tmpActiveStates)))
		m.ActiveStates = tmpActiveStates
		delayDurationNanos := m.failureDetectionIntervalNanos - (statusCheckEndTime.Sub(statusCheckStartTime))
		if delayDurationNanos < EFM_ROUTINE_SLEEP_DURATION {
			delayDurationNanos = EFM_ROUTINE_SLEEP_DURATION
		}
		time.Sleep(delayDurationNanos)
	}
	if m.MonitoringConn != nil {
		m.MonitoringConn.Close()
	}
	slog.Debug(error_util.GetMessage("MonitorImpl.stopMonitoringRoutine", m.hostInfo.Host))
}

func (m *MonitorImpl) UpdateHostHealthStatus(connIsValid bool, statusCheckStartTime time.Time, statusCheckEndTime time.Time) {
	if !connIsValid {
		m.FailureCount++

		if m.InvalidHostStartTime.IsZero() {
			m.InvalidHostStartTime = statusCheckStartTime
		}

		invalidHostDurationTimeNanos := statusCheckEndTime.Sub(m.InvalidHostStartTime)
		maxInvalidDurationTimeNanos := m.failureDetectionIntervalNanos * time.Duration(math.Max(0, float64(m.failureDetectionCount-1)))

		if invalidHostDurationTimeNanos >= maxInvalidDurationTimeNanos {
			slog.Debug(error_util.GetMessage("MonitorImpl.hostDead", m.hostInfo.Host))
			m.HostUnhealthy = true
			return
		}

		slog.Debug(error_util.GetMessage("MonitorImpl.hostNotResponding", m.hostInfo.Host))
		return
	}

	if m.FailureCount > 0 {
		// Host is back alive.
		slog.Debug(error_util.GetMessage("MonitorImpl.hostAlive", m.hostInfo.Host))
	}

	m.FailureCount = 0
	m.InvalidHostStartTime = time.Time{}
	m.HostUnhealthy = false
}

func (m *MonitorImpl) CheckConnectionStatus() bool {
	parentCtx := m.pluginService.GetTelemetryContext()
	telemetryCtx, ctx := m.pluginService.GetTelemetryFactory().OpenTelemetryContext(telemetry.TELEMETRY_CONN_STATUS_CHECK, telemetry.FORCE_TOP_LEVEL, nil)
	telemetryCtx.SetAttribute(telemetry.TELEMETRY_ATTRIBUTE_URL, m.hostInfo.Host)
	m.pluginService.SetTelemetryContext(ctx)
	defer func() {
		telemetryCtx.CloseContext()
		m.pluginService.SetTelemetryContext(parentCtx)
	}()

	if m.MonitoringConn == nil || m.pluginService.GetTargetDriverDialect().IsClosed(m.MonitoringConn) {
		// Open a new connection.
		slog.Debug(error_util.GetMessage("MonitorImpl.openingMonitoringConnection", m.hostInfo.Host))
		newMonitoringConn, err := m.pluginService.ForceConnect(m.hostInfo, m.monitoringProps)
		if err != nil || newMonitoringConn == nil {
			return false
		}
		m.MonitoringConn = newMonitoringConn
		slog.Debug(error_util.GetMessage("MonitorImpl.openedMonitoringConnection", m.hostInfo.Host))
		return true
	}

	timeout := m.failureDetectionIntervalNanos - EFM_ROUTINE_SLEEP_DURATION

	// Ensure it can never be <= 0
	if timeout < EFM_ROUTINE_SLEEP_DURATION {
		timeout = EFM_ROUTINE_SLEEP_DURATION
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return utils.IsReachable(m.MonitoringConn, ctx)
}

func (m *MonitorImpl) isStopped() bool {
	m.lock.RLock()
	defer m.lock.RUnlock()
	return m.Stopped
}
