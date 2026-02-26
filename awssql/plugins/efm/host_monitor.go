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
	"sync/atomic"
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

// MonitorResetEventType reference for subscribing to reset events.
// The actual event type is defined in services/events.go.
var MonitorResetEventType = &driver_infrastructure.EventType{Name: "MonitorReset"}

type HostMonitor interface {
	driver_infrastructure.Monitor
	driver_infrastructure.EventSubscriber
	StartMonitoring(state *MonitorConnectionState)
}

func NewHostMonitorImpl(
	servicesContainer driver_infrastructure.ServicesContainer,
	hostInfo *host_info_util.HostInfo,
	props *utils.RWMap[string, string],
	failureDetectionTimeMillis int,
	failureDetectionIntervalMillis int,
	failureDetectionCount int,
	abortedConnectionsCounter telemetry.TelemetryCounter,
) *HostMonitorImpl {
	copyProps := props.GetAllEntries()
	monitoringConnectionProps := utils.NewRWMapFromMap(copyProps)
	for propKey, propValue := range copyProps {
		if strings.HasPrefix(propKey, property_util.MONITORING_PROPERTY_PREFIX) {
			monitoringConnectionProps.Put(strings.TrimPrefix(propKey, property_util.MONITORING_PROPERTY_PREFIX), propValue)
			monitoringConnectionProps.Remove(propKey)
		}
	}
	monitor := &HostMonitorImpl{
		servicesContainer:             servicesContainer,
		hostInfo:                      hostInfo,
		monitoringProps:               monitoringConnectionProps,
		pluginService:                 servicesContainer.GetPluginService(),
		failureDetectionTimeNanos:     time.Millisecond * time.Duration(failureDetectionTimeMillis),
		failureDetectionIntervalNanos: time.Millisecond * time.Duration(failureDetectionIntervalMillis),
		failureDetectionCount:         failureDetectionCount,
		NewStates:                     utils.NewRWMap[time.Time, []weak.Pointer[MonitorConnectionState]](),
		ActiveStates:                  utils.NewRWQueue[weak.Pointer[MonitorConnectionState]](),
		abortedConnectionsCounter:     abortedConnectionsCounter,
	}

	return monitor
}

type HostMonitorImpl struct {
	servicesContainer             driver_infrastructure.ServicesContainer
	hostInfo                      *host_info_util.HostInfo
	MonitoringConn                driver.Conn
	pluginService                 driver_infrastructure.PluginService
	monitoringProps               *utils.RWMap[string, string]
	failureDetectionTimeNanos     time.Duration
	failureDetectionIntervalNanos time.Duration
	FailureCount                  atomic.Int32
	failureDetectionCount         int
	InvalidHostStartTime          time.Time
	ActiveStates                  *utils.RWQueue[weak.Pointer[MonitorConnectionState]]
	NewStates                     *utils.RWMap[time.Time, []weak.Pointer[MonitorConnectionState]]
	Stopped                       atomic.Bool
	HostUnhealthy                 atomic.Bool
	wg                            sync.WaitGroup
	abortedConnectionsCounter     telemetry.TelemetryCounter
	state                         atomic.Value // driver_infrastructure.MonitorState
	lastActivityTimestampNano     atomic.Int64
}

func (m *HostMonitorImpl) CanDispose() bool {
	return m.ActiveStates.IsEmpty() && m.NewStates.Size() == 0
}

func (m *HostMonitorImpl) Start() {
	m.state.Store(driver_infrastructure.MonitorStateRunning)
	m.lastActivityTimestampNano.Store(time.Now().UnixNano())
	m.wg.Add(2)
	go m.newStateRun()
	go m.Monitor()
}

func (m *HostMonitorImpl) Stop() {
	m.Stopped.Store(true)
	m.wg.Wait()
	m.Close()
	m.state.Store(driver_infrastructure.MonitorStateStopped)
}

func (m *HostMonitorImpl) Close() {
	if m.MonitoringConn != nil {
		_ = m.MonitoringConn.Close()
	}
	slog.Debug(error_util.GetMessage("HostMonitorImpl.stopped", m.hostInfo.Host))
}

func (m *HostMonitorImpl) GetLastActivityTimestampNanos() int64 {
	return m.lastActivityTimestampNano.Load()
}

func (m *HostMonitorImpl) GetState() driver_infrastructure.MonitorState {
	if state := m.state.Load(); state != nil {
		return state.(driver_infrastructure.MonitorState)
	}
	return driver_infrastructure.MonitorStateStopped
}

func (m *HostMonitorImpl) StartMonitoring(state *MonitorConnectionState) {
	if m.isStopped() {
		slog.Warn(error_util.GetMessage("HostMonitorImpl.monitorIsStopped", m.hostInfo.Host))
	}

	startMonitoringTimeNano := time.Now().Add(m.failureDetectionTimeNanos)
	m.NewStates.Put(startMonitoringTimeNano, []weak.Pointer[MonitorConnectionState]{weak.Make(state)})
}

func (m *HostMonitorImpl) newStateRun() {
	slog.Debug(error_util.GetMessage("HostMonitorImpl.startMonitoringRoutineNewState", m.hostInfo.Host))
	defer m.wg.Done()

	for !m.isStopped() {
		currentTime := time.Now()
		m.NewStates.ProcessAndRemoveIf(
			func(startMonitoringTime time.Time) bool {
				return startMonitoringTime.Before(currentTime)
			},
			func(_ time.Time, queuedStates []weak.Pointer[MonitorConnectionState]) {
				for _, stateWeakRef := range queuedStates {
					state := stateWeakRef.Value()
					if state != nil && state.IsActive() {
						m.ActiveStates.Enqueue(stateWeakRef)
					}
				}
			},
		)
		time.Sleep(time.Second)
	}

	slog.Debug(error_util.GetMessage("HostMonitorImpl.stopMonitoringRoutineNewState", m.hostInfo.Host))
}

func (m *HostMonitorImpl) Monitor() {
	slog.Debug(error_util.GetMessage("HostMonitorImpl.startMonitoringRoutine", m.hostInfo.Host))
	defer m.wg.Done()

	// Subscribe to MonitorResetEvent
	if eventPublisher := m.servicesContainer.GetEventPublisher(); eventPublisher != nil {
		eventPublisher.Subscribe(m, []*driver_infrastructure.EventType{MonitorResetEventType})
		defer eventPublisher.Unsubscribe(m, []*driver_infrastructure.EventType{MonitorResetEventType})
	}

	for !m.isStopped() {
		m.lastActivityTimestampNano.Store(time.Now().UnixNano())
		activeStatesEmpty := m.ActiveStates.IsEmpty()

		if activeStatesEmpty && !m.HostUnhealthy.Load() {
			time.Sleep(EFM_ROUTINE_SLEEP_DURATION)
			continue
		}

		statusCheckStartTime := time.Now()
		connIsValid := m.CheckConnectionStatus()
		statusCheckEndTime := time.Now()
		m.UpdateHostHealthStatus(connIsValid, statusCheckStartTime, statusCheckEndTime)

		if m.HostUnhealthy.Load() {
			m.pluginService.SetAvailability(m.hostInfo.AllAliases, host_info_util.UNAVAILABLE)
		}

		tmpActiveStates := utils.NewRWQueue[weak.Pointer[MonitorConnectionState]]()
		for {
			monitorStateWeakRef, ok := m.ActiveStates.Dequeue()
			if !ok {
				break
			}
			if m.isStopped() {
				break
			}

			monitorState := monitorStateWeakRef.Value()
			if monitorState == nil {
				continue
			}

			if m.HostUnhealthy.Load() {
				// Kill connection.
				monitorState.SetHostUnhealthy(true)
				connToAbort := monitorState.GetConn()
				monitorState.SetInactive()
				if connToAbort != nil {
					_ = (*connToAbort).Close()
					m.abortedConnectionsCounter.Inc(m.pluginService.GetTelemetryContext())
				}
			} else if monitorState.IsActive() {
				tmpActiveStates.Enqueue(monitorStateWeakRef)
			}
		}
		// Update activeStates to those that are still active.
		activeStatesSize := m.ActiveStates.Size()
		tmpActiveStatesSize := tmpActiveStates.Size()
		if activeStatesSize != 0 || tmpActiveStatesSize != 0 {
			slog.Debug(error_util.GetMessage("HostMonitorImpl.updatingActiveStates", m.hostInfo.Host, activeStatesSize, tmpActiveStatesSize))
		}
		m.ActiveStates = tmpActiveStates
		delayDurationNanos := m.failureDetectionIntervalNanos - (statusCheckEndTime.Sub(statusCheckStartTime))
		if delayDurationNanos < EFM_ROUTINE_SLEEP_DURATION {
			delayDurationNanos = EFM_ROUTINE_SLEEP_DURATION
		}
		time.Sleep(delayDurationNanos)
	}
	slog.Debug(error_util.GetMessage("HostMonitorImpl.stopMonitoringRoutine", m.hostInfo.Host))
}

func (m *HostMonitorImpl) UpdateHostHealthStatus(connIsValid bool, statusCheckStartTime time.Time, statusCheckEndTime time.Time) {
	if !connIsValid {
		m.FailureCount.Add(1)

		if m.InvalidHostStartTime.IsZero() {
			m.InvalidHostStartTime = statusCheckStartTime
		}

		invalidHostDurationTimeNanos := statusCheckEndTime.Sub(m.InvalidHostStartTime)
		maxInvalidDurationTimeNanos := m.failureDetectionIntervalNanos * time.Duration(math.Max(0, float64(m.failureDetectionCount-1)))

		if invalidHostDurationTimeNanos >= maxInvalidDurationTimeNanos {
			slog.Debug(error_util.GetMessage("HostMonitorImpl.hostDead", m.hostInfo.Host))
			m.HostUnhealthy.Store(true)
			return
		}

		slog.Debug(error_util.GetMessage("HostMonitorImpl.hostNotResponding", m.hostInfo.Host))
		return
	}

	if m.FailureCount.Load() > 0 {
		// Host is back alive.
		slog.Debug(error_util.GetMessage("HostMonitorImpl.hostAlive", m.hostInfo.Host))
	}

	m.FailureCount.Store(0)
	m.InvalidHostStartTime = time.Time{}
	m.HostUnhealthy.Store(false)
}

func (m *HostMonitorImpl) CheckConnectionStatus() bool {
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
		slog.Debug(error_util.GetMessage("HostMonitorImpl.openingMonitoringConnection", m.hostInfo.Host))
		newMonitoringConn, err := m.pluginService.ForceConnect(m.hostInfo, m.monitoringProps)
		if err != nil || newMonitoringConn == nil {
			return false
		}
		m.MonitoringConn = newMonitoringConn
		slog.Debug(error_util.GetMessage("HostMonitorImpl.openedMonitoringConnection", m.hostInfo.Host))
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

func (m *HostMonitorImpl) isStopped() bool {
	return m.Stopped.Load()
}

// ProcessEvent handles events from the EventPublisher.
// Implements EventSubscriber interface.
func (m *HostMonitorImpl) ProcessEvent(event driver_infrastructure.Event) {
	// Check if this is a MonitorResetEvent by checking the event type name
	if event.GetEventType().Name == MonitorResetEventType.Name {
		slog.Debug(error_util.GetMessage("HostMonitorImpl.resetEventReceived", m.hostInfo.Host))
		// Use type assertion to get the endpoints
		if resetEvent, ok := event.(interface{ GetEndpoints() map[string]struct{} }); ok {
			endpoints := resetEvent.GetEndpoints()
			if _, found := endpoints[m.hostInfo.Host]; found {
				m.reset()
			}
		}
	}
}

// reset resets the monitor state, clearing the monitoring connection and failure tracking.
// This is called when a MonitorResetEvent is received for this host.
func (m *HostMonitorImpl) reset() {
	slog.Debug(error_util.GetMessage("HostMonitorImpl.reset", m.hostInfo.Host))

	// Close and clear monitoring connection
	if m.MonitoringConn != nil {
		_ = m.MonitoringConn.Close()
		m.MonitoringConn = nil
	}

	// Reset failure tracking
	m.InvalidHostStartTime = time.Time{}
	m.FailureCount.Store(0)
	m.HostUnhealthy.Store(false)
}
