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
	"awssql/driver_infrastructure"
	"awssql/error_util"
	"awssql/host_info_util"
	"awssql/property_util"
	"awssql/utils"
	"context"
	"database/sql/driver"
	"log/slog"
	"math"
	"strings"
	"sync"
	"time"
	"weak"
)

var EFM_ROUTINE_SLEEP_DURATION = 100 * time.Millisecond

type Monitor interface {
	StartMonitoring(context *MonitorConnectionContext)
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
		NewContexts:                   map[time.Time][]weak.Pointer[MonitorConnectionContext]{}}

	monitor.wg.Add(2)
	go monitor.newContextRun()
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
	ActiveContexts                []weak.Pointer[MonitorConnectionContext]
	NewContexts                   map[time.Time][]weak.Pointer[MonitorConnectionContext]
	Stopped                       bool
	HostUnhealthy                 bool
	lock                          sync.RWMutex
	wg                            sync.WaitGroup
}

func (m *MonitorImpl) CanDispose() bool {
	return len(m.ActiveContexts) == 0 && len(m.NewContexts) == 0
}

func (m *MonitorImpl) Close() {
	m.lock.Lock()
	m.Stopped = true
	m.lock.Unlock()

	m.wg.Wait()

	slog.Debug(error_util.GetMessage("MonitorImpl.stopped", m.hostInfo.Host))
}

func (m *MonitorImpl) StartMonitoring(context *MonitorConnectionContext) {
	if m.isStopped() {
		slog.Warn(error_util.GetMessage("MonitorImpl.monitorIsStopped", m.hostInfo.Host))
	}

	startMonitoringTimeNano := time.Now().Add(m.failureDetectionTimeNanos)
	m.lock.Lock()
	m.NewContexts[startMonitoringTimeNano] = []weak.Pointer[MonitorConnectionContext]{weak.Make(context)}
	m.lock.Unlock()
}

func (m *MonitorImpl) newContextRun() {
	slog.Debug(error_util.GetMessage("MonitorImpl.startMonitoringRoutineNewContext", m.hostInfo.Host))
	defer m.wg.Done()

	for !m.isStopped() {
		currentTime := time.Now()
		for startMonitoringTime, queuedContexts := range m.NewContexts {
			// Get entries with a starting time less than or equal to the current time.
			if startMonitoringTime.Before(currentTime) {
				// The value of an entry is the queued monitoring contexts awaiting active monitoring.
				// Add the contexts to an active monitoring contexts queue. Ignore disposed contexts.
				for _, contextWeakRef := range queuedContexts {
					context := contextWeakRef.Value()
					if context != nil && context.IsActive() {
						m.ActiveContexts = append(m.ActiveContexts, contextWeakRef)
					}
				}
				// Remove the processed entry from new contexts.
				m.lock.Lock()
				delete(m.NewContexts, startMonitoringTime)
				m.lock.Unlock()
			}
		}
		time.Sleep(time.Second)
	}

	slog.Debug(error_util.GetMessage("MonitorImpl.stopMonitoringRoutineNewContext", m.hostInfo.Host))
}

func (m *MonitorImpl) run() {
	slog.Debug(error_util.GetMessage("MonitorImpl.startMonitoringRoutine", m.hostInfo.Host))
	defer m.wg.Done()

	for !m.isStopped() {
		if len(m.ActiveContexts) == 0 && !m.HostUnhealthy {
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

		var tmpActiveContexts []weak.Pointer[MonitorConnectionContext]
		for _, monitorContextWeakRef := range m.ActiveContexts {
			if m.isStopped() {
				break
			}

			monitorContext := monitorContextWeakRef.Value()
			if monitorContext == nil {
				continue
			}

			if m.HostUnhealthy {
				// Kill connection.
				monitorContext.SetHostUnhealthy(true)
				connToAbort := monitorContext.GetConn()
				monitorContext.SetInactive()
				if connToAbort != nil {
					(*connToAbort).Close()
				}
			} else if monitorContext.IsActive() {
				tmpActiveContexts = append(tmpActiveContexts, monitorContextWeakRef)
			}
		}
		// Update activeContexts to those that are still active.
		m.ActiveContexts = tmpActiveContexts
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
	if m.MonitoringConn == nil || utils.IsConnectionLost(m.MonitoringConn) {
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

	// If implemented, validate by driver.Validator. Currently, only mysql Conns support this.
	validator, implementsIsValid := m.MonitoringConn.(driver.Validator)
	if implementsIsValid {
		return validator.IsValid()
	}
	// As a last resort, check connection by executing a query.
	queryer, implementsQueryer := m.MonitoringConn.(driver.QueryerContext)
	if implementsQueryer {
		_, err := queryer.QueryContext(context.TODO(), "select 1", []driver.NamedValue{})
		return err == nil
	}
	return false
}

func (m *MonitorImpl) isStopped() bool {
	m.lock.RLock()
	defer m.lock.RUnlock()
	return m.Stopped
}
