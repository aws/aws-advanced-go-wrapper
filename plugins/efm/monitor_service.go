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
	"database/sql/driver"
	"fmt"
	"log/slog"
	"sync"
	"time"
	"weak"
)

var EFM_MONITORS *utils.SlidingExpirationCache[Monitor]

type MonitorService interface {
	StartMonitoring(conn *driver.Conn, hostInfo *host_info_util.HostInfo, props map[string]string,
		failureDetectionTimeMillis int, failureDetectionIntervalMillis int, failureDetectionCount int) (*MonitorConnectionState, error)
	StopMonitoring(state *MonitorConnectionState, connToAbort driver.Conn)
}

type MonitorServiceImpl struct {
	pluginService driver_infrastructure.PluginService
}

func NewMonitorServiceImpl(pluginService driver_infrastructure.PluginService) *MonitorServiceImpl {
	if EFM_MONITORS == nil {
		EFM_MONITORS = utils.NewSlidingExpirationCache[Monitor](
			"efm_monitors",
			func(monitor Monitor) bool {
				monitor.Close()
				return false
			},
			func(monitor Monitor) bool {
				return monitor.CanDispose()
			})
	}

	return &MonitorServiceImpl{pluginService: pluginService}
}

func ClearCaches() {
	if EFM_MONITORS != nil {
		EFM_MONITORS.Clear()
	}
}

func (m *MonitorServiceImpl) StartMonitoring(conn *driver.Conn, hostInfo *host_info_util.HostInfo, props map[string]string,
	failureDetectionTimeMillis int, failureDetectionIntervalMillis int, failureDetectionCount int) (*MonitorConnectionState, error) {
	if conn == nil {
		return nil, error_util.NewGenericAwsWrapperError(error_util.GetMessage("MonitorServiceImpl.illegalArgumentException", "conn"))
	}
	if hostInfo.IsNil() {
		return nil, error_util.NewGenericAwsWrapperError(error_util.GetMessage("MonitorServiceImpl.illegalArgumentException", "hostInfo"))
	}
	monitor := m.getMonitor(hostInfo, props, failureDetectionTimeMillis, failureDetectionIntervalMillis, failureDetectionCount)
	state := NewMonitorConnectionState(conn)
	monitor.StartMonitoring(state)
	return state, nil
}

func (m *MonitorServiceImpl) StopMonitoring(state *MonitorConnectionState, connToAbort driver.Conn) {
	if state.ShouldAbort() {
		state.SetInactive()
		err := connToAbort.Close()
		if err != nil {
			slog.Debug(error_util.GetMessage("MonitorServiceImpl.errorAbortingConn", err.Error()))
		}
	} else {
		state.SetInactive()
	}
}

func (m *MonitorServiceImpl) getMonitor(hostInfo *host_info_util.HostInfo, props map[string]string, failureDetectionTimeMillis int,
	failureDetectionIntervalMillis int, failureDetectionCount int) Monitor {
	monitorKey := fmt.Sprintf("%d:%d:%d:%s", failureDetectionTimeMillis, failureDetectionIntervalMillis, failureDetectionCount, hostInfo.GetUrl())
	cacheExpirationNano := time.Millisecond * time.Duration(property_util.GetVerifiedWrapperPropertyValue[int](props, property_util.MONITOR_DISPOSAL_TIME_MS))
	return EFM_MONITORS.ComputeIfAbsent(
		monitorKey,
		func() Monitor {
			return NewMonitorImpl(m.pluginService, hostInfo, props, failureDetectionTimeMillis, failureDetectionIntervalMillis, failureDetectionCount)
		},
		cacheExpirationNano)
}

type MonitorConnectionState struct {
	hostUnhealthy  bool
	connToAbortRef weak.Pointer[driver.Conn]
	connLock       sync.RWMutex
	hostHealthLock sync.RWMutex
}

func NewMonitorConnectionState(connToAbort *driver.Conn) *MonitorConnectionState {
	return &MonitorConnectionState{hostUnhealthy: false, connToAbortRef: weak.Make(connToAbort)}
}

func (m *MonitorConnectionState) SetHostUnhealthy(hostUnhealthy bool) {
	m.hostHealthLock.Lock()
	defer m.hostHealthLock.Unlock()
	m.hostUnhealthy = hostUnhealthy
}

func (m *MonitorConnectionState) SetInactive() {
	m.connLock.Lock()
	defer m.connLock.Unlock()

	var nilPointer *driver.Conn
	m.connToAbortRef = weak.Make((*driver.Conn)(nilPointer))
}

func (m *MonitorConnectionState) ShouldAbort() bool {
	m.connLock.RLock()
	m.hostHealthLock.RLock()
	defer m.connLock.RUnlock()
	defer m.hostHealthLock.RUnlock()

	return m.hostUnhealthy && m.connToAbortRef.Value() != nil
}

func (m *MonitorConnectionState) GetConn() *driver.Conn {
	m.connLock.RLock()
	defer m.connLock.RUnlock()

	return m.connToAbortRef.Value()
}

func (m *MonitorConnectionState) IsActive() bool {
	m.connLock.RLock()
	defer m.connLock.RUnlock()

	return m.connToAbortRef.Value() != nil
}
