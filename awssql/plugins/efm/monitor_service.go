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
	"database/sql/driver"
	"fmt"
	"log/slog"
	"sync"
	"time"
	"weak"

	"github.com/aws/aws-advanced-go-wrapper/awssql/driver_infrastructure"
	"github.com/aws/aws-advanced-go-wrapper/awssql/error_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/host_info_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils/telemetry"
	"github.com/aws/aws-advanced-go-wrapper/awssql/property_util"
)

var EFM_MONITORS *utils.SlidingExpirationCache[Monitor]

type MonitorService interface {
	StartMonitoring(conn *driver.Conn, hostInfo *host_info_util.HostInfo, props *utils.RWMap[string, string]) (*MonitorConnectionState, error)
	StopMonitoring(state *MonitorConnectionState, connToAbort driver.Conn)
}

type MonitorServiceImpl struct {
	pluginService             	driver_infrastructure.PluginService
	abortedConnectionsCounter 	telemetry.TelemetryCounter
	failureDetectionTimeMillis 	int
	failureDetectionIntervalMillis 	int
	failureDetectionCount 		int
	monitorDisposalTimeMillis	int
	monitorKey			*MonitorKey
}

func NewMonitorServiceImpl(pluginService driver_infrastructure.PluginService, properties *utils.RWMap[string, string]) (*MonitorServiceImpl, error) {
	if EFM_MONITORS == nil {
		EFM_MONITORS = utils.NewSlidingExpirationCache(
			"efm_monitors",
			func(monitor Monitor) bool {
				monitor.Close()
				return false
			},
			func(monitor Monitor) bool {
				return monitor.CanDispose()
			})
	}

	abortedConnectionsCounter, err := pluginService.GetTelemetryFactory().CreateCounter("efm.connections.aborted")
	if err != nil {
		return nil, err
	}
	failureDetectionTimeMillis, err := property_util.GetPositiveIntProperty(properties, property_util.FAILURE_DETECTION_TIME_MS)
	if err != nil {
		return nil, err
	}
	failureDetectionIntervalMillis, err := property_util.GetPositiveIntProperty(properties, property_util.FAILURE_DETECTION_INTERVAL_MS)
	if err != nil {
		return nil, err
	}
	failureDetectionCount, err := property_util.GetPositiveIntProperty(properties, property_util.FAILURE_DETECTION_COUNT)
	if err != nil {
		return nil, err
	}
	monitorDisposalTimeMillis, err := property_util.GetPositiveIntProperty(properties, property_util.MONITOR_DISPOSAL_TIME_MS)
	if err != nil {
		return nil, err
	}
	return &MonitorServiceImpl{
		pluginService: pluginService,
		abortedConnectionsCounter: abortedConnectionsCounter,
		failureDetectionTimeMillis: failureDetectionTimeMillis,
		failureDetectionIntervalMillis: failureDetectionIntervalMillis,
		failureDetectionCount: failureDetectionCount,
		monitorDisposalTimeMillis: monitorDisposalTimeMillis,
		monitorKey: nil,
	}, nil
}

func (m *MonitorServiceImpl) StartMonitoring(conn *driver.Conn, hostInfo *host_info_util.HostInfo, props *utils.RWMap[string, string]) (*MonitorConnectionState, error) {
	if conn == nil {
		return nil, error_util.NewGenericAwsWrapperError(error_util.GetMessage("MonitorServiceImpl.illegalArgumentError", "conn"))
	}
	if hostInfo.IsNil() {
		return nil, error_util.NewGenericAwsWrapperError(error_util.GetMessage("MonitorServiceImpl.illegalArgumentError", "hostInfo"))
	}
	monitor := m.getMonitor(hostInfo, props)
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

func (m *MonitorServiceImpl) getMonitor(hostInfo *host_info_util.HostInfo, props *utils.RWMap[string, string]) Monitor {
	hostUrl := hostInfo.GetUrl()
	if m.monitorKey == nil || hostUrl != m.monitorKey.GetUrl() {
		m.monitorKey = NewMonitorKey(hostUrl, fmt.Sprintf("%d:%d:%d:%s", m.failureDetectionTimeMillis, m.failureDetectionIntervalMillis, m.failureDetectionCount, hostInfo.GetUrl()))
	}
	cacheExpirationNano := time.Millisecond * time.Duration(m.monitorDisposalTimeMillis)
	return EFM_MONITORS.ComputeIfAbsent(
		m.monitorKey.GetKeyValue(),
		func() Monitor {
			return NewMonitorImpl(
				m.pluginService,
				hostInfo,
				props,
				m.failureDetectionTimeMillis,
				m.failureDetectionIntervalMillis,
				m.failureDetectionCount,
				m.abortedConnectionsCounter)
		},
		cacheExpirationNano)
}

type MonitorKey struct {
	url 		string
	keyValue 	string
}

func NewMonitorKey(url string, keyValue string) *MonitorKey {
	return &MonitorKey{url: url, keyValue: keyValue}
}

func (m *MonitorKey) GetUrl() string {
	return m.url
}

func (m *MonitorKey) GetKeyValue() string {
	return m.keyValue
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
