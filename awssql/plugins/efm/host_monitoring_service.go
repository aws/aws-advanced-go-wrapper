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
	"github.com/aws/aws-advanced-go-wrapper/awssql/property_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils/telemetry"
)

// HostMonitorType is the type descriptor for host monitors.
// Used with MonitorService to manage HostMonitor instances.
var HostMonitorType = &driver_infrastructure.MonitorType{Name: "HostMonitor"}

type HostMonitoringService interface {
	StartMonitoring(conn *driver.Conn, hostInfo *host_info_util.HostInfo, props *utils.RWMap[string, string]) (*MonitorConnectionState, error)
	StopMonitoring(state *MonitorConnectionState, connToAbort driver.Conn)
	ReleaseResources()
}

type HostMonitoringServiceImpl struct {
	servicesContainer              driver_infrastructure.ServicesContainer
	pluginService                  driver_infrastructure.PluginService
	monitorService                 driver_infrastructure.MonitorService
	abortedConnectionsCounter      telemetry.TelemetryCounter
	failureDetectionTimeMillis     int
	failureDetectionIntervalMillis int
	failureDetectionCount          int
	monitorDisposalTimeMillis      int
	monitorKey                     *MonitorKey
}

func NewHostMonitoringServiceImpl(
	servicesContainer driver_infrastructure.ServicesContainer,
	properties *utils.RWMap[string, string],
) (*HostMonitoringServiceImpl, error) {
	pluginService := servicesContainer.GetPluginService()
	monitorService := servicesContainer.GetMonitorService()

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

	// Register the monitor type with the monitor service
	monitorService.RegisterMonitorType(
		HostMonitorType,
		&driver_infrastructure.MonitorSettings{
			ExpirationTimeout: time.Millisecond * time.Duration(monitorDisposalTimeMillis),
			InactiveTimeout:   3 * time.Minute,
			ErrorResponses:    nil,
		},
		"", // No produced data type for host monitors
	)

	return &HostMonitoringServiceImpl{
		servicesContainer:              servicesContainer,
		pluginService:                  pluginService,
		monitorService:                 monitorService,
		abortedConnectionsCounter:      abortedConnectionsCounter,
		failureDetectionTimeMillis:     failureDetectionTimeMillis,
		failureDetectionIntervalMillis: failureDetectionIntervalMillis,
		failureDetectionCount:          failureDetectionCount,
		monitorDisposalTimeMillis:      monitorDisposalTimeMillis,
		monitorKey:                     nil,
	}, nil
}

// CloseAllMonitors stops and removes all host monitors.
func CloseAllMonitors(monitorService driver_infrastructure.MonitorService) {
	monitorService.StopAndRemoveByType(HostMonitorType)
}

func (m *HostMonitoringServiceImpl) StartMonitoring(conn *driver.Conn, hostInfo *host_info_util.HostInfo, props *utils.RWMap[string, string]) (*MonitorConnectionState, error) {
	if conn == nil {
		return nil, error_util.NewGenericAwsWrapperError(error_util.GetMessage("HostMonitoringServiceImpl.illegalArgumentError", "conn"))
	}
	if hostInfo.IsNil() {
		return nil, error_util.NewGenericAwsWrapperError(error_util.GetMessage("HostMonitoringServiceImpl.illegalArgumentError", "hostInfo"))
	}
	monitor, err := m.getMonitor(hostInfo, props)
	if err != nil {
		return nil, err
	}
	state := NewMonitorConnectionState(conn)
	monitor.StartMonitoring(state)
	return state, nil
}

func (m *HostMonitoringServiceImpl) StopMonitoring(state *MonitorConnectionState, connToAbort driver.Conn) {
	if state.ShouldAbort() {
		state.SetInactive()
		err := connToAbort.Close()
		if err != nil {
			slog.Debug(error_util.GetMessage("HostMonitoringServiceImpl.errorAbortingConn", err.Error()))
		}
	} else {
		state.SetInactive()
	}
}

func (m *HostMonitoringServiceImpl) ReleaseResources() {
	// Nothing to do - monitors are managed by the MonitorService
}

func (m *HostMonitoringServiceImpl) getMonitor(hostInfo *host_info_util.HostInfo, props *utils.RWMap[string, string]) (HostMonitor, error) {
	hostUrl := hostInfo.GetUrl()
	if m.monitorKey == nil || hostUrl != m.monitorKey.GetUrl() {
		m.monitorKey = NewMonitorKey(hostUrl, fmt.Sprintf("%d:%d:%d:%s", m.failureDetectionTimeMillis, m.failureDetectionIntervalMillis, m.failureDetectionCount, hostInfo.GetUrl()))
	}

	// Capture values for the initializer closure
	failureDetectionTimeMillis := m.failureDetectionTimeMillis
	failureDetectionIntervalMillis := m.failureDetectionIntervalMillis
	failureDetectionCount := m.failureDetectionCount
	abortedConnectionsCounter := m.abortedConnectionsCounter
	hostInfoCopy := hostInfo
	propsCopy := props

	monitor, err := m.monitorService.RunIfAbsent(
		HostMonitorType,
		m.monitorKey.GetKeyValue(),
		m.servicesContainer,
		func(container driver_infrastructure.ServicesContainer) (driver_infrastructure.Monitor, error) {
			return NewHostMonitorImpl(
				container,
				hostInfoCopy,
				propsCopy,
				failureDetectionTimeMillis,
				failureDetectionIntervalMillis,
				failureDetectionCount,
				abortedConnectionsCounter,
			), nil
		},
	)
	if err != nil {
		return nil, err
	}

	// Type assert to HostMonitor
	hostMonitor, ok := monitor.(HostMonitor)
	if !ok {
		return nil, error_util.NewGenericAwsWrapperError("monitor is not a HostMonitor")
	}
	return hostMonitor, nil
}

type MonitorKey struct {
	url      string
	keyValue string
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
