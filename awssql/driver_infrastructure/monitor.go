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

package driver_infrastructure

import "time"

// MonitorState represents the current state of a monitor.
type MonitorState int

const (
	MonitorStateRunning MonitorState = iota
	MonitorStateStopped
	MonitorStateError
)

// MonitorErrorResponse defines actions to take when a monitor encounters an error.
type MonitorErrorResponse int

const (
	MonitorErrorRecreate MonitorErrorResponse = iota
	MonitorErrorLog
	MonitorErrorRemove
)

// Monitor is the interface all monitors must implement.
type Monitor interface {
	Start()
	Monitor()
	Stop()
	Close()
	GetLastActivityTimestampNanos() int64
	GetState() MonitorState
	CanDispose() bool
}

// MonitorInitializer creates a new monitor instance.
type MonitorInitializer func(container ServicesContainer) (Monitor, error)

// MonitorSettings holds configuration for a monitor type.
type MonitorSettings struct {
	ExpirationTimeout time.Duration
	InactiveTimeout   time.Duration
	ErrorResponses    map[MonitorErrorResponse]bool
}

// MonitorType is a type descriptor for monitors.
// It provides type-safe monitor type identification.
type MonitorType struct {
	Name string
}

// MonitorService manages background monitors with expiration and health checks.
type MonitorService interface {
	RegisterMonitorType(monitorType *MonitorType, settings *MonitorSettings, producedDataType string)
	RunIfAbsent(monitorType *MonitorType, key any, container ServicesContainer, initializer MonitorInitializer) (Monitor, error)
	Get(monitorType *MonitorType, key any) Monitor
	Remove(monitorType *MonitorType, key any) Monitor
	StopAndRemove(monitorType *MonitorType, key any)
	StopAndRemoveByType(monitorType *MonitorType)
	StopAndRemoveAll()
	ReleaseResources()
}
