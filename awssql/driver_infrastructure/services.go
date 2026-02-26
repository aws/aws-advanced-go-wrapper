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

import (
	"time"

	"github.com/aws/aws-advanced-go-wrapper/awssql/utils/telemetry"
)

// =============================================================================
// Event Interface & Subscriber
// =============================================================================

// EventType is a type descriptor for events, similar to Java's Class<T>.
// It provides type-safe event type identification.
type EventType struct {
	Name string
}

// Event is the interface all events must implement.
type Event interface {
	// EventType returns the type descriptor for this event.
	GetEventType() *EventType
	// IsImmediateDelivery returns true if the event should be delivered immediately.
	IsImmediateDelivery() bool
}

// EventSubscriber is the interface for components that want to receive events.
type EventSubscriber interface {
	ProcessEvent(event Event)
}

// EventPublisher publishes events to subscribers.
type EventPublisher interface {
	// Subscribe registers a subscriber for the given event types.
	Subscribe(subscriber EventSubscriber, eventTypes []*EventType)
	// Unsubscribe removes a subscriber from the given event types.
	Unsubscribe(subscriber EventSubscriber, eventTypes []*EventType)
	// Publish publishes an event to all subscribers of that event type.
	Publish(event Event)
	// Stop stops the event publisher and releases resources.
	Stop()
}

// =============================================================================
// Storage Service
// =============================================================================

// Note: Storage type keys are now hardcoded in their respective StorageTypeDescriptor definitions.
// See TopologyStorageType, AllowedAndBlockedHostsStorageType, etc.

// ShouldDisposeFunc determines whether an expired item should be disposed.
type ShouldDisposeFunc func(value any) bool

// ItemDisposalFunc defines how to dispose of an item when removed.
type ItemDisposalFunc func(value any)

// StorageService provides shared key-value storage for driver components.
// Access to stored data should be done through StorageTypeDescriptor for type safety.
// Raw Get/Set methods are intentionally not exposed - use descriptors instead.
type StorageService interface {
	// Register registers a new item type with custom TTL and disposal behavior.
	Register(typeKey string, ttl time.Duration, renewOnAccess bool, shouldDispose ShouldDisposeFunc, onDispose ItemDisposalFunc)
	// Exists checks if an item exists (and is not expired).
	Exists(typeKey string, key any) bool
	// Remove removes an item by type and key.
	Remove(typeKey string, key any)
	// Clear removes all items of the given type.
	Clear(typeKey string)
	// ClearAll removes all items from all caches.
	ClearAll()
	// Size returns the number of items stored under the given type.
	Size(typeKey string) int
	// Stop stops the background cleanup goroutine.
	Stop()
}

// RawStorageAccess provides raw Get/Set methods for internal use by StorageTypeDescriptor.
// This interface should not be used directly - use StorageTypeDescriptor instead.
type RawStorageAccess interface {
	// Set stores an item under the given type and key.
	Set(typeKey string, key any, value any)
	// Get retrieves an item by type and key. Returns nil if not found or expired.
	Get(typeKey string, key any) (any, bool)
}

// =============================================================================
// Monitor Service
// =============================================================================

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
// This matches Java's Monitor interface in util/monitoring/Monitor.java.
type Monitor interface {
	// Start submits this monitor in a separate thread to begin its monitoring tasks.
	Start()
	// Run executes the monitoring loop for this monitor. This method should be called
	// in the goroutine started by Start(). The monitoring loop should regularly update
	// the last activity timestamp so that the MonitorService can detect stuck monitors.
	Monitor()
	// Stop stops the monitoring tasks for this monitor and closes all resources.
	Stop()
	// Close closes all resources used by this monitor. Called as part of Stop().
	Close()
	// GetLastActivityTimestampNanos returns the timestamp for the last action performed
	// by this monitor, in nanoseconds.
	GetLastActivityTimestampNanos() int64
	// GetState returns the current state of this monitor.
	GetState() MonitorState
	// CanDispose returns true if this monitor can be disposed.
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

// MonitorType is a type descriptor for monitors, similar to Java's Class<T>.
// It provides type-safe monitor type identification.
type MonitorType struct {
	Name string
}

// MonitorService manages background monitors with expiration and health checks.
type MonitorService interface {
	// RegisterMonitorType registers a new monitor type with the service.
	RegisterMonitorType(monitorType *MonitorType, settings *MonitorSettings, producedDataType string)
	// RunIfAbsent starts a monitor if it doesn't exist, or extends its expiration if it does.
	RunIfAbsent(monitorType *MonitorType, key any, container ServicesContainer, initializer MonitorInitializer) (Monitor, error)
	// Get retrieves a monitor by type and key.
	Get(monitorType *MonitorType, key any) Monitor
	// Remove removes a monitor without stopping it.
	Remove(monitorType *MonitorType, key any) Monitor
	// StopAndRemove stops and removes a specific monitor.
	StopAndRemove(monitorType *MonitorType, key any)
	// StopAndRemoveByType stops and removes all monitors of a given type.
	StopAndRemoveByType(monitorType *MonitorType)
	// StopAndRemoveAll stops all monitors and removes them.
	StopAndRemoveAll()
	// ReleaseResources stops the cleanup loop and all monitors.
	ReleaseResources()
}

// =============================================================================
// Services Container
// =============================================================================

// ServicesContainer provides access to all services needed by plugins and monitors.
// This interface allows for easy mocking in tests.
type ServicesContainer interface {
	// Core services (shared across connections)
	GetStorageService() StorageService
	GetMonitorService() MonitorService
	GetEventPublisher() EventPublisher

	// Connection-specific services
	GetTelemetryFactory() telemetry.TelemetryFactory
	GetPluginManager() PluginManager
	GetPluginService() PluginService
	GetHostListProviderService() HostListProviderService
	GetConnectionProvider() ConnectionProvider

	// Setters for mutable fields
	SetPluginManager(PluginManager)
	SetPluginService(PluginService)
	SetHostListProviderService(HostListProviderService)

	// Factory method
	CreateMinimal() ServicesContainer
}
