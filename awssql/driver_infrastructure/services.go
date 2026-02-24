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

// Event is the interface all events must implement.
type Event interface {
	// EventType returns the type identifier for this event.
	EventType() string
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
	Subscribe(subscriber EventSubscriber, eventTypes []string)
	// Unsubscribe removes a subscriber from the given event types.
	Unsubscribe(subscriber EventSubscriber, eventTypes []string)
	// Publish publishes an event to all subscribers of that event type.
	Publish(event Event)
	// Stop stops the event publisher and releases resources.
	Stop()
}

// =============================================================================
// Storage Service
// =============================================================================

// ShouldDisposeFunc determines whether an expired item should be disposed.
type ShouldDisposeFunc func(value any) bool

// ItemDisposalFunc defines how to dispose of an item when removed.
type ItemDisposalFunc func(value any)

// StorageService provides shared key-value storage for driver components.
type StorageService interface {
	// Register registers a new item type with custom TTL and disposal behavior.
	Register(typeKey string, ttl time.Duration, renewOnAccess bool, shouldDispose ShouldDisposeFunc, onDispose ItemDisposalFunc)
	// Set stores an item under the given type and key.
	Set(typeKey string, key any, value any)
	// Get retrieves an item by type and key. Returns nil if not found or expired.
	Get(typeKey string, key any) (any, bool)
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
type Monitor interface {
	Start()
	Stop()
	GetState() MonitorState
	GetLastActivityTimestamp() time.Time
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

// MonitorService manages background monitors with expiration and health checks.
type MonitorService interface {
	// RegisterMonitorType registers a new monitor type with the service.
	RegisterMonitorType(monitorType string, settings *MonitorSettings, producedDataClass string)
	// RunIfAbsent starts a monitor if it doesn't exist, or extends its expiration if it does.
	RunIfAbsent(monitorType string, key any, container ServicesContainer, initializer MonitorInitializer) (Monitor, error)
	// Get retrieves a monitor by type and key.
	Get(monitorType string, key any) Monitor
	// Remove removes a monitor without stopping it.
	Remove(monitorType string, key any) Monitor
	// StopAndRemove stops and removes a specific monitor.
	StopAndRemove(monitorType string, key any)
	// StopAndRemoveByType stops and removes all monitors of a given type.
	StopAndRemoveByType(monitorType string)
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
