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

package services

import (
	"github.com/aws/aws-advanced-go-wrapper/awssql/driver_infrastructure"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils/telemetry"
)

// FullServicesContainer holds all services needed by plugins and monitors.
// Implements driver_infrastructure.ServicesContainer interface.
// Note: Props/DSN are not stored here - they're passed directly to components that need them.
// Note: Dialect/DriverDialect are not stored here - they're managed by PluginService (matches Java pattern).
type FullServicesContainer struct {
	// Core services (shared across connections)
	Storage driver_infrastructure.StorageService
	Monitor driver_infrastructure.MonitorService
	Events  driver_infrastructure.EventPublisher

	// Connection-specific services
	Telemetry               telemetry.TelemetryFactory
	PluginManager           driver_infrastructure.PluginManager
	PluginService           driver_infrastructure.PluginService
	HostListProviderService driver_infrastructure.HostListProviderService
	ConnProvider            driver_infrastructure.ConnectionProvider
}

// Interface implementation - Getters
func (c *FullServicesContainer) GetStorageService() driver_infrastructure.StorageService {
	return c.Storage
}
func (c *FullServicesContainer) GetMonitorService() driver_infrastructure.MonitorService {
	return c.Monitor
}
func (c *FullServicesContainer) GetEventPublisher() driver_infrastructure.EventPublisher {
	return c.Events
}
func (c *FullServicesContainer) GetTelemetryFactory() telemetry.TelemetryFactory { return c.Telemetry }
func (c *FullServicesContainer) GetPluginManager() driver_infrastructure.PluginManager {
	return c.PluginManager
}
func (c *FullServicesContainer) GetPluginService() driver_infrastructure.PluginService {
	return c.PluginService
}
func (c *FullServicesContainer) GetHostListProviderService() driver_infrastructure.HostListProviderService {
	return c.HostListProviderService
}
func (c *FullServicesContainer) GetConnectionProvider() driver_infrastructure.ConnectionProvider {
	return c.ConnProvider
}

// Interface implementation - Setters
func (c *FullServicesContainer) SetPluginManager(pm driver_infrastructure.PluginManager) {
	c.PluginManager = pm
}
func (c *FullServicesContainer) SetPluginService(ps driver_infrastructure.PluginService) {
	c.PluginService = ps
}
func (c *FullServicesContainer) SetHostListProviderService(h driver_infrastructure.HostListProviderService) {
	c.HostListProviderService = h
}

// NewFullServicesContainer creates a full container with core services.
func NewFullServicesContainer(
	storage driver_infrastructure.StorageService,
	monitor driver_infrastructure.MonitorService,
	events driver_infrastructure.EventPublisher,
	connProvider driver_infrastructure.ConnectionProvider,
) *FullServicesContainer {
	return &FullServicesContainer{
		Storage:      storage,
		Monitor:      monitor,
		Events:       events,
		ConnProvider: connProvider,
	}
}

// CreateMinimal creates a lightweight container for internal use (failover, monitors).
// Shares core services but allows independent connection-specific services.
func (c *FullServicesContainer) CreateMinimal() driver_infrastructure.ServicesContainer {
	return &FullServicesContainer{
		// Share core services
		Storage: c.Storage,
		Monitor: c.Monitor,
		Events:  c.Events,

		// Copy connection-specific (can be overwritten by caller)
		Telemetry:               c.Telemetry,
		ConnProvider:            c.ConnProvider,
		HostListProviderService: c.HostListProviderService,
		// PluginManager and PluginService intentionally nil - caller sets up
	}
}

// ContainerOption is a functional option for configuring FullServicesContainer.
type ContainerOption func(*FullServicesContainer)

// WithCoreServices sets all core services (Storage, Monitor, Events) from a CoreServiceContainer.
func WithCoreServices(core *CoreServiceContainer) ContainerOption {
	return func(c *FullServicesContainer) {
		c.Storage = core.Storage
		c.Monitor = core.Monitor
		c.Events = core.Events
	}
}

// WithStorage sets the storage service.
func WithStorage(s driver_infrastructure.StorageService) ContainerOption {
	return func(c *FullServicesContainer) {
		c.Storage = s
	}
}

// WithMonitor sets the monitor service.
func WithMonitor(m driver_infrastructure.MonitorService) ContainerOption {
	return func(c *FullServicesContainer) {
		c.Monitor = m
	}
}

// WithTelemetry sets the telemetry factory.
func WithTelemetry(t telemetry.TelemetryFactory) ContainerOption {
	return func(c *FullServicesContainer) {
		c.Telemetry = t
	}
}

// WithEvents sets the event publisher.
func WithEvents(e driver_infrastructure.EventPublisher) ContainerOption {
	return func(c *FullServicesContainer) {
		c.Events = e
	}
}

// WithConnectionProvider sets the connection provider.
func WithConnectionProvider(p driver_infrastructure.ConnectionProvider) ContainerOption {
	return func(c *FullServicesContainer) {
		c.ConnProvider = p
	}
}

// WithPluginManager sets the plugin manager.
func WithPluginManager(pm driver_infrastructure.PluginManager) ContainerOption {
	return func(c *FullServicesContainer) {
		c.PluginManager = pm
	}
}

// WithPluginService sets the plugin service.
func WithPluginService(ps driver_infrastructure.PluginService) ContainerOption {
	return func(c *FullServicesContainer) {
		c.PluginService = ps
	}
}

// WithHostListProviderService sets the host list provider service.
func WithHostListProviderService(h driver_infrastructure.HostListProviderService) ContainerOption {
	return func(c *FullServicesContainer) {
		c.HostListProviderService = h
	}
}

// NewFullServicesContainerWithOptions creates a container using functional options.
func NewFullServicesContainerWithOptions(opts ...ContainerOption) *FullServicesContainer {
	c := &FullServicesContainer{}
	for _, opt := range opts {
		opt(c)
	}
	return c
}
