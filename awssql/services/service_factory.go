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
	"database/sql/driver"

	"github.com/aws/aws-advanced-go-wrapper/awssql/driver_infrastructure"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils/telemetry"
)

// StandardContainerConfig holds configuration for creating a standard service container.
type StandardContainerConfig struct {
	// Core services (shared)
	Storage driver_infrastructure.StorageService
	Monitor driver_infrastructure.MonitorService
	Events  driver_infrastructure.EventPublisher

	// Connection providers
	DefaultConnProvider   driver_infrastructure.ConnectionProvider
	EffectiveConnProvider driver_infrastructure.ConnectionProvider

	// Telemetry
	TelemetryFactory telemetry.TelemetryFactory

	// Connection info
	OriginalURL    string
	TargetProtocol string
	DriverDialect  driver_infrastructure.DriverDialect
	Props          *utils.RWMap[string, string]

	// Plugin configuration
	PluginFactoryByCode map[string]driver_infrastructure.ConnectionPluginFactory
}

// MinimalContainerConfig holds configuration for creating a minimal service container.
type MinimalContainerConfig struct {
	// Core services (shared)
	Storage driver_infrastructure.StorageService
	Monitor driver_infrastructure.MonitorService
	Events  driver_infrastructure.EventPublisher

	// Connection provider
	ConnProvider driver_infrastructure.ConnectionProvider

	// Telemetry
	TelemetryFactory telemetry.TelemetryFactory

	// Connection info
	OriginalURL    string
	TargetProtocol string
	DriverDialect  driver_infrastructure.DriverDialect
	Props          *utils.RWMap[string, string]
}

// PluginManagerFactory creates a plugin manager.
type PluginManagerFactory func(
	underlyingDriver driver.Driver,
	container driver_infrastructure.ServicesContainer,
	props *utils.RWMap[string, string],
) driver_infrastructure.PluginManager

// PluginServiceFactory creates a plugin service.
type PluginServiceFactory func(
	container driver_infrastructure.ServicesContainer,
	driverDialect driver_infrastructure.DriverDialect,
	props *utils.RWMap[string, string],
	dsn string,
) (driver_infrastructure.PluginService, error)

// PluginChainBuilder builds the plugin chain from factories.
type PluginChainBuilder interface {
	GetPlugins(
		container driver_infrastructure.ServicesContainer,
		props *utils.RWMap[string, string],
		pluginFactoryByCode map[string]driver_infrastructure.ConnectionPluginFactory,
	) ([]driver_infrastructure.ConnectionPlugin, error)
}

// ServiceFactory creates service containers with all necessary wiring.
// This is the Go equivalent of Java's ServiceUtility.
type ServiceFactory struct {
	pluginManagerFactory PluginManagerFactory
	pluginServiceFactory PluginServiceFactory
	pluginChainBuilder   PluginChainBuilder
}

// NewServiceFactory creates a new service factory with the given component factories.
func NewServiceFactory(
	pmFactory PluginManagerFactory,
	psFactory PluginServiceFactory,
	chainBuilder PluginChainBuilder,
) *ServiceFactory {
	return &ServiceFactory{
		pluginManagerFactory: pmFactory,
		pluginServiceFactory: psFactory,
		pluginChainBuilder:   chainBuilder,
	}
}

// CreateStandardContainer creates a fully-wired service container for normal connections.
// This is equivalent to Java's ServiceUtility.createStandardServiceContainer().
func (f *ServiceFactory) CreateStandardContainer(
	underlyingDriver driver.Driver,
	cfg *StandardContainerConfig,
) (*FullServicesContainer, error) {
	// Create the container with core services
	container := &FullServicesContainer{
		Storage:      cfg.Storage,
		Monitor:      cfg.Monitor,
		Events:       cfg.Events,
		Telemetry:    cfg.TelemetryFactory,
		ConnProvider: cfg.DefaultConnProvider,
	}

	// Create plugin manager and set it on the container
	pluginManager := f.pluginManagerFactory(underlyingDriver, container, cfg.Props)
	container.SetPluginManager(pluginManager)

	// Create plugin service and set it on the container
	pluginService, err := f.pluginServiceFactory(container, cfg.DriverDialect, cfg.Props, cfg.OriginalURL)
	if err != nil {
		return nil, err
	}
	container.SetPluginService(pluginService)

	// Build plugin chain
	plugins, err := f.pluginChainBuilder.GetPlugins(
		container,
		cfg.Props,
		cfg.PluginFactoryByCode,
	)
	if err != nil {
		return nil, err
	}

	// Initialize plugin manager with plugins
	if initializer, ok := pluginManager.(interface {
		Init([]driver_infrastructure.ConnectionPlugin) error
	}); ok {
		if err := initializer.Init(plugins); err != nil {
			return nil, err
		}
	}

	// Set up host list provider service
	if hlpService, ok := pluginService.(driver_infrastructure.HostListProviderService); ok {
		container.SetHostListProviderService(hlpService)

		// Initialize host provider through the plugin chain
		if err := pluginManager.InitHostProvider(cfg.Props, hlpService); err != nil {
			return nil, err
		}
	}

	supplier := pluginService.GetDialect().GetHostListProviderSupplier()
	if supplier != nil {
		provider := supplier(cfg.Props, cfg.OriginalURL, container)
		pluginService.SetHostListProvider(provider)
	}

	refreshErr := pluginService.RefreshHostList(nil)
	if refreshErr != nil {
		return nil, refreshErr
	}
	return container, nil
}

// CreateMinimalContainer creates a lightweight service container for internal use.
// This is equivalent to Java's ServiceUtility.createMinimalServiceContainer().
func (f *ServiceFactory) CreateMinimalContainer(
	underlyingDriver driver.Driver,
	cfg *MinimalContainerConfig,
) (*FullServicesContainer, error) {
	// Create the container with core services
	container := &FullServicesContainer{
		Storage:      cfg.Storage,
		Monitor:      cfg.Monitor,
		Events:       cfg.Events,
		Telemetry:    cfg.TelemetryFactory,
		ConnProvider: cfg.ConnProvider,
	}

	// Create plugin manager and set it on the container
	pluginManager := f.pluginManagerFactory(underlyingDriver, container, cfg.Props)
	container.SetPluginManager(pluginManager)

	// For minimal containers, we create a partial plugin service
	// The caller is responsible for setting up the plugin service if needed

	return container, nil
}

// CreateMinimalContainerFrom creates a minimal container from an existing container.
// This is equivalent to Java's ServiceUtility.createMinimalServiceContainer(FullServicesContainer, Properties).
func (f *ServiceFactory) CreateMinimalContainerFrom(
	underlyingDriver driver.Driver,
	source *FullServicesContainer,
	props *utils.RWMap[string, string],
) (*FullServicesContainer, error) {
	// Extract info from source container's plugin service
	var originalURL, targetProtocol string
	var driverDialect driver_infrastructure.DriverDialect

	if source.PluginService != nil {
		if urlGetter, ok := source.PluginService.(interface{ GetOriginalURL() string }); ok {
			originalURL = urlGetter.GetOriginalURL()
		}
		if protoGetter, ok := source.PluginService.(interface{ GetDriverProtocol() string }); ok {
			targetProtocol = protoGetter.GetDriverProtocol()
		}
		if dialectGetter, ok := source.PluginService.(interface {
			GetTargetDriverDialect() driver_infrastructure.DriverDialect
		}); ok {
			driverDialect = dialectGetter.GetTargetDriverDialect()
		}
	}

	cfg := &MinimalContainerConfig{
		Storage:          source.Storage,
		Monitor:          source.Monitor,
		Events:           source.Events,
		ConnProvider:     source.ConnProvider,
		TelemetryFactory: source.Telemetry,
		OriginalURL:      originalURL,
		TargetProtocol:   targetProtocol,
		DriverDialect:    driverDialect,
		Props:            props,
	}

	return f.CreateMinimalContainer(underlyingDriver, cfg)
}

// NewStandardContainerWithDefaults creates a standard container using the global singleton core services.
// This is a convenience function for the common case.
func NewStandardContainerWithDefaults(
	factory *ServiceFactory,
	underlyingDriver driver.Driver,
	defaultConnProvider driver_infrastructure.ConnectionProvider,
	effectiveConnProvider driver_infrastructure.ConnectionProvider,
	telemetryFactory telemetry.TelemetryFactory,
	originalURL string,
	targetProtocol string,
	driverDialect driver_infrastructure.DriverDialect,
	props *utils.RWMap[string, string],
	pluginFactoryByCode map[string]driver_infrastructure.ConnectionPluginFactory,
) (*FullServicesContainer, error) {
	core := GetCoreServiceContainer()
	cfg := &StandardContainerConfig{
		Storage:               core.Storage,
		Monitor:               core.Monitor,
		Events:                core.Events,
		DefaultConnProvider:   defaultConnProvider,
		EffectiveConnProvider: effectiveConnProvider,
		TelemetryFactory:      telemetryFactory,
		OriginalURL:           originalURL,
		TargetProtocol:        targetProtocol,
		DriverDialect:         driverDialect,
		Props:                 props,
		PluginFactoryByCode:   pluginFactoryByCode,
	}
	return factory.CreateStandardContainer(underlyingDriver, cfg)
}
