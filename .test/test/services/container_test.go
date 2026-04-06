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
	"testing"

	mock_driver_infrastructure "github.com/aws/aws-advanced-go-wrapper/.test/test/mocks/awssql/driver_infrastructure"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/driver_infrastructure"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/services"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/utils/telemetry"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

func TestNewFullServicesContainer(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStorage := mock_driver_infrastructure.NewMockStorageService(ctrl)
	mockMonitor := mock_driver_infrastructure.NewMockMonitorService(ctrl)
	publisher := services.NewBatchingEventPublisher(services.DefaultMessageInterval)
	defer publisher.Stop()
	mockConnProvider := mock_driver_infrastructure.NewMockConnectionProvider(ctrl)

	container := services.NewFullServicesContainer(mockStorage, mockMonitor, publisher, mockConnProvider)

	assert.NotNil(t, container)
	assert.Equal(t, mockStorage, container.GetStorageService())
	assert.Equal(t, mockMonitor, container.GetMonitorService())
	assert.Equal(t, publisher, container.GetEventPublisher())
	assert.Equal(t, mockConnProvider, container.GetConnectionProvider())
	assert.Nil(t, container.GetPluginManager())
	assert.Nil(t, container.GetPluginService())
	assert.Nil(t, container.GetHostListProviderService())
	assert.Nil(t, container.GetTelemetryFactory())
}

func TestFullServicesContainerSetters(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	container := &services.FullServicesContainer{}

	mockPluginManager := mock_driver_infrastructure.NewMockPluginManager(ctrl)
	mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
	mockHLPService := mock_driver_infrastructure.NewMockHostListProviderService(ctrl)

	container.PluginManager = mockPluginManager
	container.PluginService = mockPluginService
	container.HostListProviderService = mockHLPService

	assert.Equal(t, mockPluginManager, container.GetPluginManager())
	assert.Equal(t, mockPluginService, container.GetPluginService())
	assert.Equal(t, mockHLPService, container.GetHostListProviderService())
}

func TestCreateMinimal(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStorage := mock_driver_infrastructure.NewMockStorageService(ctrl)
	mockMonitor := mock_driver_infrastructure.NewMockMonitorService(ctrl)
	publisher := services.NewBatchingEventPublisher(services.DefaultMessageInterval)
	defer publisher.Stop()
	mockConnProvider := mock_driver_infrastructure.NewMockConnectionProvider(ctrl)
	telemetryFactory := &telemetry.NilTelemetryFactory{}

	container := &services.FullServicesContainer{
		Storage:      mockStorage,
		Monitor:      mockMonitor,
		Events:       publisher,
		ConnProvider: mockConnProvider,
		Telemetry:    telemetryFactory,
	}

	// Set connection-specific services that should NOT be copied
	mockPluginManager := mock_driver_infrastructure.NewMockPluginManager(ctrl)
	container.PluginManager = mockPluginManager

	minimal := container.CreateMinimal()

	assert.NotNil(t, minimal)
	// Core services should be shared
	assert.Equal(t, mockStorage, minimal.GetStorageService())
	assert.Equal(t, mockMonitor, minimal.GetMonitorService())
	assert.Equal(t, publisher, minimal.GetEventPublisher())
	assert.Equal(t, mockConnProvider, minimal.GetConnectionProvider())
	assert.Equal(t, telemetryFactory, minimal.GetTelemetryFactory())
	// PluginManager and PluginService should be nil in minimal
	assert.Nil(t, minimal.GetPluginManager())
	assert.Nil(t, minimal.GetPluginService())
}

func TestNewFullServicesContainerWithOptions(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStorage := mock_driver_infrastructure.NewMockStorageService(ctrl)
	mockMonitor := mock_driver_infrastructure.NewMockMonitorService(ctrl)
	publisher := services.NewBatchingEventPublisher(services.DefaultMessageInterval)
	defer publisher.Stop()
	mockConnProvider := mock_driver_infrastructure.NewMockConnectionProvider(ctrl)
	telemetryFactory := &telemetry.NilTelemetryFactory{}
	mockPluginManager := mock_driver_infrastructure.NewMockPluginManager(ctrl)
	mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
	mockHLPService := mock_driver_infrastructure.NewMockHostListProviderService(ctrl)

	container := services.NewFullServicesContainerWithOptions(
		services.WithStorage(mockStorage),
		services.WithMonitor(mockMonitor),
		services.WithEvents(publisher),
		services.WithConnectionProvider(mockConnProvider),
		services.WithTelemetry(telemetryFactory),
		services.WithPluginManager(mockPluginManager),
		services.WithPluginService(mockPluginService),
		services.WithHostListProviderService(mockHLPService),
	)

	assert.Equal(t, mockStorage, container.GetStorageService())
	assert.Equal(t, mockMonitor, container.GetMonitorService())
	assert.Equal(t, publisher, container.GetEventPublisher())
	assert.Equal(t, mockConnProvider, container.GetConnectionProvider())
	assert.Equal(t, telemetryFactory, container.GetTelemetryFactory())
	assert.Equal(t, mockPluginManager, container.GetPluginManager())
	assert.Equal(t, mockPluginService, container.GetPluginService())
	assert.Equal(t, mockHLPService, container.GetHostListProviderService())
}

func TestWithCoreServices(t *testing.T) {
	publisher := services.NewBatchingEventPublisher(services.DefaultMessageInterval)
	defer publisher.Stop()
	storage := services.NewExpiringStorage(services.DefaultStorageCleanupInterval, publisher)
	defer storage.Stop()
	monitor := services.NewMonitorManager(services.DefaultMonitorCheckInterval, publisher)
	defer monitor.ReleaseResources()

	core := &services.CoreServiceContainer{
		Storage: storage,
		Monitor: monitor,
		Events:  publisher,
	}

	container := services.NewFullServicesContainerWithOptions(
		services.WithCoreServices(core),
	)

	assert.Equal(t, storage, container.GetStorageService())
	assert.Equal(t, monitor, container.GetMonitorService())
	assert.Equal(t, publisher, container.GetEventPublisher())
}

func TestServicesContainerImplementsInterface(t *testing.T) {
	var _ driver_infrastructure.ServicesContainer = (*services.FullServicesContainer)(nil)
}
