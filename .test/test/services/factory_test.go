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
	"errors"
	"testing"
	"time"

	mock_driver_infrastructure "github.com/aws/aws-advanced-go-wrapper/.test/test/mocks/awssql/driver_infrastructure"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/driver_infrastructure"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/services"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/utils"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/utils/telemetry"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

type mockDriver struct{}

func (m *mockDriver) Open(_ string) (driver.Conn, error) {
	return nil, nil
}

type mockPluginChainBuilder struct {
	plugins []driver_infrastructure.ConnectionPlugin
	err     error
}

func (m *mockPluginChainBuilder) GetPlugins(
	_ driver_infrastructure.ServicesContainer,
	_ *utils.RWMap[string, string],
	_ map[string]driver_infrastructure.ConnectionPluginFactory,
) ([]driver_infrastructure.ConnectionPlugin, error) {
	return m.plugins, m.err
}

func TestNewServiceFactory(t *testing.T) {
	pmFactory := func(_ driver.Driver, _ driver_infrastructure.ServicesContainer, _ *utils.RWMap[string, string]) driver_infrastructure.PluginManager {
		return nil
	}
	psFactory := func(
		_ driver_infrastructure.ServicesContainer, _ driver_infrastructure.DriverDialect,
		_ *utils.RWMap[string, string], _ string,
	) (driver_infrastructure.PluginService, error) {
		return nil, nil
	}
	chainBuilder := &mockPluginChainBuilder{}

	factory := services.NewServiceFactory(pmFactory, psFactory, chainBuilder)
	assert.NotNil(t, factory)
}

func TestCreateMinimalContainer(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStorage := mock_driver_infrastructure.NewMockStorageService(ctrl)
	mockMonitor := mock_driver_infrastructure.NewMockMonitorService(ctrl)
	publisher := services.NewBatchingEventPublisher(1 * time.Hour)
	defer publisher.Stop()
	mockConnProvider := mock_driver_infrastructure.NewMockConnectionProvider(ctrl)
	mockPM := mock_driver_infrastructure.NewMockPluginManager(ctrl)

	pmFactory := func(_ driver.Driver, _ driver_infrastructure.ServicesContainer, _ *utils.RWMap[string, string]) driver_infrastructure.PluginManager {
		return mockPM
	}
	psFactory := func(
		_ driver_infrastructure.ServicesContainer, _ driver_infrastructure.DriverDialect,
		_ *utils.RWMap[string, string], _ string,
	) (driver_infrastructure.PluginService, error) {
		return nil, nil
	}
	chainBuilder := &mockPluginChainBuilder{}

	factory := services.NewServiceFactory(pmFactory, psFactory, chainBuilder)

	cfg := &services.MinimalContainerConfig{
		Storage:          mockStorage,
		Monitor:          mockMonitor,
		Events:           publisher,
		ConnProvider:     mockConnProvider,
		TelemetryFactory: &telemetry.NilTelemetryFactory{},
		Props:            utils.NewRWMap[string, string](),
	}

	container, err := factory.CreateMinimalContainer(&mockDriver{}, cfg)
	assert.Nil(t, err)
	assert.NotNil(t, container)
	assert.Equal(t, mockStorage, container.GetStorageService())
	assert.Equal(t, mockMonitor, container.GetMonitorService())
	assert.Equal(t, publisher, container.GetEventPublisher())
	assert.Equal(t, mockConnProvider, container.GetConnectionProvider())
	assert.Equal(t, mockPM, container.GetPluginManager())
}

func TestCreateStandardContainerPluginServiceError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStorage := mock_driver_infrastructure.NewMockStorageService(ctrl)
	mockMonitor := mock_driver_infrastructure.NewMockMonitorService(ctrl)
	publisher := services.NewBatchingEventPublisher(1 * time.Hour)
	defer publisher.Stop()
	mockConnProvider := mock_driver_infrastructure.NewMockConnectionProvider(ctrl)
	mockPM := mock_driver_infrastructure.NewMockPluginManager(ctrl)

	expectedErr := errors.New("plugin service creation failed")

	pmFactory := func(_ driver.Driver, _ driver_infrastructure.ServicesContainer, _ *utils.RWMap[string, string]) driver_infrastructure.PluginManager {
		return mockPM
	}
	psFactory := func(
		_ driver_infrastructure.ServicesContainer, _ driver_infrastructure.DriverDialect,
		_ *utils.RWMap[string, string], _ string,
	) (driver_infrastructure.PluginService, error) {
		return nil, expectedErr
	}
	chainBuilder := &mockPluginChainBuilder{}

	factory := services.NewServiceFactory(pmFactory, psFactory, chainBuilder)

	cfg := &services.StandardContainerConfig{
		Storage:             mockStorage,
		Monitor:             mockMonitor,
		Events:              publisher,
		DefaultConnProvider: mockConnProvider,
		TelemetryFactory:    &telemetry.NilTelemetryFactory{},
		Props:               utils.NewRWMap[string, string](),
		PluginFactoryByCode: map[string]driver_infrastructure.ConnectionPluginFactory{},
	}

	container, err := factory.CreateStandardContainer(&mockDriver{}, cfg)
	assert.NotNil(t, err)
	assert.Nil(t, container)
	assert.Equal(t, expectedErr, err)
}

func TestCreateStandardContainerPluginChainError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStorage := mock_driver_infrastructure.NewMockStorageService(ctrl)
	mockMonitor := mock_driver_infrastructure.NewMockMonitorService(ctrl)
	publisher := services.NewBatchingEventPublisher(1 * time.Hour)
	defer publisher.Stop()
	mockConnProvider := mock_driver_infrastructure.NewMockConnectionProvider(ctrl)
	mockPM := mock_driver_infrastructure.NewMockPluginManager(ctrl)
	mockPS := mock_driver_infrastructure.NewMockPluginService(ctrl)

	expectedErr := errors.New("chain build failed")

	pmFactory := func(_ driver.Driver, _ driver_infrastructure.ServicesContainer, _ *utils.RWMap[string, string]) driver_infrastructure.PluginManager {
		return mockPM
	}
	psFactory := func(
		_ driver_infrastructure.ServicesContainer, _ driver_infrastructure.DriverDialect,
		_ *utils.RWMap[string, string], _ string,
	) (driver_infrastructure.PluginService, error) {
		return mockPS, nil
	}
	chainBuilder := &mockPluginChainBuilder{err: expectedErr}

	factory := services.NewServiceFactory(pmFactory, psFactory, chainBuilder)

	cfg := &services.StandardContainerConfig{
		Storage:             mockStorage,
		Monitor:             mockMonitor,
		Events:              publisher,
		DefaultConnProvider: mockConnProvider,
		TelemetryFactory:    &telemetry.NilTelemetryFactory{},
		Props:               utils.NewRWMap[string, string](),
		PluginFactoryByCode: map[string]driver_infrastructure.ConnectionPluginFactory{},
	}

	container, err := factory.CreateStandardContainer(&mockDriver{}, cfg)
	assert.NotNil(t, err)
	assert.Nil(t, container)
	assert.Equal(t, expectedErr, err)
}

func TestStandardContainerConfigFields(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStorage := mock_driver_infrastructure.NewMockStorageService(ctrl)
	mockConnProvider := mock_driver_infrastructure.NewMockConnectionProvider(ctrl)
	props := utils.NewRWMap[string, string]()
	props.Put("key", "value")

	cfg := &services.StandardContainerConfig{
		Storage:               mockStorage,
		DefaultConnProvider:   mockConnProvider,
		EffectiveConnProvider: mockConnProvider,
		OriginalURL:           "postgres://localhost:5432/test",
		TargetProtocol:        "postgresql",
		Props:                 props,
		PluginFactoryByCode:   map[string]driver_infrastructure.ConnectionPluginFactory{},
	}

	assert.Equal(t, mockStorage, cfg.Storage)
	assert.Equal(t, "postgres://localhost:5432/test", cfg.OriginalURL)
	assert.Equal(t, "postgresql", cfg.TargetProtocol)
	assert.NotNil(t, cfg.Props)
	val, ok := cfg.Props.Get("key")
	assert.True(t, ok)
	assert.Equal(t, "value", val)
}

func TestMinimalContainerConfigFields(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStorage := mock_driver_infrastructure.NewMockStorageService(ctrl)
	mockConnProvider := mock_driver_infrastructure.NewMockConnectionProvider(ctrl)
	props := utils.NewRWMap[string, string]()

	cfg := &services.MinimalContainerConfig{
		Storage:      mockStorage,
		ConnProvider: mockConnProvider,
		OriginalURL:  "postgres://localhost:5432/test",
		Props:        props,
	}

	assert.Equal(t, mockStorage, cfg.Storage)
	assert.Equal(t, mockConnProvider, cfg.ConnProvider)
	assert.Equal(t, "postgres://localhost:5432/test", cfg.OriginalURL)
}

func TestCreateMinimalContainerFrom(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStorage := mock_driver_infrastructure.NewMockStorageService(ctrl)
	mockMonitor := mock_driver_infrastructure.NewMockMonitorService(ctrl)
	publisher := services.NewBatchingEventPublisher(1 * time.Hour)
	defer publisher.Stop()
	mockConnProvider := mock_driver_infrastructure.NewMockConnectionProvider(ctrl)
	mockPM := mock_driver_infrastructure.NewMockPluginManager(ctrl)

	pmFactory := func(_ driver.Driver, _ driver_infrastructure.ServicesContainer, _ *utils.RWMap[string, string]) driver_infrastructure.PluginManager {
		return mockPM
	}
	psFactory := func(
		_ driver_infrastructure.ServicesContainer, _ driver_infrastructure.DriverDialect,
		_ *utils.RWMap[string, string], _ string,
	) (driver_infrastructure.PluginService, error) {
		return nil, nil
	}
	chainBuilder := &mockPluginChainBuilder{}

	factory := services.NewServiceFactory(pmFactory, psFactory, chainBuilder)

	source := &services.FullServicesContainer{
		Storage:      mockStorage,
		Monitor:      mockMonitor,
		Events:       publisher,
		ConnProvider: mockConnProvider,
		Telemetry:    &telemetry.NilTelemetryFactory{},
	}

	props := utils.NewRWMap[string, string]()
	container, err := factory.CreateMinimalContainerFrom(&mockDriver{}, source, props)
	assert.Nil(t, err)
	assert.NotNil(t, container)
	assert.Equal(t, mockStorage, container.GetStorageService())
	assert.Equal(t, mockMonitor, container.GetMonitorService())
	assert.Equal(t, publisher, container.GetEventPublisher())
	assert.Equal(t, mockConnProvider, container.GetConnectionProvider())
	assert.Equal(t, mockPM, container.GetPluginManager())
}

func TestCreateMinimalContainerFromWithPluginService(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStorage := mock_driver_infrastructure.NewMockStorageService(ctrl)
	mockMonitor := mock_driver_infrastructure.NewMockMonitorService(ctrl)
	publisher := services.NewBatchingEventPublisher(1 * time.Hour)
	defer publisher.Stop()
	mockConnProvider := mock_driver_infrastructure.NewMockConnectionProvider(ctrl)
	mockPM := mock_driver_infrastructure.NewMockPluginManager(ctrl)

	pmFactory := func(_ driver.Driver, _ driver_infrastructure.ServicesContainer, _ *utils.RWMap[string, string]) driver_infrastructure.PluginManager {
		return mockPM
	}
	psFactory := func(
		_ driver_infrastructure.ServicesContainer, _ driver_infrastructure.DriverDialect,
		_ *utils.RWMap[string, string], _ string,
	) (driver_infrastructure.PluginService, error) {
		return nil, nil
	}
	chainBuilder := &mockPluginChainBuilder{}

	factory := services.NewServiceFactory(pmFactory, psFactory, chainBuilder)

	// Source with a PluginService that implements the optional interfaces
	mockPS := mock_driver_infrastructure.NewMockPluginService(ctrl)
	mockPS.EXPECT().GetTargetDriverDialect().Return(nil).AnyTimes()
	source := &services.FullServicesContainer{
		Storage:      mockStorage,
		Monitor:      mockMonitor,
		Events:       publisher,
		ConnProvider: mockConnProvider,
		Telemetry:    &telemetry.NilTelemetryFactory{},
	}
	source.PluginService = mockPS

	props := utils.NewRWMap[string, string]()
	container, err := factory.CreateMinimalContainerFrom(&mockDriver{}, source, props)
	assert.Nil(t, err)
	assert.NotNil(t, container)
}
