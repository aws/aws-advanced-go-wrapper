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
	"time"

	"github.com/aws/aws-advanced-go-wrapper/awssql/driver_infrastructure"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils"
)

// Default timeouts matching Java implementation.
const (
	DefaultMonitorCleanupInterval = 1 * time.Minute
	DefaultExpirationTimeout      = 15 * time.Minute
	DefaultInactiveTimeout        = 3 * time.Minute
)

// DefaultMonitorSettings returns the default settings for monitors.
func DefaultMonitorSettings() *driver_infrastructure.MonitorSettings {
	return &driver_infrastructure.MonitorSettings{
		ExpirationTimeout: DefaultExpirationTimeout,
		InactiveTimeout:   DefaultInactiveTimeout,
		ErrorResponses:    map[driver_infrastructure.MonitorErrorResponse]bool{driver_infrastructure.MonitorErrorRecreate: true},
	}
}

// monitorItem holds a monitor with its supplier for recreation.
type monitorItem struct {
	monitor         driver_infrastructure.Monitor
	monitorSupplier func() (driver_infrastructure.Monitor, error)
	expiresAt       time.Time
}

// cacheContainer holds a cache of monitors with related settings.
type cacheContainer struct {
	settings         *driver_infrastructure.MonitorSettings
	cache            *utils.RWMap[any, *monitorItem]
	producedDataType string // The type key of data produced by this monitor type
}

// MonitorManager manages background monitors with expiration and health checks.
// It maintains a map of monitor caches, similar to Java's MonitorServiceImpl.
// Implements driver_infrastructure.MonitorService.
type MonitorManager struct {
	publisher       driver_infrastructure.EventPublisher
	monitorCaches   *utils.RWMap[string, *cacheContainer] // monitorType.Name -> cacheContainer
	cleanupInterval time.Duration
	stopCh          chan struct{}
}

// NewMonitorManager creates a new monitor manager.
func NewMonitorManager(cleanupInterval time.Duration, publisher driver_infrastructure.EventPublisher) *MonitorManager {
	m := &MonitorManager{
		publisher:       publisher,
		monitorCaches:   utils.NewRWMap[string, *cacheContainer](),
		cleanupInterval: cleanupInterval,
		stopCh:          make(chan struct{}),
	}

	// Subscribe to data access events to extend monitor expiration
	if publisher != nil {
		publisher.Subscribe(m, []*driver_infrastructure.EventType{DataAccessEventType, MonitorStopEventType})
	}

	go m.cleanupLoop()
	return m
}

// ProcessEvent handles events from the EventPublisher.
func (m *MonitorManager) ProcessEvent(event driver_infrastructure.Event) {
	switch event.GetEventType() {
	case DataAccessEventType:
		accessEvent, ok := event.(DataAccessEvent)
		if !ok {
			return
		}

		// Extend expiration for monitors that produce this data type
		m.monitorCaches.ForEach(func(_ string, container *cacheContainer) {
			if container.producedDataType == "" || container.producedDataType != accessEvent.TypeKey {
				return
			}
			// Extend expiration for the monitor with this key
			item, ok := container.cache.Get(accessEvent.Key)
			if ok && item != nil {
				item.expiresAt = time.Now().Add(container.settings.ExpirationTimeout)
			}
		})

	case MonitorStopEventType:
		stopEvent, ok := event.(MonitorStopEvent)
		if !ok {
			return
		}
		m.StopAndRemove(stopEvent.MonitorType, stopEvent.Key)
	}
}

// RegisterMonitorType registers a new monitor type with the service.
func (m *MonitorManager) RegisterMonitorType(
	monitorType *driver_infrastructure.MonitorType,
	settings *driver_infrastructure.MonitorSettings,
	producedDataType string,
) {
	m.monitorCaches.PutIfAbsent(monitorType.Name, &cacheContainer{
		settings:         settings,
		cache:            utils.NewRWMap[any, *monitorItem](),
		producedDataType: producedDataType,
	})
}

// RunIfAbsent starts a monitor if it doesn't exist, or extends its expiration if it does.
func (m *MonitorManager) RunIfAbsent(
	monitorType *driver_infrastructure.MonitorType,
	key any,
	container driver_infrastructure.ServicesContainer,
	initializer driver_infrastructure.MonitorInitializer,
) (driver_infrastructure.Monitor, error) {
	cacheContainer, ok := m.monitorCaches.Get(monitorType.Name)
	if !ok {
		// Register with default settings if not registered
		m.RegisterMonitorType(monitorType, DefaultMonitorSettings(), "")
		cacheContainer, _ = m.monitorCaches.Get(monitorType.Name)
	}

	// Check if monitor already exists
	existingItem, exists := cacheContainer.cache.Get(key)
	if exists && existingItem != nil {
		// Extend expiration
		existingItem.expiresAt = time.Now().Add(cacheContainer.settings.ExpirationTimeout)
		return existingItem.monitor, nil
	}

	// Create new monitor
	monitorSupplier := func() (driver_infrastructure.Monitor, error) {
		return initializer(container)
	}

	monitor, err := monitorSupplier()
	if err != nil {
		return nil, err
	}

	item := &monitorItem{
		monitor:         monitor,
		monitorSupplier: monitorSupplier,
		expiresAt:       time.Now().Add(cacheContainer.settings.ExpirationTimeout),
	}

	cacheContainer.cache.PutIfAbsent(key, item)
	monitor.Start()

	return monitor, nil
}

// Get retrieves a monitor by type and key.
func (m *MonitorManager) Get(monitorType *driver_infrastructure.MonitorType, key any) driver_infrastructure.Monitor {
	cacheContainer, ok := m.monitorCaches.Get(monitorType.Name)
	if !ok {
		return nil
	}

	item, ok := cacheContainer.cache.Get(key)
	if !ok || item == nil {
		return nil
	}

	return item.monitor
}

// Remove removes a monitor without stopping it.
func (m *MonitorManager) Remove(monitorType *driver_infrastructure.MonitorType, key any) driver_infrastructure.Monitor {
	cacheContainer, ok := m.monitorCaches.Get(monitorType.Name)
	if !ok {
		return nil
	}

	item, ok := cacheContainer.cache.Get(key)
	if !ok || item == nil {
		return nil
	}

	cacheContainer.cache.Remove(key)
	return item.monitor
}

// StopAndRemove stops and removes a specific monitor.
func (m *MonitorManager) StopAndRemove(monitorType *driver_infrastructure.MonitorType, key any) {
	cacheContainer, ok := m.monitorCaches.Get(monitorType.Name)
	if !ok {
		return
	}

	item, ok := cacheContainer.cache.Get(key)
	if ok && item != nil {
		cacheContainer.cache.Remove(key)
		item.monitor.Stop()
	}
}

// StopAndRemoveByType stops and removes all monitors of a given type.
func (m *MonitorManager) StopAndRemoveByType(monitorType *driver_infrastructure.MonitorType) {
	cacheContainer, ok := m.monitorCaches.Get(monitorType.Name)
	if !ok {
		return
	}

	// Collect all keys and items first to avoid deadlock (ForEach holds read lock, Remove needs write lock)
	var keysToRemove []any
	var itemsToStop []*monitorItem
	cacheContainer.cache.ForEach(func(key any, item *monitorItem) {
		keysToRemove = append(keysToRemove, key)
		itemsToStop = append(itemsToStop, item)
	})

	// Now remove and stop outside the ForEach
	for i, key := range keysToRemove {
		cacheContainer.cache.Remove(key)
		if itemsToStop[i] != nil {
			itemsToStop[i].monitor.Stop()
		}
	}
}

// StopAndRemoveAll stops all monitors and removes them.
func (m *MonitorManager) StopAndRemoveAll() {
	// Collect all containers first
	var containers []*cacheContainer
	m.monitorCaches.ForEach(func(_ string, container *cacheContainer) {
		containers = append(containers, container)
	})

	// Process each container
	for _, container := range containers {
		// Collect keys and items
		var keysToRemove []any
		var itemsToStop []*monitorItem
		container.cache.ForEach(func(key any, item *monitorItem) {
			keysToRemove = append(keysToRemove, key)
			itemsToStop = append(itemsToStop, item)
		})

		// Remove and stop outside ForEach
		for i, key := range keysToRemove {
			container.cache.Remove(key)
			if itemsToStop[i] != nil {
				itemsToStop[i].monitor.Stop()
			}
		}
	}
}

// ReleaseResources stops the cleanup loop and all monitors.
func (m *MonitorManager) ReleaseResources() {
	close(m.stopCh)
	m.StopAndRemoveAll()
}

func (m *MonitorManager) cleanupLoop() {
	ticker := time.NewTicker(m.cleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-m.stopCh:
			return
		case <-ticker.C:
			m.checkMonitors()
		}
	}
}

func (m *MonitorManager) checkMonitors() {
	now := time.Now()

	m.monitorCaches.ForEach(func(_ string, container *cacheContainer) {
		container.cache.ForEach(func(key any, item *monitorItem) {
			if item == nil {
				return
			}

			monitor := item.monitor
			settings := container.settings

			// Check if monitor is stopped
			if monitor.GetState() == driver_infrastructure.MonitorStateStopped {
				container.cache.Remove(key)
				monitor.Stop()
				return
			}

			// Check if monitor is in error state
			if monitor.GetState() == driver_infrastructure.MonitorStateError {
				container.cache.Remove(key)
				m.handleMonitorError(container, key, item)
				return
			}

			// Check if monitor is stuck (inactive for too long)
			lastActivity := time.Unix(0, monitor.GetLastActivityTimestampNanos())
			if now.Sub(lastActivity) > settings.InactiveTimeout {
				container.cache.Remove(key)
				m.handleMonitorError(container, key, item)
				return
			}

			// Check if monitor is expired and can be disposed
			if now.After(item.expiresAt) && monitor.CanDispose() {
				container.cache.Remove(key)
				monitor.Stop()
				return
			}
		})
	})
}

func (m *MonitorManager) handleMonitorError(container *cacheContainer, key any, errorItem *monitorItem) {
	errorItem.monitor.Stop()

	// Check if we should recreate the monitor
	if container.settings.ErrorResponses[driver_infrastructure.MonitorErrorRecreate] {
		// Try to recreate the monitor
		newMonitor, err := errorItem.monitorSupplier()
		if err != nil {
			return
		}

		newItem := &monitorItem{
			monitor:         newMonitor,
			monitorSupplier: errorItem.monitorSupplier,
			expiresAt:       time.Now().Add(container.settings.ExpirationTimeout),
		}

		container.cache.PutIfAbsent(key, newItem)
		newMonitor.Start()
	}
}
