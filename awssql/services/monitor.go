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
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/driver_infrastructure"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/error_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/utils"
)

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
	expiresAt       atomic.Int64 // unix nano timestamp
}

type monitorInitialization struct {
	done    chan struct{}
	monitor driver_infrastructure.Monitor
	err     error
}

// cacheContainer holds a cache of monitors with related settings.
type cacheContainer struct {
	settings               *driver_infrastructure.MonitorSettings
	cache                  *utils.RWMap[any, *monitorItem]
	producedDataType       string // The type key of data produced by this monitor type
	initializationLock     sync.Mutex
	monitorInitializations map[any]*monitorInitialization
}

// getAndExtendExpiration returns the cached monitor for key, extending its
// expiration, or false if no monitor is cached.
func (c *cacheContainer) getAndExtendExpiration(key any) (driver_infrastructure.Monitor, bool) {
	item, exists := c.cache.Get(key)
	if !exists || item == nil {
		return nil, false
	}
	item.expiresAt.Store(time.Now().Add(c.settings.ExpirationTimeout).UnixNano())
	return item.monitor, true
}

// runIfAbsent returns the cached monitor for key, coalescing concurrent
// initialization so that exactly one caller invokes monitorSupplier while
// concurrent same-key callers wait for and share its result. A failed
// initialization is not cached, so a later call can retry.
func (c *cacheContainer) runIfAbsent(
	key any,
	monitorSupplier func() (driver_infrastructure.Monitor, error),
) (driver_infrastructure.Monitor, error) {
	// Cache hits stay off initializationLock so they don't serialize; some
	// callers look up their monitor on every network-bound method call.
	if monitor, exists := c.getAndExtendExpiration(key); exists {
		return monitor, nil
	}

	c.initializationLock.Lock()
	// Re-check under the lock: an initialization that completed after the
	// lock-free read would otherwise be duplicated.
	if monitor, exists := c.getAndExtendExpiration(key); exists {
		c.initializationLock.Unlock()
		return monitor, nil
	}

	if initialization, exists := c.monitorInitializations[key]; exists {
		c.initializationLock.Unlock()
		<-initialization.done
		return initialization.monitor, initialization.err
	}

	initialization := &monitorInitialization{done: make(chan struct{})}
	c.monitorInitializations[key] = initialization
	c.initializationLock.Unlock()

	return c.initializeMonitor(key, initialization, monitorSupplier)
}

// initializeMonitor invokes monitorSupplier and publishes the result to
// same-key callers waiting on initialization. Publication runs in a defer so
// that a panicking supplier still unblocks waiters with an error and clears
// the in-flight entry; otherwise every later same-key call would block
// forever. The panic itself continues to propagate to this caller.
func (c *cacheContainer) initializeMonitor(
	key any,
	initialization *monitorInitialization,
	monitorSupplier func() (driver_infrastructure.Monitor, error),
) (monitor driver_infrastructure.Monitor, err error) {
	completed := false
	defer func() {
		if !completed {
			monitor = nil
			err = error_util.NewGenericAwsWrapperError(error_util.GetMessage("MonitorManager.initializationPanicked"))
		}

		c.initializationLock.Lock()
		defer c.initializationLock.Unlock()

		initialization.monitor = monitor
		initialization.err = err
		delete(c.monitorInitializations, key)
		// done is closed via defer so waiters are unblocked even if Start panics.
		defer close(initialization.done)

		if err == nil {
			item := &monitorItem{
				monitor:         monitor,
				monitorSupplier: monitorSupplier,
			}
			item.expiresAt.Store(time.Now().Add(c.settings.ExpirationTimeout).UnixNano())

			// Cache insertion and Start stay under initializationLock so the
			// cleanup loop cannot dispose a monitor that has not started yet.
			c.cache.Put(key, item)
			monitor.Start()
		}
	}()

	monitor, err = monitorSupplier()
	completed = true
	return monitor, err
}

// MonitorManager manages background monitors with expiration and health checks.
// It maintains a map of monitor caches.
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
				item.expiresAt.Store(time.Now().Add(container.settings.ExpirationTimeout).UnixNano())
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
		settings:               settings,
		cache:                  utils.NewRWMap[any, *monitorItem](),
		producedDataType:       producedDataType,
		monitorInitializations: make(map[any]*monitorInitialization),
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

	monitorSupplier := func() (driver_infrastructure.Monitor, error) {
		return initializer(container)
	}
	return cacheContainer.runIfAbsent(key, monitorSupplier)
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

	removed := cacheContainer.cache.RemoveIf(func(_ any, _ *monitorItem) bool { return true })
	for _, entry := range removed {
		if entry.Value != nil {
			entry.Value.monitor.Stop()
		}
	}
}

// StopAndRemoveAll stops all monitors and removes them.
func (m *MonitorManager) StopAndRemoveAll() {
	m.monitorCaches.ForEach(func(_ string, container *cacheContainer) {
		removed := container.cache.RemoveIf(func(_ any, _ *monitorItem) bool { return true })
		for _, entry := range removed {
			if entry.Value != nil {
				entry.Value.monitor.Stop()
			}
		}
	})
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
		settings := container.settings

		container.initializationLock.Lock()

		// Remove and stop monitors that are stopped or expired
		stopped := container.cache.RemoveIf(func(_ any, item *monitorItem) bool {
			if item == nil {
				return false
			}
			if item.monitor.GetState() == driver_infrastructure.MonitorStateStopped {
				return true
			}
			return now.After(time.Unix(0, item.expiresAt.Load())) && item.monitor.CanDispose()
		})

		// Remove and handle monitors in error state or stuck
		errored := container.cache.RemoveIf(func(_ any, item *monitorItem) bool {
			if item == nil {
				return false
			}
			if item.monitor.GetState() == driver_infrastructure.MonitorStateError {
				return true
			}
			lastActivity := time.Unix(0, item.monitor.GetLastActivityTimestampNanos())
			return now.Sub(lastActivity) > settings.InactiveTimeout
		})

		container.initializationLock.Unlock()

		for _, entry := range stopped {
			entry.Value.monitor.Stop()
		}
		for _, entry := range errored {
			m.handleMonitorError(container, entry.Key, entry.Value)
		}
	})
}

func (m *MonitorManager) handleMonitorError(container *cacheContainer, key any, errorItem *monitorItem) {
	errorItem.monitor.Stop()

	// Check if we should recreate the monitor
	if container.settings.ErrorResponses[driver_infrastructure.MonitorErrorRecreate] {
		_, _ = container.runIfAbsent(key, errorItem.monitorSupplier)
	}
}
