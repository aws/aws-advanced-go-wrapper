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
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-advanced-go-wrapper/awssql/driver_infrastructure"
	"github.com/aws/aws-advanced-go-wrapper/awssql/services"
	"github.com/stretchr/testify/assert"
)

// mockMonitor is a simple mock for the Monitor interface used in monitor manager tests.
type mockMonitor struct {
	started       atomic.Bool
	stopped       atomic.Bool
	state         driver_infrastructure.MonitorState
	lastActivity  int64
	canDispose    bool
	monitorCalled atomic.Bool
}

func newMockMonitor() *mockMonitor {
	return &mockMonitor{
		state:        driver_infrastructure.MonitorStateRunning,
		lastActivity: time.Now().UnixNano(),
		canDispose:   false,
	}
}

func (m *mockMonitor) Start()   { m.started.Store(true) }
func (m *mockMonitor) Monitor() { m.monitorCalled.Store(true) }
func (m *mockMonitor) Stop()    { m.stopped.Store(true) }
func (m *mockMonitor) Close()   {}
func (m *mockMonitor) GetLastActivityTimestampNanos() int64 {
	return m.lastActivity
}
func (m *mockMonitor) GetState() driver_infrastructure.MonitorState {
	return m.state
}
func (m *mockMonitor) CanDispose() bool {
	return m.canDispose
}

var testMonitorType = &driver_infrastructure.MonitorType{Name: "test-monitor"}

func TestMonitorManagerRunIfAbsent(t *testing.T) {
	publisher := services.NewBatchingEventPublisher(1 * time.Hour)
	defer publisher.Stop()
	manager := services.NewMonitorManager(1*time.Hour, publisher)
	defer manager.ReleaseResources()

	mock := newMockMonitor()
	initializer := func(_ driver_infrastructure.ServicesContainer) (driver_infrastructure.Monitor, error) {
		return mock, nil
	}

	monitor, err := manager.RunIfAbsent(testMonitorType, "key1", nil, initializer)
	assert.Nil(t, err)
	assert.NotNil(t, monitor)
	assert.True(t, mock.started.Load())
}

func TestMonitorManagerRunIfAbsentReturnsExisting(t *testing.T) {
	publisher := services.NewBatchingEventPublisher(1 * time.Hour)
	defer publisher.Stop()
	manager := services.NewMonitorManager(1*time.Hour, publisher)
	defer manager.ReleaseResources()

	mock := newMockMonitor()
	callCount := 0
	initializer := func(_ driver_infrastructure.ServicesContainer) (driver_infrastructure.Monitor, error) {
		callCount++
		return mock, nil
	}

	monitor1, err := manager.RunIfAbsent(testMonitorType, "key1", nil, initializer)
	assert.Nil(t, err)

	monitor2, err := manager.RunIfAbsent(testMonitorType, "key1", nil, initializer)
	assert.Nil(t, err)

	assert.Equal(t, monitor1, monitor2)
	assert.Equal(t, 1, callCount) // initializer should only be called once
}

func TestMonitorManagerGet(t *testing.T) {
	publisher := services.NewBatchingEventPublisher(1 * time.Hour)
	defer publisher.Stop()
	manager := services.NewMonitorManager(1*time.Hour, publisher)
	defer manager.ReleaseResources()

	// Get non-existent monitor
	assert.Nil(t, manager.Get(testMonitorType, "key1"))

	mock := newMockMonitor()
	initializer := func(_ driver_infrastructure.ServicesContainer) (driver_infrastructure.Monitor, error) {
		return mock, nil
	}

	_, err := manager.RunIfAbsent(testMonitorType, "key1", nil, initializer)
	assert.Nil(t, err)

	result := manager.Get(testMonitorType, "key1")
	assert.Equal(t, mock, result)
}

func TestMonitorManagerGetNonExistentType(t *testing.T) {
	publisher := services.NewBatchingEventPublisher(1 * time.Hour)
	defer publisher.Stop()
	manager := services.NewMonitorManager(1*time.Hour, publisher)
	defer manager.ReleaseResources()

	unknownType := &driver_infrastructure.MonitorType{Name: "unknown"}
	assert.Nil(t, manager.Get(unknownType, "key1"))
}

func TestMonitorManagerRemove(t *testing.T) {
	publisher := services.NewBatchingEventPublisher(1 * time.Hour)
	defer publisher.Stop()
	manager := services.NewMonitorManager(1*time.Hour, publisher)
	defer manager.ReleaseResources()

	mock := newMockMonitor()
	initializer := func(_ driver_infrastructure.ServicesContainer) (driver_infrastructure.Monitor, error) {
		return mock, nil
	}

	_, err := manager.RunIfAbsent(testMonitorType, "key1", nil, initializer)
	assert.Nil(t, err)

	removed := manager.Remove(testMonitorType, "key1")
	assert.Equal(t, mock, removed)
	assert.Nil(t, manager.Get(testMonitorType, "key1"))
	// Remove should not stop the monitor
	assert.False(t, mock.stopped.Load())
}

func TestMonitorManagerRemoveNonExistent(t *testing.T) {
	publisher := services.NewBatchingEventPublisher(1 * time.Hour)
	defer publisher.Stop()
	manager := services.NewMonitorManager(1*time.Hour, publisher)
	defer manager.ReleaseResources()

	assert.Nil(t, manager.Remove(testMonitorType, "nonexistent"))
}

func TestMonitorManagerStopAndRemove(t *testing.T) {
	publisher := services.NewBatchingEventPublisher(1 * time.Hour)
	defer publisher.Stop()
	manager := services.NewMonitorManager(1*time.Hour, publisher)
	defer manager.ReleaseResources()

	mock := newMockMonitor()
	initializer := func(_ driver_infrastructure.ServicesContainer) (driver_infrastructure.Monitor, error) {
		return mock, nil
	}

	_, err := manager.RunIfAbsent(testMonitorType, "key1", nil, initializer)
	assert.Nil(t, err)

	manager.StopAndRemove(testMonitorType, "key1")
	assert.True(t, mock.stopped.Load())
	assert.Nil(t, manager.Get(testMonitorType, "key1"))
}

func TestMonitorManagerStopAndRemoveNonExistent(t *testing.T) {
	publisher := services.NewBatchingEventPublisher(1 * time.Hour)
	defer publisher.Stop()
	manager := services.NewMonitorManager(1*time.Hour, publisher)
	defer manager.ReleaseResources()

	// Should not panic
	manager.StopAndRemove(testMonitorType, "nonexistent")
}

func TestMonitorManagerStopAndRemoveByType(t *testing.T) {
	publisher := services.NewBatchingEventPublisher(1 * time.Hour)
	defer publisher.Stop()
	manager := services.NewMonitorManager(1*time.Hour, publisher)
	defer manager.ReleaseResources()

	mock1 := newMockMonitor()
	mock2 := newMockMonitor()
	callCount := 0
	initializer := func(_ driver_infrastructure.ServicesContainer) (driver_infrastructure.Monitor, error) {
		callCount++
		if callCount == 1 {
			return mock1, nil
		}
		return mock2, nil
	}

	_, err := manager.RunIfAbsent(testMonitorType, "key1", nil, initializer)
	assert.Nil(t, err)
	_, err = manager.RunIfAbsent(testMonitorType, "key2", nil, initializer)
	assert.Nil(t, err)

	manager.StopAndRemoveByType(testMonitorType)
	assert.True(t, mock1.stopped.Load())
	assert.True(t, mock2.stopped.Load())
	assert.Nil(t, manager.Get(testMonitorType, "key1"))
	assert.Nil(t, manager.Get(testMonitorType, "key2"))
}

func TestMonitorManagerStopAndRemoveAll(t *testing.T) {
	publisher := services.NewBatchingEventPublisher(1 * time.Hour)
	defer publisher.Stop()
	manager := services.NewMonitorManager(1*time.Hour, publisher)
	defer manager.ReleaseResources()

	type1 := &driver_infrastructure.MonitorType{Name: "type1"}
	type2 := &driver_infrastructure.MonitorType{Name: "type2"}

	mock1 := newMockMonitor()
	mock2 := newMockMonitor()

	_, err := manager.RunIfAbsent(type1, "key1", nil, func(_ driver_infrastructure.ServicesContainer) (driver_infrastructure.Monitor, error) {
		return mock1, nil
	})
	assert.Nil(t, err)

	_, err = manager.RunIfAbsent(type2, "key2", nil, func(_ driver_infrastructure.ServicesContainer) (driver_infrastructure.Monitor, error) {
		return mock2, nil
	})
	assert.Nil(t, err)

	manager.StopAndRemoveAll()
	assert.True(t, mock1.stopped.Load())
	assert.True(t, mock2.stopped.Load())
}

func TestMonitorManagerRegisterMonitorType(t *testing.T) {
	publisher := services.NewBatchingEventPublisher(1 * time.Hour)
	defer publisher.Stop()
	manager := services.NewMonitorManager(1*time.Hour, publisher)
	defer manager.ReleaseResources()

	settings := &driver_infrastructure.MonitorSettings{
		ExpirationTimeout: 10 * time.Minute,
		InactiveTimeout:   2 * time.Minute,
		ErrorResponses:    map[driver_infrastructure.MonitorErrorResponse]bool{driver_infrastructure.MonitorErrorRecreate: true},
	}

	manager.RegisterMonitorType(testMonitorType, settings, "topology")

	// Should be able to run a monitor of this type
	mock := newMockMonitor()
	monitor, err := manager.RunIfAbsent(testMonitorType, "key1", nil, func(_ driver_infrastructure.ServicesContainer) (driver_infrastructure.Monitor, error) {
		return mock, nil
	})
	assert.Nil(t, err)
	assert.NotNil(t, monitor)
}

func TestMonitorManagerProcessDataAccessEvent(t *testing.T) {
	publisher := services.NewBatchingEventPublisher(1 * time.Hour)
	defer publisher.Stop()
	manager := services.NewMonitorManager(1*time.Hour, publisher)
	defer manager.ReleaseResources()

	// Register a monitor type that produces "topology" data
	settings := &driver_infrastructure.MonitorSettings{
		ExpirationTimeout: 1 * time.Second, // short expiration
		InactiveTimeout:   3 * time.Minute,
		ErrorResponses:    map[driver_infrastructure.MonitorErrorResponse]bool{},
	}
	manager.RegisterMonitorType(testMonitorType, settings, "topology")

	mock := newMockMonitor()
	_, err := manager.RunIfAbsent(testMonitorType, "key1", nil, func(_ driver_infrastructure.ServicesContainer) (driver_infrastructure.Monitor, error) {
		return mock, nil
	})
	assert.Nil(t, err)

	// Process a data access event - should extend expiration
	manager.ProcessEvent(services.DataAccessEvent{TypeKey: "topology", Key: "key1"})

	// Monitor should still be retrievable
	assert.NotNil(t, manager.Get(testMonitorType, "key1"))
}

func TestMonitorManagerProcessMonitorStopEvent(t *testing.T) {
	publisher := services.NewBatchingEventPublisher(1 * time.Hour)
	defer publisher.Stop()
	manager := services.NewMonitorManager(1*time.Hour, publisher)
	defer manager.ReleaseResources()

	mock := newMockMonitor()
	_, err := manager.RunIfAbsent(testMonitorType, "key1", nil, func(_ driver_infrastructure.ServicesContainer) (driver_infrastructure.Monitor, error) {
		return mock, nil
	})
	assert.Nil(t, err)

	// Process a monitor stop event
	manager.ProcessEvent(services.MonitorStopEvent{MonitorType: testMonitorType, Key: "key1"})

	assert.True(t, mock.stopped.Load())
	assert.Nil(t, manager.Get(testMonitorType, "key1"))
}

func TestMonitorManagerDefaultSettings(t *testing.T) {
	settings := services.DefaultMonitorSettings()
	assert.Equal(t, services.DefaultExpirationTimeout, settings.ExpirationTimeout)
	assert.Equal(t, services.DefaultInactiveTimeout, settings.InactiveTimeout)
	assert.True(t, settings.ErrorResponses[driver_infrastructure.MonitorErrorRecreate])
}

func TestMonitorManagerImplementsInterface(t *testing.T) {
	var _ driver_infrastructure.MonitorService = (*services.MonitorManager)(nil)
	var _ driver_infrastructure.EventSubscriber = (*services.MonitorManager)(nil)
}

func TestMonitorManagerWithNilPublisher(t *testing.T) {
	manager := services.NewMonitorManager(1*time.Hour, nil)
	defer manager.ReleaseResources()

	mock := newMockMonitor()
	monitor, err := manager.RunIfAbsent(testMonitorType, "key1", nil, func(_ driver_infrastructure.ServicesContainer) (driver_infrastructure.Monitor, error) {
		return mock, nil
	})
	assert.Nil(t, err)
	assert.NotNil(t, monitor)
}

func TestMonitorManagerRunIfAbsentAutoRegisters(t *testing.T) {
	publisher := services.NewBatchingEventPublisher(1 * time.Hour)
	defer publisher.Stop()
	manager := services.NewMonitorManager(1*time.Hour, publisher)
	defer manager.ReleaseResources()

	// Don't register the type first - RunIfAbsent should auto-register with defaults
	unregisteredType := &driver_infrastructure.MonitorType{Name: "auto-registered"}
	mock := newMockMonitor()
	monitor, err := manager.RunIfAbsent(unregisteredType, "key1", nil, func(_ driver_infrastructure.ServicesContainer) (driver_infrastructure.Monitor, error) {
		return mock, nil
	})
	assert.Nil(t, err)
	assert.NotNil(t, monitor)
	assert.True(t, mock.started.Load())
}

func TestMonitorManagerCheckMonitorsStoppedState(t *testing.T) {
	publisher := services.NewBatchingEventPublisher(1 * time.Hour)
	defer publisher.Stop()
	manager := services.NewMonitorManager(50*time.Millisecond, publisher)
	defer manager.ReleaseResources()

	mock := newMockMonitor()
	mock.state = driver_infrastructure.MonitorStateStopped

	_, err := manager.RunIfAbsent(testMonitorType, "stopped-key", nil, func(_ driver_infrastructure.ServicesContainer) (driver_infrastructure.Monitor, error) {
		return mock, nil
	})
	assert.Nil(t, err)

	// Wait for cleanup loop to run
	time.Sleep(200 * time.Millisecond)

	assert.Nil(t, manager.Get(testMonitorType, "stopped-key"))
	assert.True(t, mock.stopped.Load())
}

func TestMonitorManagerCheckMonitorsErrorStateRecreate(t *testing.T) {
	publisher := services.NewBatchingEventPublisher(1 * time.Hour)
	defer publisher.Stop()
	manager := services.NewMonitorManager(50*time.Millisecond, publisher)
	defer manager.ReleaseResources()

	originalMock := newMockMonitor()
	originalMock.state = driver_infrastructure.MonitorStateError

	recreatedMock := newMockMonitor()
	callCount := 0

	_, err := manager.RunIfAbsent(testMonitorType, "error-key", nil, func(_ driver_infrastructure.ServicesContainer) (driver_infrastructure.Monitor, error) {
		callCount++
		if callCount == 1 {
			return originalMock, nil
		}
		return recreatedMock, nil
	})
	assert.Nil(t, err)

	// Wait for cleanup loop to detect the error and recreate
	time.Sleep(200 * time.Millisecond)

	assert.True(t, originalMock.stopped.Load())
	result := manager.Get(testMonitorType, "error-key")
	if result != nil {
		assert.Equal(t, recreatedMock, result)
		assert.True(t, recreatedMock.started.Load())
	}
}

func TestMonitorManagerCheckMonitorsInactiveTimeout(t *testing.T) {
	publisher := services.NewBatchingEventPublisher(1 * time.Hour)
	defer publisher.Stop()

	settings := &driver_infrastructure.MonitorSettings{
		ExpirationTimeout: 10 * time.Minute,
		InactiveTimeout:   1 * time.Millisecond,
		ErrorResponses:    map[driver_infrastructure.MonitorErrorResponse]bool{driver_infrastructure.MonitorErrorRecreate: true},
	}

	manager := services.NewMonitorManager(50*time.Millisecond, publisher)
	defer manager.ReleaseResources()
	manager.RegisterMonitorType(testMonitorType, settings, "")

	originalMock := newMockMonitor()
	originalMock.lastActivity = time.Now().Add(-1 * time.Hour).UnixNano()

	recreatedMock := newMockMonitor()
	callCount := 0

	_, err := manager.RunIfAbsent(testMonitorType, "inactive-key", nil, func(_ driver_infrastructure.ServicesContainer) (driver_infrastructure.Monitor, error) {
		callCount++
		if callCount == 1 {
			return originalMock, nil
		}
		return recreatedMock, nil
	})
	assert.Nil(t, err)

	time.Sleep(200 * time.Millisecond)

	assert.True(t, originalMock.stopped.Load())
}

func TestMonitorManagerCheckMonitorsExpiredDisposable(t *testing.T) {
	publisher := services.NewBatchingEventPublisher(1 * time.Hour)
	defer publisher.Stop()

	settings := &driver_infrastructure.MonitorSettings{
		ExpirationTimeout: 1 * time.Millisecond,
		InactiveTimeout:   10 * time.Minute,
		ErrorResponses:    map[driver_infrastructure.MonitorErrorResponse]bool{},
	}

	manager := services.NewMonitorManager(50*time.Millisecond, publisher)
	defer manager.ReleaseResources()
	manager.RegisterMonitorType(testMonitorType, settings, "")

	mock := newMockMonitor()
	mock.canDispose = true

	_, err := manager.RunIfAbsent(testMonitorType, "expired-key", nil, func(_ driver_infrastructure.ServicesContainer) (driver_infrastructure.Monitor, error) {
		return mock, nil
	})
	assert.Nil(t, err)

	time.Sleep(200 * time.Millisecond)

	assert.Nil(t, manager.Get(testMonitorType, "expired-key"))
	assert.True(t, mock.stopped.Load())
}

func TestMonitorManagerHandleMonitorErrorRecreateFailure(t *testing.T) {
	publisher := services.NewBatchingEventPublisher(1 * time.Hour)
	defer publisher.Stop()
	manager := services.NewMonitorManager(50*time.Millisecond, publisher)
	defer manager.ReleaseResources()

	originalMock := newMockMonitor()
	originalMock.state = driver_infrastructure.MonitorStateError

	callCount := 0
	_, err := manager.RunIfAbsent(testMonitorType, "fail-recreate", nil, func(_ driver_infrastructure.ServicesContainer) (driver_infrastructure.Monitor, error) {
		callCount++
		if callCount == 1 {
			return originalMock, nil
		}
		return nil, errors.New("recreation failed")
	})
	assert.Nil(t, err)

	time.Sleep(200 * time.Millisecond)

	assert.True(t, originalMock.stopped.Load())
	assert.Nil(t, manager.Get(testMonitorType, "fail-recreate"))
}

func TestMonitorManagerHandleMonitorErrorNoRecreate(t *testing.T) {
	publisher := services.NewBatchingEventPublisher(1 * time.Hour)
	defer publisher.Stop()

	// Register with no error responses (no recreate)
	settings := &driver_infrastructure.MonitorSettings{
		ExpirationTimeout: 10 * time.Minute,
		InactiveTimeout:   10 * time.Minute,
		ErrorResponses:    map[driver_infrastructure.MonitorErrorResponse]bool{},
	}

	manager := services.NewMonitorManager(50*time.Millisecond, publisher)
	defer manager.ReleaseResources()
	manager.RegisterMonitorType(testMonitorType, settings, "")

	mock := newMockMonitor()
	mock.state = driver_infrastructure.MonitorStateError

	callCount := 0
	_, err := manager.RunIfAbsent(testMonitorType, "no-recreate", nil, func(_ driver_infrastructure.ServicesContainer) (driver_infrastructure.Monitor, error) {
		callCount++
		return mock, nil
	})
	assert.Nil(t, err)

	time.Sleep(200 * time.Millisecond)

	assert.True(t, mock.stopped.Load())
	assert.Nil(t, manager.Get(testMonitorType, "no-recreate"))
	assert.Equal(t, 1, callCount) // should NOT have tried to recreate
}

func TestMonitorManagerRunIfAbsentError(t *testing.T) {
	publisher := services.NewBatchingEventPublisher(1 * time.Hour)
	defer publisher.Stop()
	manager := services.NewMonitorManager(1*time.Hour, publisher)
	defer manager.ReleaseResources()

	expectedErr := errors.New("initializer failed")
	initializer := func(_ driver_infrastructure.ServicesContainer) (driver_infrastructure.Monitor, error) {
		return nil, expectedErr
	}

	monitor, err := manager.RunIfAbsent(testMonitorType, "key1", nil, initializer)
	assert.NotNil(t, err)
	assert.Equal(t, expectedErr, err)
	assert.Nil(t, monitor)
}

func TestMonitorManagerRemoveNonExistentType(t *testing.T) {
	publisher := services.NewBatchingEventPublisher(1 * time.Hour)
	defer publisher.Stop()
	manager := services.NewMonitorManager(1*time.Hour, publisher)
	defer manager.ReleaseResources()

	unknownType := &driver_infrastructure.MonitorType{Name: "unknown"}
	assert.Nil(t, manager.Remove(unknownType, "key1"))
}

func TestMonitorManagerStopAndRemoveNonExistentType(t *testing.T) {
	publisher := services.NewBatchingEventPublisher(1 * time.Hour)
	defer publisher.Stop()
	manager := services.NewMonitorManager(1*time.Hour, publisher)
	defer manager.ReleaseResources()

	unknownType := &driver_infrastructure.MonitorType{Name: "unknown"}
	// Should not panic
	manager.StopAndRemove(unknownType, "key1")
}

func TestMonitorManagerStopAndRemoveByTypeNonExistent(t *testing.T) {
	publisher := services.NewBatchingEventPublisher(1 * time.Hour)
	defer publisher.Stop()
	manager := services.NewMonitorManager(1*time.Hour, publisher)
	defer manager.ReleaseResources()

	unknownType := &driver_infrastructure.MonitorType{Name: "unknown"}
	// Should not panic
	manager.StopAndRemoveByType(unknownType)
}

func TestMonitorManagerProcessEventWrongType(t *testing.T) {
	publisher := services.NewBatchingEventPublisher(1 * time.Hour)
	defer publisher.Stop()
	manager := services.NewMonitorManager(1*time.Hour, publisher)
	defer manager.ReleaseResources()

	// Process a DataAccessEvent with no matching monitor type - should not panic
	manager.ProcessEvent(services.DataAccessEvent{TypeKey: "nonexistent", Key: "key1"})
}

func TestMonitorManagerProcessEventNonMatchingDataType(t *testing.T) {
	publisher := services.NewBatchingEventPublisher(1 * time.Hour)
	defer publisher.Stop()
	manager := services.NewMonitorManager(1*time.Hour, publisher)
	defer manager.ReleaseResources()

	// Register a monitor type that produces "topology" data
	settings := services.DefaultMonitorSettings()
	manager.RegisterMonitorType(testMonitorType, settings, "topology")

	mock := newMockMonitor()
	_, err := manager.RunIfAbsent(testMonitorType, "key1", nil, func(_ driver_infrastructure.ServicesContainer) (driver_infrastructure.Monitor, error) {
		return mock, nil
	})
	assert.Nil(t, err)

	// Process a DataAccessEvent with a different type key - should not extend expiration
	manager.ProcessEvent(services.DataAccessEvent{TypeKey: "other-type", Key: "key1"})

	// Monitor should still be there
	assert.NotNil(t, manager.Get(testMonitorType, "key1"))
}
