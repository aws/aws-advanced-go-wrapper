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
	"testing"
	"time"

	"github.com/aws/aws-advanced-go-wrapper/awssql/driver_infrastructure"
	"github.com/aws/aws-advanced-go-wrapper/awssql/services"
	"github.com/stretchr/testify/assert"
)

// testSubscriber is a simple subscriber that records received events.
type testSubscriber struct {
	mu     sync.Mutex
	events []driver_infrastructure.Event
}

func (s *testSubscriber) ProcessEvent(event driver_infrastructure.Event) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, event)
}

func (s *testSubscriber) getEvents() []driver_infrastructure.Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make([]driver_infrastructure.Event, len(s.events))
	copy(result, s.events)
	return result
}

func TestDataAccessEventProperties(t *testing.T) {
	event := services.DataAccessEvent{TypeKey: "topology", Key: "cluster-1"}
	assert.Equal(t, services.DataAccessEventType, event.GetEventType())
	assert.False(t, event.IsImmediateDelivery())
}

func TestMonitorStopEventProperties(t *testing.T) {
	monitorType := &driver_infrastructure.MonitorType{Name: "test-monitor"}
	event := services.MonitorStopEvent{MonitorType: monitorType, Key: "key-1"}
	assert.Equal(t, services.MonitorStopEventType, event.GetEventType())
	assert.True(t, event.IsImmediateDelivery())
}

func TestMonitorResetEventProperties(t *testing.T) {
	endpoints := map[string]struct{}{"endpoint-1": {}, "endpoint-2": {}}
	event := services.MonitorResetEvent{ClusterId: "cluster-1", Endpoints: endpoints}
	assert.Equal(t, driver_infrastructure.MonitorResetEventType, event.GetEventType())
	assert.True(t, event.IsImmediateDelivery())
	assert.Equal(t, "cluster-1", event.GetClusterId())
	assert.Equal(t, endpoints, event.GetEndpoints())
}

func TestPublishImmediateEvent(t *testing.T) {
	publisher := services.NewBatchingEventPublisher(1 * time.Hour) // long interval so batching doesn't fire
	defer publisher.Stop()

	sub := &testSubscriber{}
	publisher.Subscribe(sub, []*driver_infrastructure.EventType{services.MonitorStopEventType})

	monitorType := &driver_infrastructure.MonitorType{Name: "test"}
	event := services.MonitorStopEvent{MonitorType: monitorType, Key: "key-1"}
	publisher.Publish(event)

	// Immediate events should be delivered synchronously
	events := sub.getEvents()
	assert.Len(t, events, 1)
	assert.Equal(t, services.MonitorStopEventType, events[0].GetEventType())
}

func TestPublishBatchedEvent(t *testing.T) {
	publisher := services.NewBatchingEventPublisher(50 * time.Millisecond)
	defer publisher.Stop()

	sub := &testSubscriber{}
	publisher.Subscribe(sub, []*driver_infrastructure.EventType{services.DataAccessEventType})

	event := services.DataAccessEvent{TypeKey: "topology", Key: "cluster-1"}
	publisher.Publish(event)

	// Should not be delivered immediately
	events := sub.getEvents()
	assert.Len(t, events, 0)

	// Wait for the batching interval to fire
	time.Sleep(150 * time.Millisecond)

	events = sub.getEvents()
	assert.Len(t, events, 1)
	assert.Equal(t, services.DataAccessEventType, events[0].GetEventType())
}

func TestUnsubscribe(t *testing.T) {
	publisher := services.NewBatchingEventPublisher(1 * time.Hour)
	defer publisher.Stop()

	sub := &testSubscriber{}
	publisher.Subscribe(sub, []*driver_infrastructure.EventType{services.MonitorStopEventType})

	// Unsubscribe
	publisher.Unsubscribe(sub, []*driver_infrastructure.EventType{services.MonitorStopEventType})

	monitorType := &driver_infrastructure.MonitorType{Name: "test"}
	event := services.MonitorStopEvent{MonitorType: monitorType, Key: "key-1"}
	publisher.Publish(event)

	events := sub.getEvents()
	assert.Len(t, events, 0)
}

func TestSubscribeMultipleEventTypes(t *testing.T) {
	publisher := services.NewBatchingEventPublisher(1 * time.Hour)
	defer publisher.Stop()

	sub := &testSubscriber{}
	publisher.Subscribe(sub, []*driver_infrastructure.EventType{
		services.MonitorStopEventType,
		driver_infrastructure.MonitorResetEventType,
	})

	// Publish MonitorStopEvent (immediate)
	monitorType := &driver_infrastructure.MonitorType{Name: "test"}
	publisher.Publish(services.MonitorStopEvent{MonitorType: monitorType, Key: "key-1"})

	// Publish MonitorResetEvent (immediate)
	publisher.Publish(services.MonitorResetEvent{ClusterId: "cluster-1", Endpoints: map[string]struct{}{}})

	events := sub.getEvents()
	assert.Len(t, events, 2)
}

func TestMultipleSubscribers(t *testing.T) {
	publisher := services.NewBatchingEventPublisher(1 * time.Hour)
	defer publisher.Stop()

	sub1 := &testSubscriber{}
	sub2 := &testSubscriber{}
	publisher.Subscribe(sub1, []*driver_infrastructure.EventType{services.MonitorStopEventType})
	publisher.Subscribe(sub2, []*driver_infrastructure.EventType{services.MonitorStopEventType})

	monitorType := &driver_infrastructure.MonitorType{Name: "test"}
	publisher.Publish(services.MonitorStopEvent{MonitorType: monitorType, Key: "key-1"})

	assert.Len(t, sub1.getEvents(), 1)
	assert.Len(t, sub2.getEvents(), 1)
}

func TestUnsubscribeOneOfMultiple(t *testing.T) {
	publisher := services.NewBatchingEventPublisher(1 * time.Hour)
	defer publisher.Stop()

	sub1 := &testSubscriber{}
	sub2 := &testSubscriber{}
	publisher.Subscribe(sub1, []*driver_infrastructure.EventType{services.MonitorStopEventType})
	publisher.Subscribe(sub2, []*driver_infrastructure.EventType{services.MonitorStopEventType})

	publisher.Unsubscribe(sub1, []*driver_infrastructure.EventType{services.MonitorStopEventType})

	monitorType := &driver_infrastructure.MonitorType{Name: "test"}
	publisher.Publish(services.MonitorStopEvent{MonitorType: monitorType, Key: "key-1"})

	assert.Len(t, sub1.getEvents(), 0)
	assert.Len(t, sub2.getEvents(), 1)
}

func TestPublishToNonSubscribedEventType(t *testing.T) {
	publisher := services.NewBatchingEventPublisher(1 * time.Hour)
	defer publisher.Stop()

	sub := &testSubscriber{}
	publisher.Subscribe(sub, []*driver_infrastructure.EventType{services.DataAccessEventType})

	// Publish a different event type (immediate)
	monitorType := &driver_infrastructure.MonitorType{Name: "test"}
	publisher.Publish(services.MonitorStopEvent{MonitorType: monitorType, Key: "key-1"})

	assert.Len(t, sub.getEvents(), 0)
}

func TestBatchedEventDeduplication(t *testing.T) {
	publisher := services.NewBatchingEventPublisher(50 * time.Millisecond)
	defer publisher.Stop()

	sub := &testSubscriber{}
	publisher.Subscribe(sub, []*driver_infrastructure.EventType{services.DataAccessEventType})

	// Publish multiple batched events of the same type - they should be deduplicated by event type name
	publisher.Publish(services.DataAccessEvent{TypeKey: "topology", Key: "cluster-1"})
	publisher.Publish(services.DataAccessEvent{TypeKey: "topology", Key: "cluster-2"})

	time.Sleep(150 * time.Millisecond)

	// The pending events map uses event type name as key, so only the last one should be delivered
	events := sub.getEvents()
	assert.Len(t, events, 1)
}

func TestNewEventPublisher(t *testing.T) {
	publisher := services.NewEventPublisher()
	assert.NotNil(t, publisher)
	publisher.Stop()
}
