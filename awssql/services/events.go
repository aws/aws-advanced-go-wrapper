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
	"time"

	"github.com/aws/aws-advanced-go-wrapper/awssql/driver_infrastructure"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils"
)

// Event type descriptors.
var (
	DataAccessEventType  = &driver_infrastructure.EventType{Name: "DataAccess"}
	MonitorStopEventType = &driver_infrastructure.EventType{Name: "MonitorStop"}
)

const DefaultMessageInterval = 30 * time.Second

// =============================================================================
// Event Types (concrete implementations of driver_infrastructure.Event)
// =============================================================================

// DataAccessEvent is published when data is retrieved from the storage service.
type DataAccessEvent struct {
	TypeKey string
	Key     any
}

func (e DataAccessEvent) GetEventType() *driver_infrastructure.EventType { return DataAccessEventType }
func (e DataAccessEvent) IsImmediateDelivery() bool                      { return false }

// MonitorStopEvent is published to request stopping a specific monitor.
type MonitorStopEvent struct {
	MonitorType *driver_infrastructure.MonitorType
	Key         any
}

func (e MonitorStopEvent) GetEventType() *driver_infrastructure.EventType {
	return MonitorStopEventType
}
func (e MonitorStopEvent) IsImmediateDelivery() bool { return true }

// =============================================================================
// Event Publisher Implementation
// =============================================================================

// subscriberSet holds subscribers for a specific event type.
type subscriberSet struct {
	subscribers map[driver_infrastructure.EventSubscriber]struct{}
	mu          sync.RWMutex
}

func newSubscriberSet() *subscriberSet {
	return &subscriberSet{
		subscribers: make(map[driver_infrastructure.EventSubscriber]struct{}),
	}
}

func (s *subscriberSet) add(subscriber driver_infrastructure.EventSubscriber) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.subscribers[subscriber] = struct{}{}
}

func (s *subscriberSet) remove(subscriber driver_infrastructure.EventSubscriber) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.subscribers, subscriber)
}

func (s *subscriberSet) isEmpty() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.subscribers) == 0
}

func (s *subscriberSet) forEach(fn func(driver_infrastructure.EventSubscriber)) {
	s.mu.RLock()
	subs := make([]driver_infrastructure.EventSubscriber, 0, len(s.subscribers))
	for sub := range s.subscribers {
		subs = append(subs, sub)
	}
	s.mu.RUnlock()

	for _, sub := range subs {
		fn(sub)
	}
}

// BatchingEventPublisher is an EventPublisher that batches events and delivers them periodically.
// Implements driver_infrastructure.EventPublisher.
type BatchingEventPublisher struct {
	subscribers     *utils.RWMap[string, *subscriberSet]
	pendingEvents   *utils.RWMap[string, driver_infrastructure.Event]
	messageInterval time.Duration
	stopCh          chan struct{}
}

// NewEventPublisher creates a new batching event publisher.
func NewEventPublisher() driver_infrastructure.EventPublisher {
	return NewBatchingEventPublisher(DefaultMessageInterval)
}

// NewBatchingEventPublisher creates a new batching event publisher with custom interval.
func NewBatchingEventPublisher(messageInterval time.Duration) *BatchingEventPublisher {
	p := &BatchingEventPublisher{
		subscribers:     utils.NewRWMap[string, *subscriberSet](),
		pendingEvents:   utils.NewRWMap[string, driver_infrastructure.Event](),
		messageInterval: messageInterval,
		stopCh:          make(chan struct{}),
	}
	go p.publishingLoop()
	return p
}

// Subscribe registers a subscriber for the given event types.
func (p *BatchingEventPublisher) Subscribe(subscriber driver_infrastructure.EventSubscriber, eventTypes []*driver_infrastructure.EventType) {
	for _, eventType := range eventTypes {
		set := p.subscribers.ComputeIfAbsent(eventType.Name, func() *subscriberSet {
			return newSubscriberSet()
		})
		set.add(subscriber)
	}
}

// Unsubscribe removes a subscriber from the given event types.
func (p *BatchingEventPublisher) Unsubscribe(subscriber driver_infrastructure.EventSubscriber, eventTypes []*driver_infrastructure.EventType) {
	for _, eventType := range eventTypes {
		set, ok := p.subscribers.Get(eventType.Name)
		if !ok {
			continue
		}
		set.remove(subscriber)
		if set.isEmpty() {
			p.subscribers.Remove(eventType.Name)
		}
	}
}

// Publish publishes an event. Immediate delivery events are sent now; others are batched.
func (p *BatchingEventPublisher) Publish(event driver_infrastructure.Event) {
	if event.IsImmediateDelivery() {
		p.deliverEvent(event)
	} else {
		p.pendingEvents.Put(event.GetEventType().Name, event)
	}
}

func (p *BatchingEventPublisher) deliverEvent(event driver_infrastructure.Event) {
	set, ok := p.subscribers.Get(event.GetEventType().Name)
	if !ok {
		return
	}

	set.forEach(func(subscriber driver_infrastructure.EventSubscriber) {
		subscriber.ProcessEvent(event)
	})
}

func (p *BatchingEventPublisher) publishingLoop() {
	ticker := time.NewTicker(p.messageInterval)
	defer ticker.Stop()

	for {
		select {
		case <-p.stopCh:
			return
		case <-ticker.C:
			p.sendPendingEvents()
		}
	}
}

func (p *BatchingEventPublisher) sendPendingEvents() {
	events := p.pendingEvents.GetAllEntries()
	p.pendingEvents.Clear()

	for _, event := range events {
		p.deliverEvent(event)
	}
}

// Stop stops the background publishing goroutine.
func (p *BatchingEventPublisher) Stop() {
	close(p.stopCh)
}
