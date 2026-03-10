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

// EventType is a type descriptor for events.
// It provides type-safe event type identification.
type EventType struct {
	Name string
}

// Event is the interface all events must implement.
type Event interface {
	GetEventType() *EventType
	IsImmediateDelivery() bool
}

// EventSubscriber is the interface for components that want to receive events.
type EventSubscriber interface {
	ProcessEvent(event Event)
}

// EventPublisher publishes events to subscribers.
type EventPublisher interface {
	Subscribe(subscriber EventSubscriber, eventTypes []*EventType)
	Unsubscribe(subscriber EventSubscriber, eventTypes []*EventType)
	Publish(event Event)
	Stop()
}

// MonitorResetEventType is defined here because it's used across multiple packages
// (driver_infrastructure, services, plugins/efm).
var MonitorResetEventType = &EventType{Name: "MonitorReset"}
